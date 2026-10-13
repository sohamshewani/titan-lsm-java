package com.titan.lsm.index;

import com.titan.lsm.key.InternalKey;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.StampedLock;

/**
 * TieredSparseIndex implements a multi-level sparse index mapping boundary keys 
 * of data blocks to their physical file offsets in an SSTable. It enables O(log N) 
 * binary search coupled with block-level traversal for LSM-tree read paths.
 */
public final class TieredSparseIndex {

    private final StampedLock lock = new StampedLock();
    
    // Primary index level (Level 0 - fine-grained, maps every data block boundary)
    private final List<IndexEntry> primaryIndex;
    
    // Secondary index level (Level 1 - coarse-grained, maps boundary keys of Level 0 index blocks)
    private final List<IndexEntry> secondaryIndex;
    
    // Configuration for compaction / sparseness interval
    private final int indexInterval;

    public static final class IndexEntry implements Comparable<IndexEntry> {
        private final InternalKey key;
        private final long fileOffset;
        private final long dataSize;

        public IndexEntry(InternalKey key, long fileOffset, long dataSize) {
            if (key == null) {
                throw new IllegalArgumentException("IndexEntry key cannot be null");
            }
            this.key = key;
            this.fileOffset = fileOffset;
            this.dataSize = dataSize;
        }

        public InternalKey getKey() {
            return key;
        }

        public long getFileOffset() {
            return fileOffset;
        }

        public long getDataSize() {
            return dataSize;
        }

        @Override
        public int compareTo(IndexEntry o) {
            return this.key.compareTo(o.key);
        }
    }

    public TieredSparseIndex(int indexInterval) {
        if (indexInterval <= 0) {
            throw new IllegalArgumentException("Index interval must be greater than zero");
        }
        this.indexInterval = indexInterval;
        this.primaryIndex = new ArrayList<>();
        this.secondaryIndex = new ArrayList<>();
    }

    /**
     * Adds an index entry pointing to a data block.
     * Must be added in strict lexicographical order of InternalKey.
     */
    public void addEntry(InternalKey key, long fileOffset, long dataSize) {
        long stamp = lock.writeLock();
        try {
            IndexEntry entry = new IndexEntry(key, fileOffset, dataSize);
            if (!primaryIndex.isEmpty()) {
                IndexEntry last = primaryIndex.get(primaryIndex.size() - 1);
                if (entry.compareTo(last) <= 0) {
                    throw new IllegalArgumentException("Index entries must be strictly sorted and unique.");
                }
            }
            primaryIndex.add(entry);
            
            // Rebuild or update secondary index if primary grows by indexInterval
            if (primaryIndex.size() % indexInterval == 0) {
                secondaryIndex.add(entry);
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Finalizes the index structure, ensuring the secondary index covers all necessary partitions.
     */
    public void buildSecondaryIndex() {
        long stamp = lock.writeLock();
        try {
            secondaryIndex.clear();
            for (int i = 0; i < primaryIndex.size(); i += indexInterval) {
                secondaryIndex.add(primaryIndex.get(i));
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Searches for the target InternalKey using the tiered sparse index.
     * First performs binary search on the secondary index to narrow down the range,
     * then binary searches the primary index within that range.
     * 
     * @return The IndexEntry representing the candidate data block containing or preceding the key.
     */
    public IndexEntry findBlock(InternalKey target) {
        if (target == null) {
            throw new IllegalArgumentException("Target key cannot be null");
        }

        long stamp = lock.readLock();
        try {
            if (primaryIndex.isEmpty()) {
                return null;
            }

            int primaryStartIndex = 0;
            int primaryEndIndex = primaryIndex.size() - 1;

            // If secondary index is populated, use it to bound the primary search space
            if (!secondaryIndex.isEmpty()) {
                int secIdx = binarySearchSecondary(target);
                if (secIdx < 0) {
                    // Insertion point calculation
                    secIdx = -secIdx - 1;
                }
                
                if (secIdx == 0) {
                    primaryStartIndex = 0;
                } else if (secIdx >= secondaryIndex.size()) {
                    IndexEntry lastSec = secondaryIndex.get(secondaryIndex.size() - 1);
                    primaryStartIndex = findPrimaryIndexPosition(lastSec.getKey());
                } else {
                    IndexEntry prevSec = secondaryIndex.get(secIdx - 1);
                    primaryStartIndex = findPrimaryIndexPosition(prevSec.getKey());
                }

                if (secIdx < secondaryIndex.size()) {
                    IndexEntry nextSec = secondaryIndex.get(secIdx);
                    primaryEndIndex = Math.min(primaryIndex.size() - 1, findPrimaryIndexPosition(nextSec.getKey()));
                }
            }

            // Binary search within the determined primary index segment
            int idx = binarySearchPrimarySegment(target, primaryStartIndex, primaryEndIndex);
            if (idx < 0) {
                idx = -idx - 1;
                // If the insertion point is 0, the key is smaller than the first block's boundary
                if (idx == 0) {
                    return null;
                }
                // The candidate block is the one right before the insertion point
                idx = idx - 1;
            }

            if (idx >= primaryIndex.size()) {
                idx = primaryIndex.size() - 1;
            }

            return primaryIndex.get(idx);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    private int binarySearchSecondary(InternalKey target) {
        int low = 0;
        int high = secondaryIndex.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            IndexEntry midVal = secondaryIndex.get(mid);
            int cmp = midVal.getKey().compareTo(target);

            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid; // Exact match
            }
        }
        return -(low + 1); // Key not found, return insertion point
    }

    private int binarySearchPrimarySegment(InternalKey target, int low, int high) {
        int originalLow = low;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            IndexEntry midVal = primaryIndex.get(mid);
            int cmp = midVal.getKey().compareTo(target);

            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -(low + 1);
    }

    private int findPrimaryIndexPosition(InternalKey key) {
        int low = 0;
        int high = primaryIndex.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = primaryIndex.get(mid).getKey().compareTo(key);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return Math.max(0, low);
    }

    /**
     * Serializes the tiered sparse index to a ByteBuffer for writing into an SSTable block index section.
     */
    public ByteBuffer serialize() {
        long stamp = lock.readLock();
        try {
            // Calculate required capacity:
            // 4 bytes: primary index size
            // For each primary entry: 
            //   4 bytes user key length + user key bytes
            //   8 bytes sequence number
            //   1 byte record type
            //   8 bytes file offset
            //   8 bytes data size
            int estimatedSize = 4;
            for (IndexEntry entry : primaryIndex) {
                estimatedSize += 4 + entry.getKey().getUserKey().length + 8 + 1 + 8 + 8;
            }

            ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);
            buffer.putInt(primaryIndex.size());

            for (IndexEntry entry : primaryIndex) {
                byte[] userKey = entry.getKey().getUserKey();
                buffer.putInt(userKey.length);
                buffer.put(userKey);
                buffer.putLong(entry.getKey().getSequenceNumber());
                buffer.put(entry.getKey().getRecordType());
                buffer.putLong(entry.getFileOffset());
                buffer.putLong(entry.getDataSize());
            }

            buffer.flip();
            return buffer;
        } finally {
            lock.unlockRead(stamp);
        }
    }

    /**
     * Deserializes a TieredSparseIndex from a ByteBuffer.
     */
    public static TieredSparseIndex deserialize(ByteBuffer buffer, int indexInterval) {
        TieredSparseIndex index = new TieredSparseIndex(indexInterval);
        int primarySize = buffer.getInt();

        for (int i = 0; i < primarySize; i++) {
            int keyLen = buffer.getInt();
            byte[] userKey = new byte[keyLen];
            buffer.get(userKey);
            long seqNum = buffer.getLong();
            byte recordType = buffer.get();
            long fileOffset = buffer.getLong();
            long dataSize = buffer.getLong();

            InternalKey key = new InternalKey(userKey, seqNum, recordType);
            index.primaryIndex.add(new IndexEntry(key, fileOffset, dataSize));
        }

        index.buildSecondaryIndex();
        return index;
    }

    public void writeToChannel(FileChannel channel) throws IOException {
        ByteBuffer serialized = serialize();
        while (serialized.hasRemaining()) {
            channel.write(serialized);
        }
    }

    public int size() {
        long stamp = lock.readLock();
        try {
            return primaryIndex.size();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public List<IndexEntry> getPrimaryIndex() {
        long stamp = lock.readLock();
        try {
            return Collections.unmodifiableList(new ArrayList<>(primaryIndex));
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public List<IndexEntry> getSecondaryIndex() {
        long stamp = lock.readLock();
        try {
            return Collections.unmodifiableList(new ArrayList<>(secondaryIndex));
        } finally {
            lock.unlockRead(stamp);
        }
    }
}