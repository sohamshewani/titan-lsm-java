package com.titan.lsm.memory;

import com.titan.lsm.key.InternalKey;
import com.titan.lsm.key.RecordType;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * ConcurrentSkipListMemTable implements a lock-free probabilistic skip-list based memory table
 * utilizing concurrent skip-list maps and byte-size tracking to bound memory consumption
 * before transitioning to an immutable flushing state.
 */
public class ConcurrentSkipListMemTable implements Iterable<Map.Entry<InternalKey, byte[]>> {

    private final ConcurrentSkipListMap<InternalKey, byte[]> table;
    private final AtomicLong approximateMemoryUsage;
    private final long memoryThresholdBytes;
    private final AtomicBoolean isImmutable;
    private final ReentrantReadWriteLock rwLock;
    private final long creationTimestamp;
    private final AtomicLong minSequenceNumber;
    private final AtomicLong maxSequenceNumber;

    /**
     * Constructs a new ConcurrentSkipListMemTable with a defined memory threshold.
     * 
     * @param memoryThresholdBytes Maximum capacity in bytes before marking table immutable.
     */
    public ConcurrentSkipListMemTable(long memoryThresholdBytes) {
        this.table = new ConcurrentSkipListMap<>();
        this.approximateMemoryUsage = new AtomicLong(0);
        this.memoryThresholdBytes = memoryThresholdBytes;
        this.isImmutable = new AtomicBoolean(false);
        this.rwLock = new ReentrantReadWriteLock();
        this.creationTimestamp = System.currentTimeMillis();
        this.minSequenceNumber = new AtomicLong(Long.MAX_VALUE);
        this.maxSequenceNumber = new AtomicLong(Long.MIN_VALUE);
    }

    /**
     * Inserts a key-value pair into the MemTable.
     * 
     * @param key   The InternalKey wrapping user key, sequence number, and record type.
     * @param value The raw byte payload. If RecordType is TOMBSTONE, value may be empty/null.
     * @return true if successfully inserted, false if the MemTable is immutable or closed.
     */
    public boolean put(InternalKey key, byte[] value) {
        if (isImmutable.get()) {
            return false;
        }

        rwLock.readLock().lock();
        try {
            if (isImmutable.get()) {
                return false;
            }

            byte[] safeValue = (value == null) ? new byte[0] : value;
            byte[] existing = table.putIfAbsent(key, safeValue);
            
            // If already present under the exact same InternalKey, replace it
            if (existing != null) {
                table.put(key, safeValue);
                // Memory delta calculation for replacement
                long oldSize = calculateEntrySize(key, existing);
                long newSize = calculateEntrySize(key, safeValue);
                approximateMemoryUsage.addAndGet(newSize - oldSize);
            } else {
                long entrySize = calculateEntrySize(key, safeValue);
                approximateMemoryUsage.addAndGet(entrySize);
            }

            updateSequenceBounds(key.getSequenceNumber());
            return true;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Retrieves the value associated with the given InternalKey.
     * 
     * @param key The InternalKey to search for.
     * @return The byte array value, or null if not found.
     */
    public byte[] get(InternalKey key) {
        rwLock.readLock().lock();
        try {
            return table.get(key);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Performs a point lookup for a user key, returning the latest version less than or equal
     * to the requested sequence number.
     * 
     * @param userKey The raw user key bytes.
     * @param snapshotSequence The sequence number boundary for MVCC visibility.
     * @return Map.Entry containing the matching InternalKey and value, or null.
     */
    public Map.Entry<InternalKey, byte[]> getLatestVisible(byte[] userKey, long snapshotSequence) {
        rwLock.readLock().lock();
        try {
            // Construct a search key with max sequence for the user key to floor-search
            InternalKey searchKey = new InternalKey(userKey, snapshotSequence, RecordType.PUT);
            
            // ConcurrentSkipListMap floorEntry gives us the greatest key less than or equal to searchKey
            Map.Entry<InternalKey, byte[]> entry = table.floorEntry(searchKey);
            
            if (entry == null) {
                return null;
            }

            InternalKey foundKey = entry.getKey();
            // Verify that the user keys match (floorEntry matches lexicographically)
            if (java.util.Arrays.equals(foundKey.getUserKey(), userKey)) {
                return entry;
            }

            return null;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Calculates the memory footprint overhead of a single skip-list entry.
     * Accounts for object headers, internal structures, key bytes, and value bytes.
     */
    private long calculateEntrySize(InternalKey key, byte[] value) {
        // Base object overhead for SkipList node, InternalKey object, and arrays
        long baseNodeOverhead = 96; 
        long keySize = key.sizeInMemory();
        long valueSize = 16 + (value != null ? value.length : 0);
        return baseNodeOverhead + keySize + valueSize;
    }

    /**
     * Updates internal tracking of minimum and maximum sequence numbers.
     */
    private void updateSequenceBounds(long seqNum) {
        long currentMin;
        do {
            currentMin = minSequenceNumber.get();
            if (seqNum >= currentMin) {
                break;
            }
        } while (!minSequenceNumber.compareAndSet(currentMin, seqNum));

        long currentMax;
        do {
            currentMax = maxSequenceNumber.get();
            if (seqNum <= currentMax) {
                break;
            }
        } while (!maxSequenceNumber.compareAndSet(currentMax, seqNum));
    }

    /**
     * Checks if the MemTable has exceeded its memory threshold and should be flushed.
     * 
     * @return true if memory usage is greater than or equal to threshold.
     */
    public boolean shouldFlush() {
        return approximateMemoryUsage.get() >= memoryThresholdBytes;
    }

    /**
     * Marks this MemTable as immutable, preventing further write operations.
     * Acquires the write lock to safely transition states.
     */
    public void makeImmutable() {
        rwLock.writeLock().lock();
        try {
            isImmutable.set(true);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Returns whether the MemTable is currently marked as immutable.
     */
    public boolean isImmutable() {
        return isImmutable.get();
    }

    /**
     * Returns the current approximate memory consumption in bytes.
     */
    public long getMemoryUsage() {
        return approximateMemoryUsage.get();
    }

    /**
     * Returns the number of entries currently stored in the MemTable.
     */
    public int size() {
        return table.size();
    }

    /**
     * Returns true if the MemTable contains no entries.
     */
    public boolean isEmpty() {
        return table.isEmpty();
    }

    /**
     * Returns the minimum sequence number recorded in this MemTable.
     */
    public long getMinSequenceNumber() {
        long val = minSequenceNumber.get();
        return val == Long.MAX_VALUE ? 0 : val;
    }

    /**
     * Returns the maximum sequence number recorded in this MemTable.
     */
    public long getMaxSequenceNumber() {
        long val = maxSequenceNumber.get();
        return val == Long.MIN_VALUE ? 0 : val;
    }

    /**
     * Returns the creation timestamp of this MemTable.
     */
    public long getCreationTimestamp() {
        return creationTimestamp;
    }

    /**
     * Provides a thread-safe iterator over all entries in sorted order.
     */
    @Override
    public Iterator<Map.Entry<InternalKey, byte[]>> iterator() {
        return table.entrySet().iterator();
    }

    /**
     * Clears all entries from the underlying structure and resets metrics.
     */
    public void clear() {
        rwLock.writeLock().lock();
        try {
            table.clear();
            approximateMemoryUsage.set(0);
            minSequenceNumber.set(Long.MAX_VALUE);
            maxSequenceNumber.set(Long.MIN_VALUE);
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}