package com.titan.lsm.compaction;

import com.titan.lsm.SSTable;
import com.titan.lsm.InternalKey;
import com.titan.lsm.ValueType;
import com.titan.lsm.SSTableWriter;
import com.titan.lsm.SSTableIterator;
import com.titan.lsm.VersionSet;
import com.titan.lsm.VersionEdit;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * LeveledCompactionWorker executes background compaction tasks across LSM levels.
 * It reads overlapping SSTables from level L_i and L_{i+1}, merges them using a 
 * multi-way merge-iterator, drops obsolete MVCC versions and tombstones based on 
 * the lowest active snapshot sequence number, and writes out new sorted SSTables.
 */
public class LeveledCompactionWorker implements Callable<Void> {

    private static final Logger logger = Logger.getLogger(LeveledCompactionWorker.class.getName());

    private final CompactionTask task;
    private final VersionSet versionSet;
    private final Path dbDirectory;
    private final long lowestActiveSnapshotSeqNum;
    private final AtomicBoolean shutdownRequested;

    public LeveledCompactionWorker(
            CompactionTask task,
            VersionSet versionSet,
            Path dbDirectory,
            long lowestActiveSnapshotSeqNum,
            AtomicBoolean shutdownRequested) {
        this.task = task;
        this.versionSet = versionSet;
        this.dbDirectory = dbDirectory;
        this.lowestActiveSnapshotSeqNum = lowestActiveSnapshotSeqNum;
        this.shutdownRequested = shutdownRequested;
    }

    @Override
    public Void call() throws Exception {
        logger.info(() -> String.format("Starting compaction task for level %d -> %d with %d input files at L%d and %d at L%d",
                task.getSourceLevel(),
                task.getSourceLevel() + 1,
                task.getInputsLevelA().size(),
                task.getSourceLevel(),
                task.getInputsLevelB().size()));

        List<SSTableIterator> iterators = new ArrayList<>();
        SSTableWriter builder = null;
        List<SSTable> outputs = new ArrayList<>();
        
        try {
            // 1. Gather all input SSTables from Level L and Level L+1
            List<SSTable> levelInputs = new ArrayList<>();
            levelInputs.addAll(task.getInputsLevelA());
            levelInputs.addAll(task.getInputsLevelB());

            if (levelInputs.isEmpty()) {
                logger.warning("Compaction task received zero input SSTables. Aborting compaction.");
                return null;
            }

            // 2. Open iterators for each input SSTable
            for (SSTable sstable : levelInputs) {
                if (shutdownRequested.get()) {
                    throw new IOException("Compaction aborted due to shutdown request.");
                }
                SSTableIterator it = sstable.iterator();
                it.seekToFirst();
                iterators.add(it);
            }

            // 3. Setup multi-way merge priority queue
            PriorityQueue<MergeEntry> mergeQueue = new PriorityQueue<>(
                Comparator.comparing(MergeEntry::getKey)
                        .thenComparingInt(MergeEntry::getIteratorIndex)
            );

            for (int i = 0; i < iterators.size(); i++) {
                SSTableIterator it = iterators.get(i);
                if (it.isValid()) {
                    mergeQueue.offer(new MergeEntry(it.key(), it.value(), it.type(), i));
                }
            }

            InternalKey lastUserKey = null;
            long currentFileMaxSequenceNumber = -1;
            boolean dropCurrentKeyVersion = false;

            // 4. Execute multi-way merge
            while (!mergeQueue.isEmpty()) {
                if (shutdownRequested.get()) {
                    throw new IOException("Compaction interrupted during merge phase.");
                }

                MergeEntry entry = mergeQueue.poll();
                InternalKey currentInternalKey = entry.getKey();
                byte[] value = entry.getValue();
                ValueType type = entry.getType();
                int iteratorIndex = entry.getIteratorIndex();

                // Advance the corresponding iterator and push next element into queue
                SSTableIterator sourceIterator = iterators.get(iteratorIndex);
                sourceIterator.next();
                if (sourceIterator.isValid()) {
                    mergeQueue.offer(new MergeEntry(sourceIterator.key(), sourceIterator.value(), sourceIterator.type(), iteratorIndex));
                }

                // MVCC Versioning and Tombstone Pruning Logic:
                // Compare user keys to handle multiple versions of the same user key.
                // InternalKeys are sorted primarily by UserKey (ascending) and secondarily by SequenceNumber (descending).
                // This means newer sequence numbers for the same user key appear *first*.
                if (lastUserKey == null || !lastUserKey.getUserKey().equals(currentInternalKey.getUserKey())) {
                    // Encountered a new user key
                    lastUserKey = currentInternalKey;
                    currentFileMaxSequenceNumber = currentInternalKey.getSequenceNumber();

                    // If this key's sequence number is older than the lowest active snapshot sequence number,
                    // any older versions of this user key (or tombstones) can be safely dropped.
                    // If the latest version is a TOMBSTONE and its sequence number is below the snapshot boundary,
                    // we can drop the tombstone entirely as no active snapshot can view it.
                    if (currentInternalKey.getSequenceNumber() <= lowestActiveSnapshotSeqNum) {
                        if (type == ValueType.TOMBSTONE) {
                            dropCurrentKeyVersion = true;
                        } else {
                            dropCurrentKeyVersion = false;
                        }
                    } else {
                        dropCurrentKeyVersion = false;
                    }
                } else {
                    // We have seen this user key already in a previous iteration (meaning it's an older version, i.e., lower seqNum).
                    // Since active snapshots might need older versions, we drop versions older than the lowest active snapshot.
                    if (currentInternalKey.getSequenceNumber() < lowestActiveSnapshotSeqNum) {
                        // Drop all subsequent older versions of this user key
                        dropCurrentKeyVersion = true;
                    }
                }

                if (dropCurrentKeyVersion) {
                    continue;
                }

                // Initialize SSTableWriter if needed
                if (builder == null) {
                    Path outputPath = generateNewSSTablePath(dbDirectory, task.getSourceLevel() + 1);
                    builder = new SSTableWriter(outputPath);
                }

                // Append valid record to the output SSTable
                builder.append(currentInternalKey, value, type);

                // Check if current SSTable has reached target size; if so, flush and rotate builder
                if (builder.getFileSize() >= task.getTargetFileSize()) {
                    builder.finish();
                    outputs.add(builder.getSSTable());
                    
                    Path outputPath = generateNewSSTablePath(dbDirectory, task.getSourceLevel() + 1);
                    builder = new SSTableWriter(outputPath);
                }
            }

            // Finalize last SSTable builder if populated
            if (builder != null) {
                if (builder.getEntryCount() > 0) {
                    builder.finish();
                    outputs.add(builder.getSSTable());
                } else {
                    builder.abandon();
                }
            }

            // 5. Update VersionSet atomically (Commit VersionEdit)
            VersionEdit edit = new VersionEdit();
            for (SSTable input : task.getInputsLevelA()) {
                edit.deleteFile(task.getSourceLevel(), input.getFileNumber());
            }
            for (SSTable input : task.getInputsLevelB()) {
                edit.deleteFile(task.getSourceLevel() + 1, input.getFileNumber());
            }
            for (SSTable output : outputs) {
                edit.addFile(task.getSourceLevel() + 1, output);
            }

            versionSet.logAndApply(edit);
            logger.info(() -> String.format("Compaction successfully completed for level %d -> %d. Generated %d new SSTables.",
                    task.getSourceLevel(), task.getSourceLevel() + 1, outputs.size()));

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Compaction failed with exception", e);
            if (builder != null) {
                try {
                    builder.abandon();
                } catch (IOException ex) {
                    logger.log(Level.WARNING, "Failed to abandon partial SSTable writer", ex);
                }
            }
            // Cleanup any successfully written outputs on failure
            for (SSTable output : outputs) {
                try {
                    output.delete();
                } catch (IOException ex) {
                    logger.log(Level.WARNING, "Failed to clean up output SSTable after compaction failure", ex);
                }
            }
            throw e;
        } finally {
            // Close all underlying iterators
            for (SSTableIterator it : iterators) {
                try {
                    it.close();
                } catch (IOException e) {
                    logger.log(Level.WARNING, "Failed to close SSTable iterator", e);
                }
            }
        }

        return null;
    }

    private Path generateNewSSTablePath(Path directory, int level) {
        long fileNumber = versionSet.getNextFileNumber();
        String fileName = String.format("L%d_%06d.sst", level, fileNumber);
        return directory.resolve(fileName);
    }

    /**
     * Helper container for multi-way merge priority queue.
     */
    private static class MergeEntry {
        private final InternalKey key;
        private final byte[] value;
        private final ValueType type;
        private final int iteratorIndex;

        public MergeEntry(InternalKey key, byte[] value, ValueType type, int iteratorIndex) {
            this.key = key;
            this.value = value;
            this.type = type;
            this.iteratorIndex = iteratorIndex;
        }

        public InternalKey getKey() {
            return key;
        }

        public byte[] getValue() {
            return value;
        }

        public ValueType getType() {
            return type;
        }

        public int getIteratorIndex() {
            return iteratorIndex;
        }
    }
}