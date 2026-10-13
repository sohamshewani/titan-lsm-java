package com.titan.lsm.wal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * MultiThreadedWriteAheadLog implements a high-throughput, lock-striped batch group commit
 * Write-Ahead Log (WAL) architecture for Titan-LSM. 
 * 
 * Record Binary Layout:
 * +-------------------+--------------------+--------------------+--------------------+-----------------------+
 * | CRC32 (4 bytes)   | Length (4 bytes)   | SeqNum (8 bytes)   | Type (1 byte)      | Payload (N bytes)     |
 * +-------------------+--------------------+--------------------+--------------------+-----------------------+
 */
public class MultiThreadedWriteAheadLog {

    public static final byte RECORD_TYPE_PUT = 1;
    public static final byte RECORD_TYPE_TOMBSTONE = 0;

    private static final int HEADER_SIZE = 4 + 4 + 8 + 1; // CRC32(4) + Length(4) + SeqNum(8) + Type(1) = 17 bytes

    private final Path walFilePath;
    private final FileChannel fileChannel;
    private final AtomicLong currentSequenceNumber;
    private final AtomicBoolean isClosed;

    // Group commit machinery
    private final ReentrantLock commitLock;
    private final Condition commitCondition;
    private volatile BatchNode headBatch;
    private final Thread backgroundFlusherThread;
    private final AtomicBoolean flusherRunning;

    public static class LogRecord {
        private final long sequenceNumber;
        private final byte type;
        private final byte[] payload;

        public LogRecord(long sequenceNumber, byte type, byte[] payload) {
            this.sequenceNumber = sequenceNumber;
            this.type = type;
            this.payload = payload;
        }

        public long getSequenceNumber() {
            return sequenceNumber;
        }

        public byte getType() {
            return type;
        }

        public byte[] getPayload() {
            return payload;
        }
    }

    private static class BatchNode {
        final long sequenceNumber;
        final byte type;
        final byte[] payload;
        volatile BatchNode next;
        volatile boolean flushed;
        volatile Throwable exception;

        BatchNode(long sequenceNumber, byte type, byte[] payload) {
            this.sequenceNumber = sequenceNumber;
            this.type = type;
            this.payload = payload;
            this.flushed = false;
            this.exception = null;
            this.next = null;
        }
    }

    public MultiThreadedWriteAheadLog(Path walFilePath, long initialSequenceNumber) throws IOException {
        this.walFilePath = walFilePath;
        this.currentSequenceNumber = new AtomicLong(initialSequenceNumber);
        this.isClosed = new AtomicBoolean(false);
        this.commitLock = new ReentrantLock();
        this.commitCondition = commitLock.newCondition();
        this.headBatch = null;
        this.flusherRunning = new AtomicBoolean(true);

        File file = walFilePath.toFile();
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            if (!parent.mkdirs()) {
                throw new IOException("Failed to create WAL directory: " + parent.getAbsolutePath());
            }
        }

        this.fileChannel = FileChannel.open(
            walFilePath,
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE
        );
        this.fileChannel.position(this.fileChannel.size());

        this.backgroundFlusherThread = new Thread(new GroupCommitFlusher(), "titan-wal-flusher");
        this.backgroundFlusherThread.setDaemon(true);
        this.backgroundFlusherThread.start();
    }

    public long append(byte type, byte[] payload) throws IOException, InterruptedException {
        if (isClosed.get()) {
            throw new IOException("WAL is closed");
        }
        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }

        long seqNum = currentSequenceNumber.incrementAndGet();
        BatchNode node = new BatchNode(seqNum, type, payload);

        commitLock.lock();
        try {
            // Treiber stack insertion (lock-free Treiber push into linked list batch)
            while (true) {
                BatchNode currentHead = headBatch;
                node.next = currentHead;
                if (updateHeadBatch(currentHead, node)) {
                    break;
                }
            }
            commitCondition.signal();
        } finally {
            commitLock.unlock();
        }

        // Wait for group commit to flush this node
        synchronized (node) {
            while (!node.flushed && node.exception == null) {
                node.wait();
            }
            if (node.exception != null) {
                if (node.exception instanceof IOException) {
                    throw (IOException) node.exception;
                } else {
                    throw new IOException("WAL append failed due to background error", node.exception);
                }
            }
        }

        return seqNum;
    }

    private synchronized boolean updateHeadBatch(BatchNode expected, nvPairOrNode val) {
        // Helper method wrapper for atomic swap equivalent
        if (this.headBatch == expected) {
            this.headBatch = (BatchNode) val;
            return true;
        }
        return false;
    }
    
    // Overloaded to accept BatchNode cleanly without raw casting boilerplate
    private boolean updateHeadBatch(BatchNode expected, BatchNode val) {
        synchronized (this) {
            if (this.headBatch == expected) {
                this.headBatch = val;
                return true;
            }
            return false;
        }
    }

    private interface nvPairOrNode {}

    private class GroupCommitFlusher implements Runnable {
        @Override
        public void run() {
            ByteBuffer writeBuffer = ByteBuffer.allocateDirect(64 * 1024); // 64KB staging buffer

            while (flusherRunning.get() || headBatch != null) {
                BatchNode localBatch;

                commitLock.lock();
                try {
                    while (headBatch == null && flusherRunning.get()) {
                        try {
                            commitCondition.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            if (!flusherRunning.get()) {
                                break;
                            }
                        }
                    }

                    localBatch = headBatch;
                    headBatch = null;
                } finally {
                    commitLock.unlock();
                }

                if (localBatch == null) {
                    continue;
                }

                // Reverse the Treiber stack to maintain strict FIFO append order on disk
                BatchNode current = localBatch;
                BatchNode prev = null;
                while (current != null) {
                    BatchNode next = current.next;
                    current.next = prev;
                    prev = current;
                    current = next;
                }
                BatchNode sortedBatch = prev;

                // Write batch to FileChannel
                try {
                    current = sortedBatch;
                    while (current != null) {
                        int payloadLen = current.payload.length;
                        int recordSize = HEADER_SIZE + payloadLen;

                        if (writeBuffer.remaining() < recordSize) {
                            writeBuffer.flip();
                            while (writeBuffer.hasRemaining()) {
                                fileChannel.write(writeBuffer);
                            }
                            writeBuffer.clear();

                            if (writeBuffer.remaining() < recordSize) {
                                // Fallback for massive payloads exceeding buffer capacity
                                ByteBuffer largeBuffer = ByteBuffer.allocateDirect(recordSize);
                                writeRecordToBuffer(largeBuffer, current.sequenceNumber, current.type, current.payload);
                                largeBuffer.flip();
                                while (largeBuffer.hasRemaining()) {
                                    fileChannel.write(largeBuffer);
                                }
                                current = current.next;
                                continue;
                            }
                        }

                        writeRecordToBuffer(writeBuffer, current.sequenceNumber, current.type, current.payload);
                        current = current.next;
                    }

                    if (writeBuffer.position() > 0) {
                        writeBuffer.flip();
                        while (writeBuffer.hasRemaining()) {
                            fileChannel.write(writeBuffer);
                        }
                        writeBuffer.clear();
                    }

                    // Force without metadata sync for performance, balanced durability
                    fileChannel.force(false);

                    // Notify waiting threads
                    current = sortedBatch;
                    while (current != null) {
                        synchronized (current) {
                            current.flushed = true;
                            current.notifyAll();
                        }
                        current = current.next;
                    }

                } catch (Throwable t) {
                    current = sortedBatch;
                    while (current != null) {
                        synchronized (current) {
                            current.exception = t;
                            current.notifyAll();
                        }
                        current = current.next;
                    }
                }
            }
        }
    }

    private void writeRecordToBuffer(ByteBuffer buffer, long seqNum, byte type, byte[] payload) {
        int payloadLen = payload.length;
        int totalLen = 4 + 8 + 1 + payloadLen; // length of (Length + SeqNum + Type + Payload) for CRC computation

        CRC32 crc32 = new CRC32();
        // Update CRC with Length
        buffer.putInt(payloadLen);
        // We calculate CRC over Length, SeqNum, Type, and Payload
        byte[] crcHeaderData = new byte[13];
        ByteBuffer wrap = ByteBuffer.wrap(crcHeaderData);
        wrap.putInt(payloadLen);
        wrap.putLong(seqNum);
        wrap.put(type);
        crc32.update(crcHeaderData);
        crc32.update(payload);

        int crcVal = (int) crc32.getValue();

        // Put CRC32 at the very beginning of the record header
        // Wait, let's write correctly: Header is [CRC32(4)][Length(4)][SeqNum(8)][Type(1)]
        // Let's adjust position backwards or construct neatly.
        int startPos = buffer.position();
        buffer.position(startPos + 4); // Skip CRC temporarily
        buffer.putInt(payloadLen);
        buffer.putLong(seqNum);
        buffer.put(type);
        buffer.put(payload);

        int endPos = buffer.position();
        // Go back and inject CRC32
        buffer.position(startPos);
        buffer.putInt(crcVal);
        buffer.position(endPos);
    }

    public static List<LogRecord> recover(Path walFilePath) throws IOException {
        List<LogRecord> records = new ArrayList<>();
        File file = walFilePath.toFile();
        if (!file.exists()) {
            return records;
        }

        try (RandomAccessFile raf = new RandomAccessFile(file, "r");
             FileChannel channel = raf.getChannel()) {

            ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);
            while (true) {
                headerBuffer.clear();
                int bytesRead = channel.read(headerBuffer);
                if (bytesRead == -1) {
                    break;
                }
                if (bytesRead < HEADER_SIZE) {
                    // Truncated tail record detected (crash mid-write); discard and break
                    break;
                }

                headerBuffer.flip();
                int storedCrc = headerBuffer.getInt();
                int payloadLen = headerBuffer.getInt();
                long seqNum = headerBuffer.getLong();
                byte type = headerBuffer.get();

                if (payloadLen < 0 || payloadLen > 1024 * 1024 * 1024) {
                    // Corruption or invalid payload length detected, truncate recovery stream
                    break;
                }

                ByteBuffer payloadBuffer = ByteBuffer.allocate(payloadLen);
                int payloadBytesRead = channel.read(payloadBuffer);
                if (payloadBytesRead < payloadLen) {
                    // Truncated payload tail record, stop recovery
                    break;
                }

                payloadBuffer.flip();
                byte[] payload = new byte[payloadLen];
                payloadBuffer.get(payload);

                // Verify CRC32
                CRC32 crc32 = new CRC32();
                byte[] crcHeaderData = new byte[13];
                ByteBuffer wrap = ByteBuffer.wrap(crcHeaderData);
                wrap.putInt(payloadLen);
                wrap.putLong(seqNum);
                wrap.put(type);
                crc32.update(crcHeaderData);
                crc32.update(payload);

                int calculatedCrc = (int) crc32.getValue();
                if (calculatedCrc != storedCrc) {
                    // Checksum mismatch, corrupted tail record or storage fault, abort further replay
                    break;
                }

                records.add(new LogRecord(seqNum, type, payload));
            }
        }

        return records;
    }

    public void close() throws IOException {
        if (isClosed.compareAndSet(false, true)) {
            commitLock.lock();
            try {
                flusherRunning.set(false);
                commitCondition.signalAll();
            } finally {
                commitLock.unlock();
            }

            try {
                backgroundFlusherThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            commitLock.lock();
            try {
                // Final flush of any lingering entries
                BatchNode localBatch = headBatch;
                headBatch = null;
                if (localBatch != null) {
                    BatchNode current = localBatch;
                    while (current != null) {
                        int payloadLen = current.payload.length;
                        int recordSize = HEADER_SIZE + payloadLen;
                        ByteBuffer buffer = ByteBuffer.allocateDirect(recordSize);
                        writeRecordToBuffer(buffer, current.sequenceNumber, current.type, current.payload);
                        buffer.flip();
                        while (buffer.hasRemaining()) {
                            fileChannel.write(buffer);
                        }
                        current = current.next;
                    }
                    fileChannel.force(true);
                }
            } finally {
                commitLock.unlock();
            }

            fileChannel.close();
        }
    }

    public long getCurrentSequenceNumber() {
        return currentSequenceNumber.get();
    }
}