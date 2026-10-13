package com.titan.lsm.core;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

public class LsmEngine implements AutoCloseable {

    public enum RecordType {
        TOMBSTONE((byte) 0),
        PUT((byte) 1);

        private final byte code;

        RecordType(byte code) {
            this.code = code;
        }

        public static RecordType fromCode(byte code) {
            for (RecordType rt : values()) {
                if (rt.code == code) {
                    return rt;
                }
            }
            throw new IllegalArgumentException("Unknown RecordType code: " + code);
        }

        public byte getCode() {
            return code;
        }
    }

    public static final class InternalKey implements Comparable<InternalKey> {
        private final byte[] userKey;
        private final long sequenceNumber;
        private final RecordType recordType;

        public InternalKey(byte[] userKey, long sequenceNumber, RecordType recordType) {
            if (userKey == null) {
                throw new IllegalArgumentException("userKey cannot be null");
            }
            this.userKey = userKey;
            this.sequenceNumber = sequenceNumber;
            this.recordType = recordType;
        }

        public byte[] getUserKey() {
            return userKey;
        }

        public long getSequenceNumber() {
            return sequenceNumber;
        }

        public RecordType getRecordType() {
            return recordType;
        }

        @Override
        public int compareTo(InternalKey other) {
            int cmp = Arrays.compare(this.userKey, other.userKey);
            if (cmp != 0) {
                return cmp;
            }
            if (this.sequenceNumber != other.sequenceNumber) {
                return Long.compare(other.sequenceNumber, this.sequenceNumber);
            }
            return Byte.compare(other.recordType.getCode(), this.recordType.getCode());
        }

        public byte[] serialize() {
            ByteBuffer buffer = ByteBuffer.allocate(4 + userKey.length + 8 + 1);
            buffer.putInt(userKey.length);
            buffer.put(userKey);
            buffer.putLong(sequenceNumber);
            buffer.put(recordType.getCode());
            return buffer.array();
        }

        public static InternalKey deserialize(ByteBuffer buffer) {
            int keyLen = buffer.getInt();
            byte[] userKey = new byte[keyLen];
            buffer.get(userKey);
            long seq = buffer.getLong();
            byte typeCode = buffer.get();
            RecordType type = RecordType.fromCode(typeCode);
            return new InternalKey(userKey, seq, type);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof InternalKey)) return false;
            InternalKey other = (InternalKey) obj;
            return sequenceNumber == other.sequenceNumber &&
                    recordType == other.recordType &&
                    Arrays.equals(userKey, other.userKey);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(userKey);
            result = 31 * result + Long.hashCode(sequenceNumber);
            result = 31 * result + recordType.hashCode();
            return result;
        }
    }

    public static final class WriteBatch {
        private final List<Mutation> mutations = new ArrayList<>();

        public void put(byte[] key, byte[] value) {
            mutations.add(new Mutation(RecordType.PUT, key, value));
        }

        public void delete(byte[] key) {
            mutations.add(new Mutation(RecordType.TOMBSTONE, key, new byte[0]));
        }

        public List<Mutation> getMutations() {
            return mutations;
        }
    }

    public static final class Mutation {
        private final RecordType type;
        private final byte[] key;
        private final byte[] value;

        public Mutation(RecordType type, byte[] key, byte[] value) {
            this.type = type;
            this.key = key;
            this.value = value;
        }

        public RecordType getType() { return type; }
        public byte[] getKey() { return key; }
        public byte[] getValue() { return value; }
    }

    public static final class WriteAheadLog implements AutoCloseable {
        private final FileChannel channel;
        private final Path path;

        public WriteAheadLog(Path path) throws IOException {
            this.path = path;
            this.channel = FileChannel.open(path, 
                    StandardOpenOption.CREATE, 
                    StandardOpenOption.WRITE, 
                    StandardOpenOption.READ,
                    StandardOpenOption.APPEND);
        }

        public synchronized void append(long sequenceNumber, RecordType type, byte[] key, byte[] value) throws IOException {
            int payloadLen = 8 + 1 + 4 + key.length + 4 + (value != null ? value.length : 0);
            ByteBuffer buffer = ByteBuffer.allocate(4 + payloadLen);
            
            CRC32 crc = new CRC32();
            
            ByteBuffer payload = ByteBuffer.allocate(payloadLen);
            payload.putLong(sequenceNumber);
            payload.put(type.getCode());
            payload.putInt(key.length);
            payload.put(key);
            int valLen = value != null ? value.length : 0;
            payload.putInt(valLen);
            if (valLen > 0) {
                payload.put(value);
            }
            
            crc.update(payload.array());
            long checksum = crc.getValue();

            buffer.putInt((int) checksum);
            buffer.put(payload.array());
            buffer.flip();

            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(false);
        }

        public void recover(ConcurrentSkipListMap<InternalKey, byte[]> memTable, AtomicLong sequenceCounter) throws IOException {
            if (!Files.exists(path)) {
                return;
            }
            try (FileChannel readChannel = FileChannel.open(path, StandardOpenOption.READ)) {
                ByteBuffer lenBuf = ByteBuffer.allocate(4);
                while (readChannel.read(lenBuf) > 0) {
                    lenBuf.flip();
                    int checksum = lenBuf.getInt();
                    lenBuf.clear();

                    // Read size indicator or payload header. Since structure is uniform, parse sequentially.
                    // To handle arbitrary records safely, we read header first.
                    // Let's read seq(8) + type(1) + kLen(4) = 13 bytes to read key length.
                    ByteBuffer headerBuf = ByteBuffer.allocate(13);
                    int readBytes = readChannel.read(headerBuf);
                    if (readBytes < 13) {
                        // Corrupted tail
                        break;
                    }
                    headerBuf.flip();
                    long seq = headerBuf.getLong();
                    byte typeCode = headerBuf.get();
                    int keyLen = headerBuf.getInt();

                    byte[] key = new byte[keyLen];
                    ByteBuffer keyBuf = ByteBuffer.wrap(key);
                    readChannel.read(keyBuf);

                    ByteBuffer valLenBuf = ByteBuffer.allocate(4);
                    readChannel.read(valLenBuf);
                    valLenBuf.flip();
                    int valLen = valLenBuf.getInt();

                    byte[] value = new byte[valLen];
                    if (valLen > 0) {
                        ByteBuffer valBuf = ByteBuffer.wrap(value);
                        readChannel.read(valBuf);
                    }

                    RecordType type = RecordType.fromCode(typeCode);

                    // Validate checksum
                    int payloadLen = 8 + 1 + 4 + key.length + 4 + valLen;
                    ByteBuffer payload = ByteBuffer.allocate(payloadLen);
                    payload.putLong(seq);
                    payload.put(typeCode);
                    payload.putInt(keyLen);
                    payload.put(key);
                    payload.putInt(valLen);
                    if (valLen > 0) {
                        payload.put(value);
                    }

                    CRC32 crc = new CRC32();
                    crc.update(payload.array());
                    if ((int) crc.getValue() != checksum) {
                        // Corrupted tail record detected
                        break;
                    }

                    InternalKey internalKey = new InternalKey(key, seq, type);
                    memTable.put(internalKey, value);
                    sequenceCounter.set(Math.max(sequenceCounter.get(), seq + 1));
                }
            }
        }

        @Override
        public void close() throws IOException {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        }
    }

    public static final class BloomFilter {
        private final long[] bitmap;
        private final int numHashFunctions;
        private final int bitmapSizeBits;

        public BloomFilter(int expectedInsertions, double fpp) {
            this.bitmapSizeBits = (int) (-expectedInsertions * Math.log(fpp) / (Math.log(2) * Math.log(2)));
            this.numHashFunctions = Math.max(1, (int) Math.round((double) bitmapSizeBits / expectedInsertions * Math.log(2)));
            int longWords = (bitmapSizeBits + 63) / 64;
            this.bitmap = new long[longWords];
        }

        public void put(byte[] key) {
            long hash1 = hash(key, 0x12345678);
            long hash2 = hash(key, 0x87654321);
            for (int i = 0; i < numHashFunctions; i++) {
                long combinedHash = Math.abs(hash1 + i * hash2);
                int bitIndex = (int) (combinedHash % bitmapSizeBits);
                bitmap[bitIndex / 64] |= (1L << (bitIndex % 64));
            }
        }

        public boolean mightContain(byte[] key) {
            long hash1 = hash(key, 0x12345678);
            long hash2 = hash(key, 0x87654321);
            for (int i = 0; i < numHashFunctions; i++) {
                long combinedHash = Math.abs(hash1 + i * hash2);
                int bitIndex = (int) (combinedHash % bitmapSizeBits);
                if ((bitmap[bitIndex / 64] & (1L << (bitIndex % 64))) == 0) {
                    return false;
                }
            }
            return true;
        }

        private long hash(byte[] data, int seed) {
            long h = seed;
            for (byte b : data) {
                h = (h * 31) + b;
            }
            return h;
        }

        public byte[] serialize() {
            ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + 4 + bitmap.length * 8);
            buffer.putInt(numHashFunctions);
            buffer.putInt(bitmapSizeBits);
            buffer.putInt(bitmap.length);
            for (long l : bitmap) {
                buffer.putLong(l);
            }
            return buffer.array();
        }

        public static BloomFilter deserialize(ByteBuffer buffer) {
            int numHash = buffer.getInt();
            int bitSize = buffer.getInt();
            int len = buffer.getInt();
            long[] bitmap = new long[len];
            for (int i = 0; i < len; i++) {
                bitmap[i] = buffer.getLong();
            }
            BloomFilter bf = new BloomFilter(100, 0.01);
            // override fields
            return bf;
        }
    }

    public static final class SstMetadata {
        private final Path filePath;
        private final InternalKey minKey;
        private final InternalKey maxKey;
        private final List<BlockIndexEntry> blockIndex;

        public SstMetadata(Path filePath, InternalKey minKey, InternalKey maxKey, List<BlockIndexEntry> blockIndex) {
            this.filePath = filePath;
            this.minKey = minKey;
            this.maxKey = maxKey;
            this.blockIndex = blockIndex;
        }

        public Path getFilePath() { return filePath; }
        public InternalKey getMinKey() { return minKey; }
        public InternalKey getMaxKey() { return maxKey; }
        public List<BlockIndexEntry> getBlockIndex() { return blockIndex; }
    }

    public static final class BlockIndexEntry {
        private final InternalKey firstKey;
        private final long offset;
        private final int size;

        public BlockIndexEntry(InternalKey firstKey, long offset, int size) {
            this.firstKey = firstKey;
            this.offset = offset;
            this.size = size;
        }

        public InternalKey getFirstKey() { return firstKey; }
        public long getOffset() { return offset; }
        public int getSize() { return size; }
    }

    private final Path dataDir;
    private final AtomicLong sequenceCounter = new AtomicLong(0);
    private ConcurrentSkipListMap<InternalKey, byte[]> memTable = new ConcurrentSkipListMap<>();
    private ConcurrentSkipListMap<InternalKey, byte[]> immutableMemTable = null;
    private WriteAheadLog wal;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final List<SstMetadata> sstables = new ArrayList<>();
    private final ScheduledExecutorService backgroundExecutor = Executors.newScheduledThreadPool(2);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public LsmEngine(String directoryPath) throws IOException {
        this.dataDir = Paths.get(directoryPath);
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir);
        }
        Path walPath = dataDir.resolve("wal.log");
        this.wal = new WriteAheadLog(walPath);
        this.wal.recover(memTable, sequenceCounter);

        backgroundExecutor.scheduleAtFixedRate(this::backgroundFlush, 1, 1, TimeUnit.SECONDS);
        backgroundExecutor.scheduleAtFixedRate(this::backgroundCompaction, 5, 5, TimeUnit.SECONDS);
    }

    public void put(byte[] key, byte[] value) throws IOException {
        write(RecordType.PUT, key, value);
    }

    public void delete(byte[] key) throws IOException {
        write(RecordType.TOMBSTONE, key, new byte[0]);
    }

    private void write(RecordType type, byte[] key, byte[] value) throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException("LsmEngine is closed");
        }
        long seq = sequenceCounter.getAndIncrement();
        wal.append(seq, type, key, value);

        InternalKey internalKey = new InternalKey(key, seq, type);
        rwLock.writeLock().lock();
        try {
            memTable.put(internalKey, value);
            if (memTable.size() >= 10000 && immutableMemTable == null) {
                immutableMemTable = memTable;
                memTable = new ConcurrentSkipListMap<>();
                // Rotate WAL
                wal.close();
                Path oldWal = dataDir.resolve("wal.log");
                Path rotatedWal = dataDir.resolve("wal_" + System.currentTimeMillis() + ".log");
                Files.move(oldWal, rotatedWal);
                this.wal = new WriteAheadLog(oldWal);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void writeBatch(WriteBatch batch) throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException("LsmEngine is closed");
        }