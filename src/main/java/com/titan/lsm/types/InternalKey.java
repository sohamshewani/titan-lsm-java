package com.titan.lsm.types;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Encapsulates a composite immutable layout for LSM storage:
 * [user_key_bytes][sequence_number (8 bytes)][record_type (1 byte)]
 * 
 * Supports MVCC and point-in-time snapshot isolation.
 * Keys are compared lexicographically by user key (ascending), 
 * and then by sequence number (descending—so higher/newer sequence numbers come first),
 * and then by record type (descending).
 */
public final class InternalKey implements Comparable<InternalKey> {

    public static final byte TYPE_TOMBSTONE = 0;
    public static final byte TYPE_PUT = 1;

    private final byte[] userKey;
    private final long sequenceNumber;
    private final byte recordType;

    // Cached hash code for performance since InternalKey is immutable and heavily used in maps/skip-lists
    private final int cachedHashCode;

    public InternalKey(byte[] userKey, long sequenceNumber, byte recordType) {
        Objects.requireNonNull(userKey, "userKey cannot be null");
        if (recordType != TYPE_TOMBSTONE && recordType != TYPE_PUT) {
            throw new IllegalArgumentException("Invalid record type: " + recordType);
        }
        if (sequenceNumber < 0) {
            throw new IllegalArgumentException("Sequence number cannot be negative: " + sequenceNumber);
        }
        
        // Defensive copy to guarantee immutability
        this.userKey = Arrays.copyOf(userKey, userKey.length);
        this.sequenceNumber = sequenceNumber;
        this.recordType = recordType;
        this.cachedHashCode = computeHashCode();
    }

    public byte[] getUserKey() {
        return Arrays.copyOf(userKey, userKey.length);
    }

    /**
     * Internal direct accessor to avoid array allocations during hot-path comparisons.
     */
    public byte[] rawUserKey() {
        return userKey;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public byte getRecordType() {
        return recordType;
    }

    /**
     * Serializes this InternalKey into a newly allocated ByteBuffer.
     * Layout: [4-byte userKey length][userKey bytes][8-byte sequenceNumber][1-byte recordType]
     */
    public ByteBuffer serialize() {
        int capacity = 4 + userKey.length + 8 + 1;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.putInt(userKey.length);
        buffer.put(userKey);
        buffer.putLong(sequenceNumber);
        buffer.put(recordType);
        buffer.flip();
        return buffer;
    }

    /**
     * Serializes this InternalKey into an existing target ByteBuffer.
     */
    public void serialize(ByteBuffer target) {
        Objects.requireNonNull(target, "target buffer cannot be null");
        int required = 4 + userKey.length + 8 + 1;
        if (target.remaining() < required) {
            throw new IllegalArgumentException("Target buffer has insufficient capacity: remaining=" 
                    + target.remaining() + ", required=" + required);
        }
        target.putInt(userKey.length);
        target.put(userKey);
        target.putLong(sequenceNumber);
        target.put(recordType);
    }

    /**
     * Deserializes an InternalKey from a ByteBuffer at its current position.
     */
    public static InternalKey deserialize(ByteBuffer buffer) {
        Objects.requireNonNull(buffer, "buffer cannot be null");
        int keyLength = buffer.getInt();
        if (keyLength < 0) {
            throw new IllegalStateException("Corrupted InternalKey: negative user key length=" + keyLength);
        }
        if (buffer.remaining() < keyLength + 8 + 1) {
            throw new IllegalStateException("Corrupted InternalKey: buffer underflow during deserialization");
        }
        
        byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);
        long seq = buffer.getLong();
        byte type = buffer.get();
        
        return new InternalKey(keyBytes, seq, type);
    }

    /**
     * Computes the total serialized size of this InternalKey in bytes.
     */
    public int serializedSize() {
        return 4 + userKey.length + 8 + 1;
    }

    @Override
    public int compareTo(InternalKey other) {
        Objects.requireNonNull(other, "other InternalKey cannot be null");
        
        // 1. Compare user keys lexicographically (unsigned byte comparison)
        int userKeyComparison = compareBytesUnsigned(this.userKey, other.userKey);
        if (userKeyComparison != 0) {
            return userKeyComparison;
        }

        // 2. Compare sequence numbers in descending order (higher/newer seq comes first)
        if (this.sequenceNumber != other.sequenceNumber) {
            return Long.compare(other.sequenceNumber, this.sequenceNumber);
        }

        // 3. Compare record type in descending order (PUT before TOMBSTONE if same seq)
        return Byte.compare(other.recordType, this.recordType);
    }

    private static int compareBytesUnsigned(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int result = Byte.toUnsignedInt(a[i]) - Byte.toUnsignedInt(b[i]);
            if (result != 0) {
                return result;
            }
        }
        return Integer.compare(a.length, b.length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InternalKey that = (InternalKey) o;
        return sequenceNumber == that.sequenceNumber &&
                recordType == that.recordType &&
                Arrays.equals(userKey, that.userKey);
    }

    private int computeHashCode() {
        int result = Arrays.hashCode(userKey);
        result = 31 * result + Long.hashCode(sequenceNumber);
        result = 31 * result + (int) recordType;
        return result;
    }

    @Override
    public int hashCode() {
        return cachedHashCode;
    }

    @Override
    public String toString() {
        return "InternalKey{" +
                "userKeyLength=" + userKey.length +
                ", sequenceNumber=" + sequenceNumber +
                ", recordType=" + (recordType == TYPE_PUT ? "PUT" : "TOMBSTONE") +
                '}';
    }
}