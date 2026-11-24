/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.protocol.decode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Defensive Big-Endian Kafka binary wire reader with strict bounds and allocation checks.
 */
public final class KafkaByteReader {

    public static final int DEFAULT_MAX_FIELD_BYTES = 1024 * 1024; // 1 MiB
    public static final int DEFAULT_MAX_ARRAY_ELEMENTS = 100_000;

    private final ByteBuffer buffer;
    private final int maxFieldBytes;

    public KafkaByteReader(byte[] data) {
        this(data, DEFAULT_MAX_FIELD_BYTES);
    }

    public KafkaByteReader(byte[] data, int maxFieldBytes) {
        this.buffer = ByteBuffer.wrap(data);
        this.maxFieldBytes = maxFieldBytes;
    }

    public int remaining() {
        return buffer.remaining();
    }

    public byte readInt8(String field) throws DecodeException {
        ensureRemaining(1, field);
        return buffer.get();
    }

    public short readInt16(String field) throws DecodeException {
        ensureRemaining(2, field);
        return buffer.getShort();
    }

    public int readInt32(String field) throws DecodeException {
        ensureRemaining(4, field);
        return buffer.getInt();
    }

    public long readInt64(String field) throws DecodeException {
        ensureRemaining(8, field);
        return buffer.getLong();
    }

    public UUID readUuid(String field) throws DecodeException {
        ensureRemaining(16, field);
        long msb = buffer.getLong();
        long lsb = buffer.getLong();
        return new UUID(msb, lsb);
    }

    /**
     * Reads an unsigned LEB128 varint (up to 32-bit unsigned value, max 5 bytes) as used by Kafka flexible versions.
     */
    public long readUnsignedVarint(String field) throws DecodeException {
        long value = 0L;
        int shift = 0;
        for (int i = 0; i < 5; i++) {
            if (buffer.remaining() < 1) {
                throw new DecodeException(field + ": truncated varint");
            }
            int b = buffer.get() & 0xFF;
            if (i == 4 && (b & 0xF0) != 0) {
                throw new DecodeException(field + ": varint overflow");
            }
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
            shift += 7;
        }
        throw new DecodeException(field + ": varint exceeds 5 bytes");
    }

    /**
     * Reads a Kafka non-flexible nullable string (int16 length, -1 means null).
     */
    public String readNullableString(String field) throws DecodeException {
        short len = readInt16(field + "_len");
        if (len == -1) {
            return null;
        }
        if (len < -1) {
            throw new DecodeException(field + ": invalid negative string length " + len);
        }
        return readUtf8Bytes(len, field);
    }

    /**
     * Reads a Kafka non-flexible string; maps null (-1) to empty string for Burrow compatibility.
     */
    public String readString(String field) throws DecodeException {
        String s = readNullableString(field);
        return s == null ? "" : s;
    }

    /**
     * Reads a Kafka flexible compact nullable string (uvarint len: 0=null, 1="", N=N-1 bytes).
     */
    public String readCompactNullableString(String field) throws DecodeException {
        long encodedLen = readUnsignedVarint(field + "_len");
        if (encodedLen == 0L) {
            return null;
        }
        if (encodedLen == 1L) {
            return "";
        }
        long byteLen = encodedLen - 1L;
        if (byteLen > maxFieldBytes || byteLen > buffer.remaining()) {
            throw new DecodeException(field + ": compact string length out of bounds (" + byteLen + ")");
        }
        return readUtf8Bytes((int) byteLen, field);
    }

    public String readCompactString(String field) throws DecodeException {
        String s = readCompactNullableString(field);
        return s == null ? "" : s;
    }

    /**
     * Reads a Kafka flexible compact array length (uvarint: 0=null, 1=empty, N=N-1 elements).
     * Returns -1 for null array.
     */
    public int readCompactArrayLength(String field) throws DecodeException {
        long encodedLen = readUnsignedVarint(field + "_count");
        if (encodedLen == 0L) {
            return -1;
        }
        long count = encodedLen - 1L;
        if (count > DEFAULT_MAX_ARRAY_ELEMENTS) {
            throw new DecodeException(field + ": compact array count exceeds limit (" + count + ")");
        }
        return (int) count;
    }

    public byte[] readBytes(String field) throws DecodeException {
        int len = readInt32(field + "_len");
        if (len == -1 || len == 0) {
            return new byte[0];
        }
        if (len < -1 || len > maxFieldBytes || len > buffer.remaining()) {
            throw new DecodeException(field + ": bytes length out of bounds (" + len + ")");
        }
        byte[] out = new byte[len];
        buffer.get(out);
        return out;
    }

    public void skipBytes(String field) throws DecodeException {
        int len = readInt32(field + "_len");
        if (len == -1 || len == 0) {
            return;
        }
        if (len < -1 || len > maxFieldBytes || len > buffer.remaining()) {
            throw new DecodeException(field + ": skip bytes length out of bounds (" + len + ")");
        }
        buffer.position(buffer.position() + len);
    }

    public byte[] readCompactBytes(String field) throws DecodeException {
        long encodedLen = readUnsignedVarint(field + "_len");
        if (encodedLen <= 1L) {
            return new byte[0];
        }
        long len = encodedLen - 1L;
        if (len > maxFieldBytes || len > buffer.remaining()) {
            throw new DecodeException(field + ": compact bytes length out of bounds (" + len + ")");
        }
        byte[] out = new byte[(int) len];
        buffer.get(out);
        return out;
    }

    public void skipCompactBytes(String field) throws DecodeException {
        long encodedLen = readUnsignedVarint(field + "_len");
        if (encodedLen <= 1L) {
            return;
        }
        long len = encodedLen - 1L;
        if (len > maxFieldBytes || len > buffer.remaining()) {
            throw new DecodeException(field + ": skip compact bytes length out of bounds (" + len + ")");
        }
        buffer.position(buffer.position() + (int) len);
    }

    public void skipTaggedFields(String field) throws DecodeException {
        if (buffer.remaining() == 0) {
            return;
        }
        long numTags = readUnsignedVarint(field + "_num_tags");
        if (numTags > 1024) {
            throw new DecodeException(field + ": excessive tagged fields (" + numTags + ")");
        }
        for (int i = 0; i < numTags; i++) {
            readUnsignedVarint(field + "_tag_id");
            long tagSize = readUnsignedVarint(field + "_tag_size");
            if (tagSize > maxFieldBytes || tagSize > buffer.remaining()) {
                throw new DecodeException(field + ": tagged field size out of bounds (" + tagSize + ")");
            }
            buffer.position(buffer.position() + (int) tagSize);
        }
    }

    public void skipRaw(int count, String field) throws DecodeException {
        if (count < 0 || count > buffer.remaining()) {
            throw new DecodeException(field + ": raw skip out of bounds (" + count + ")");
        }
        buffer.position(buffer.position() + count);
    }

    private String readUtf8Bytes(int len, String field) throws DecodeException {
        if (len == 0) {
            return "";
        }
        if (len > maxFieldBytes || len > buffer.remaining()) {
            throw new DecodeException(field + ": string length out of bounds (" + len + ")");
        }
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private void ensureRemaining(int needed, String field) throws DecodeException {
        if (buffer.remaining() < needed) {
            throw new DecodeException(field + ": buffer underflow (need " + needed + ", have " + buffer.remaining() + ")");
        }
    }

    public static final class DecodeException extends Exception {
        public DecodeException(String message) {
            super(message);
        }
    }
}
