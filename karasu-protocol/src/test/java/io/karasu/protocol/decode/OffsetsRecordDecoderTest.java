/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.protocol.decode;

import io.karasu.protocol.decode.DecodedRecord.ClassicGroupMetadataRecord;
import io.karasu.protocol.decode.DecodedRecord.ClassicGroupTombstoneRecord;
import io.karasu.protocol.decode.DecodedRecord.DecodeFailureRecord;
import io.karasu.protocol.decode.DecodedRecord.IgnoredInternalRecord;
import io.karasu.protocol.decode.DecodedRecord.Kip848AssignmentTombstoneRecord;
import io.karasu.protocol.decode.DecodedRecord.Kip848CurrentAssignmentRecord;
import io.karasu.protocol.decode.DecodedRecord.Kip848GroupMetadataRecord;
import io.karasu.protocol.decode.DecodedRecord.Kip848GroupTombstoneRecord;
import io.karasu.protocol.decode.DecodedRecord.Kip848MemberMetadataRecord;
import io.karasu.protocol.decode.DecodedRecord.Kip848MemberTombstoneRecord;
import io.karasu.protocol.decode.DecodedRecord.OffsetCommitRecord;
import io.karasu.protocol.decode.DecodedRecord.OffsetTombstoneRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class OffsetsRecordDecoderTest {

    private final OffsetsRecordDecoder decoder = new OffsetsRecordDecoder();

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4})
    void decodesAllOffsetCommitValueVersions(int version) throws Exception {
        byte[] key = encodeOffsetKey((short) 1, "group-a", "orders", 3);
        long expectedTs = 1793932555656L;
        UUID expectedTopicId = UUID.fromString("12345678-1234-5678-9abc-def012345678");
        byte[] value = encodeOffsetValue((short) version, 42L, 7, "meta-hello", expectedTs, expectedTopicId);

        DecodedRecord record = decoder.decode(key, value);
        assertThat(record).isInstanceOf(OffsetCommitRecord.class);
        OffsetCommitRecord commit = (OffsetCommitRecord) record;
        assertThat(commit.group()).isEqualTo("group-a");
        assertThat(commit.topic()).isEqualTo("orders");
        assertThat(commit.partition()).isEqualTo(3);
        assertThat(commit.offset()).isEqualTo(42L);
        assertThat(commit.commitTimestampMs()).isEqualTo(expectedTs);
        assertThat(commit.metadata()).isEqualTo("meta-hello");
        if (version >= 3) {
            assertThat(commit.leaderEpoch()).isEqualTo(7);
        }
        if (version == 4) {
            assertThat(commit.topicId()).isEqualTo(expectedTopicId);
        }
    }

    @Test
    void decodesV4LongAndMultibyteMetadataBoundaries() throws Exception {
        byte[] key = encodeOffsetKey((short) 1, "metadata-long", "lag-demo", 0);
        long expectedTs = 1793932555656L;
        List<String> samples = List.of(
                "",
                "x",
                "x".repeat(126),
                "x".repeat(127),
                "x".repeat(130),
                "汉".repeat(100),
                "x".repeat(4096)
        );
        for (String meta : samples) {
            byte[] value = encodeOffsetValue((short) 4, 123L, 1, meta, expectedTs, null);
            DecodedRecord record = decoder.decode(key, value);
            assertThat(record).isInstanceOf(OffsetCommitRecord.class);
            OffsetCommitRecord commit = (OffsetCommitRecord) record;
            assertThat(commit.offset()).isEqualTo(123L);
            assertThat(commit.commitTimestampMs()).isEqualTo(expectedTs);
            assertThat(commit.metadata()).isEqualTo(meta);
        }
    }

    @Test
    void rejectsMalformedCompactStringAndVarintWithoutThrowing() throws Exception {
        byte[] key = encodeOffsetKey((short) 1, "g", "t", 0);
        // Truncated varint in v4 metadata
        byte[] truncated = new byte[]{0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, (byte) 0x80};
        assertThat(decoder.decode(key, truncated)).isInstanceOf(DecodeFailureRecord.class);

        // Oversized varint claiming 130 bytes when only 1 byte follows
        byte[] underflow = new byte[]{0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, (byte) 0x83, 0x01, 'x'};
        assertThat(decoder.decode(key, underflow)).isInstanceOf(DecodeFailureRecord.class);

        // Overflow > 5 byte varint
        byte[] overflow = new byte[]{0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1,
                (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x01};
        assertThat(decoder.decode(key, overflow)).isInstanceOf(DecodeFailureRecord.class);

        // Negative standard string length (-2) in key
        byte[] badKey = new byte[]{0, 1, (byte) 0xff, (byte) 0xfe};
        assertThat(decoder.decode(badKey, null)).isInstanceOf(DecodeFailureRecord.class);
    }

    @Test
    void decodesOffsetTombstone() throws Exception {
        byte[] key = encodeOffsetKey((short) 1, "expired-group", "orders", 2);
        assertThat(decoder.decode(key, null))
                .isEqualTo(new OffsetTombstoneRecord("expired-group", "orders", 2));
        assertThat(decoder.decode(key, new byte[0]))
                .isEqualTo(new OffsetTombstoneRecord("expired-group", "orders", 2));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4})
    void decodesClassicGroupMetadataAllVersions(int version) throws Exception {
        byte[] key = encodeGroupKey((short) 2, "classic-group");
        byte[] value = encodeClassicGroupMetadata((short) version, "consumer", "member-1", "client-a", "/10.0.0.5", "orders", new int[]{0, 2});

        DecodedRecord record = decoder.decode(key, value);
        assertThat(record).isInstanceOf(ClassicGroupMetadataRecord.class);
        ClassicGroupMetadataRecord meta = (ClassicGroupMetadataRecord) record;
        assertThat(meta.group()).isEqualTo("classic-group");
        assertThat(meta.protocolType()).isEqualTo("consumer");
        assertThat(meta.members()).hasSize(1);
        assertThat(meta.members().get(0).clientId()).isEqualTo("client-a");
        assertThat(meta.members().get(0).clientHost()).isEqualTo("/10.0.0.5");
        assertThat(meta.members().get(0).assignment()).containsKey("orders");
        assertThat(meta.members().get(0).assignment().get("orders")).containsExactly(0, 2);
    }

    @Test
    void decodesDistinctGroupTombstonesForClassicAndKip848() throws Exception {
        assertThat(decoder.decode(encodeGroupKey((short) 2, "g-classic"), null))
                .isEqualTo(new ClassicGroupTombstoneRecord("g-classic"));
        assertThat(decoder.decode(encodeGroupKey((short) 3, "g-modern"), null))
                .isEqualTo(new Kip848GroupTombstoneRecord("g-modern"));
        assertThat(decoder.decode(encodeGroupKey((short) 3, "g-modern"), new byte[]{0, 0, 0, 0, 0, 1}))
                .isEqualTo(new Kip848GroupMetadataRecord("g-modern"));
    }

    @Test
    void decodesKip848MemberAndCurrentAssignmentAndTombstones() throws Exception {
        byte[] memberKey = encodeGroupMemberKey((short) 5, "modern-group", "m-1");
        ByteArrayOutputStream memberVal = new ByteArrayOutputStream();
        DataOutputStream mdos = new DataOutputStream(memberVal);
        mdos.writeShort(0); // value version 0
        writeCompactString(mdos, "inst-1");
        writeCompactString(mdos, null); // rackId
        writeCompactString(mdos, "modern-client");
        writeCompactString(mdos, "/10.1.2.3");
        writeUvarint(mdos, 1); // empty subscribedTopicNames
        writeCompactString(mdos, null); // regex
        mdos.writeInt(30000);
        writeCompactString(mdos, null);
        writeUvarint(mdos, 0); // tagged fields

        DecodedRecord memberRec = decoder.decode(memberKey, memberVal.toByteArray());
        assertThat(memberRec).isEqualTo(new Kip848MemberMetadataRecord(
                "modern-group", "m-1", "inst-1", "modern-client", "/10.1.2.3"));
        assertThat(decoder.decode(memberKey, null))
                .isEqualTo(new Kip848MemberTombstoneRecord("modern-group", "m-1"));

        UUID topicId = UUID.randomUUID();
        byte[] assignKey = encodeGroupMemberKey((short) 8, "modern-group", "m-1");
        ByteArrayOutputStream assignVal = new ByteArrayOutputStream();
        DataOutputStream ados = new DataOutputStream(assignVal);
        ados.writeShort(0); // value version 0
        ados.writeInt(5);   // memberEpoch
        ados.writeInt(4);   // previousMemberEpoch
        ados.writeByte(0);  // state
        writeUvarint(ados, 2); // 1 topic entry (count+1)
        ados.writeLong(topicId.getMostSignificantBits());
        ados.writeLong(topicId.getLeastSignificantBits());
        writeUvarint(ados, 3); // 2 partitions (count+1)
        ados.writeInt(1);
        ados.writeInt(4);
        writeUvarint(ados, 0); // TopicPartitions tagged fields
        writeUvarint(ados, 1); // empty pendingRevocation
        writeUvarint(ados, 0); // top-level tagged fields

        DecodedRecord assignRec = decoder.decode(assignKey, assignVal.toByteArray());
        assertThat(assignRec).isInstanceOf(Kip848CurrentAssignmentRecord.class);
        Kip848CurrentAssignmentRecord parsed = (Kip848CurrentAssignmentRecord) assignRec;
        assertThat(parsed.group()).isEqualTo("modern-group");
        assertThat(parsed.memberId()).isEqualTo("m-1");
        assertThat(parsed.memberEpoch()).isEqualTo(5);
        assertThat(parsed.assignedPartitions()).hasSize(1);
        assertThat(parsed.assignedPartitions().get(0).topicId()).isEqualTo(topicId);
        assertThat(parsed.assignedPartitions().get(0).partitions()).containsExactly(1, 4);

        assertThat(decoder.decode(assignKey, null))
                .isEqualTo(new Kip848AssignmentTombstoneRecord("modern-group", "m-1"));
    }

    @Test
    void silentlyIgnoresKnownAuxiliaryAndShareAndStreamsKeys() {
        for (short keyType : new short[]{4, 6, 7, 10, 11, 12, 13, 14, 15, 16, 17, 19, 20, 21, 22, 23}) {
            byte[] key = new byte[]{(byte) (keyType >> 8), (byte) keyType, 0, 1, 'g'};
            DecodedRecord record = decoder.decode(key, new byte[]{0, 0});
            assertThat(record).isInstanceOf(IgnoredInternalRecord.class);
        }
        byte[] unknownKey = new byte[]{0, 99, 0, 1, 'g'};
        assertThat(decoder.decode(unknownKey, new byte[]{0, 0})).isInstanceOf(DecodeFailureRecord.class);
    }

    // --- Encoding test helpers ---

    private static byte[] encodeOffsetKey(short keyType, String group, String topic, int partition) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeShort(keyType);
        writeStdString(dos, group);
        writeStdString(dos, topic);
        dos.writeInt(partition);
        return out.toByteArray();
    }

    private static byte[] encodeOffsetValue(short version, long offset, int leaderEpoch, String metadata, long ts, UUID topicId) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeShort(version);
        dos.writeLong(offset);
        if (version >= 3) {
            dos.writeInt(leaderEpoch);
        }
        if (version >= 4) {
            writeCompactString(dos, metadata);
        } else {
            writeStdString(dos, metadata);
        }
        dos.writeLong(ts);
        if (version == 1) {
            dos.writeLong(-1L);
        }
        if (version >= 4) {
            if (topicId != null) {
                writeUvarint(dos, 1); // 1 tagged field
                writeUvarint(dos, 0); // tag 0 = topicId
                writeUvarint(dos, 16);
                dos.writeLong(topicId.getMostSignificantBits());
                dos.writeLong(topicId.getLeastSignificantBits());
            } else {
                writeUvarint(dos, 0);
            }
        }
        return out.toByteArray();
    }

    private static byte[] encodeGroupKey(short keyType, String group) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeShort(keyType);
        writeStdString(dos, group);
        return out.toByteArray();
    }

    private static byte[] encodeGroupMemberKey(short keyType, String group, String memberId) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeShort(keyType);
        writeStdString(dos, group);
        writeStdString(dos, memberId);
        return out.toByteArray();
    }

    private static byte[] encodeClassicGroupMetadata(short version, String protocolType, String memberId,
                                                     String clientId, String clientHost, String topic, int[] partitions) throws IOException {
        boolean flexible = version >= 4;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeShort(version);
        if (flexible) {
            writeCompactString(dos, protocolType);
        } else {
            writeStdString(dos, protocolType);
        }
        dos.writeInt(3); // generation
        if (flexible) {
            writeCompactString(dos, "range");
            writeCompactString(dos, memberId);
        } else {
            writeStdString(dos, "range");
            writeStdString(dos, memberId);
        }
        if (version >= 2) {
            dos.writeLong(1793932555000L);
        }
        if (flexible) {
            writeUvarint(dos, 2); // 1 member + 1
        } else {
            dos.writeInt(1);
        }
        // Member
        if (flexible) {
            writeCompactString(dos, memberId);
            writeCompactString(dos, null); // groupInstanceId
            writeCompactString(dos, clientId);
            writeCompactString(dos, clientHost);
        } else {
            writeStdString(dos, memberId);
            if (version >= 3) {
                dos.writeShort(-1);
            }
            writeStdString(dos, clientId);
            writeStdString(dos, clientHost);
        }
        if (version >= 1) {
            dos.writeInt(30000); // rebalanceTimeout
        }
        dos.writeInt(10000); // sessionTimeout
        if (flexible) {
            writeUvarint(dos, 1); // empty subscription
        } else {
            dos.writeInt(0);
        }
        // ConsumerProtocolAssignment bytes
        ByteArrayOutputStream assignOut = new ByteArrayOutputStream();
        DataOutputStream ados = new DataOutputStream(assignOut);
        ados.writeShort(0); // ConsumerProtocol v0
        ados.writeInt(1);   // 1 topic
        writeStdString(ados, topic);
        ados.writeInt(partitions.length);
        for (int p : partitions) {
            ados.writeInt(p);
        }
        ados.writeInt(0); // userData
        byte[] assignBytes = assignOut.toByteArray();
        if (flexible) {
            writeUvarint(dos, assignBytes.length + 1);
            dos.write(assignBytes);
            writeUvarint(dos, 0); // member tagged fields
            writeUvarint(dos, 0); // top-level tagged fields
        } else {
            dos.writeInt(assignBytes.length);
            dos.write(assignBytes);
        }
        return out.toByteArray();
    }

    private static void writeStdString(DataOutputStream dos, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        dos.writeShort(b.length);
        dos.write(b);
    }

    private static void writeCompactString(DataOutputStream dos, String s) throws IOException {
        if (s == null) {
            writeUvarint(dos, 0);
            return;
        }
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        writeUvarint(dos, b.length + 1);
        dos.write(b);
    }

    private static void writeUvarint(DataOutputStream dos, int value) throws IOException {
        int v = value;
        while ((v & ~0x7F) != 0) {
            dos.writeByte((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        dos.writeByte(v & 0x7F);
    }
}
