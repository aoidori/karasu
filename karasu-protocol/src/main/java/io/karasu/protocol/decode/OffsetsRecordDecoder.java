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
import io.karasu.protocol.decode.DecodedRecord.MemberAssignment;
import io.karasu.protocol.decode.DecodedRecord.OffsetCommitRecord;
import io.karasu.protocol.decode.DecodedRecord.OffsetTombstoneRecord;
import io.karasu.protocol.decode.DecodedRecord.TopicIdPartitions;
import io.karasu.protocol.decode.KafkaByteReader.DecodeException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Stateless, thread-safe binary decoder for Kafka's internal {@code __consumer_offsets} topic.
 * Supports Kafka 3.9 through 4.2 schemas including:
 * <ul>
 *   <li>Offset commits (Key 0/1, Value v0-v4 including flexible compact-string metadata and tagged topicId)</li>
 *   <li>Offset tombstones (Key 0/1 with null/empty value)</li>
 *   <li>Classic group metadata (Key 2, Value v0-v4 including flexible v4 and tombstones)</li>
 *   <li>KIP-848 modern consumer group metadata, members, and current assignments (Keys 3, 5, 8 and tombstones)</li>
 *   <li>Silent recognition of auxiliary KIP-848, Share Group, and Streams Group coordinator records</li>
 * </ul>
 */
public final class OffsetsRecordDecoder {

    public static final short KEY_LEGACY_OFFSET_COMMIT = 0;
    public static final short KEY_OFFSET_COMMIT = 1;
    public static final short KEY_GROUP_METADATA = 2;
    public static final short KEY_CONSUMER_GROUP_METADATA = 3;
    public static final short KEY_CONSUMER_GROUP_PARTITION_METADATA = 4;
    public static final short KEY_CONSUMER_GROUP_MEMBER_METADATA = 5;
    public static final short KEY_CONSUMER_GROUP_TARGET_ASSIGNMENT_METADATA = 6;
    public static final short KEY_CONSUMER_GROUP_TARGET_ASSIGNMENT_MEMBER = 7;
    public static final short KEY_CONSUMER_GROUP_CURRENT_MEMBER_ASSIGNMENT = 8;
    public static final short KEY_CONSUMER_GROUP_REGULAR_EXPRESSION = 16;

    private final int maxFieldBytes;

    public OffsetsRecordDecoder() {
        this(KafkaByteReader.DEFAULT_MAX_FIELD_BYTES);
    }

    public OffsetsRecordDecoder(int maxFieldBytes) {
        this.maxFieldBytes = maxFieldBytes;
    }

    public DecodedRecord decode(byte[] key, byte[] value) {
        if (key == null || key.length < 2) {
            return new DecodeFailureRecord((short) -1, "missing_key_version");
        }
        KafkaByteReader keyReader = new KafkaByteReader(key, maxFieldBytes);
        short keyType;
        try {
            keyType = keyReader.readInt16("key_version");
        } catch (DecodeException e) {
            return new DecodeFailureRecord((short) -1, e.getMessage());
        }

        try {
            return switch (keyType) {
                case KEY_LEGACY_OFFSET_COMMIT, KEY_OFFSET_COMMIT -> decodeOffsetCommit(keyType, keyReader, value);
                case KEY_GROUP_METADATA -> decodeClassicGroupMetadata(keyType, keyReader, value);
                case KEY_CONSUMER_GROUP_METADATA -> decodeKip848GroupMetadata(keyReader, value);
                case KEY_CONSUMER_GROUP_MEMBER_METADATA -> decodeKip848MemberMetadata(keyType, keyReader, value);
                case KEY_CONSUMER_GROUP_CURRENT_MEMBER_ASSIGNMENT -> decodeKip848CurrentAssignment(keyType, keyReader, value);
                case KEY_CONSUMER_GROUP_PARTITION_METADATA,
                        KEY_CONSUMER_GROUP_TARGET_ASSIGNMENT_METADATA,
                        KEY_CONSUMER_GROUP_TARGET_ASSIGNMENT_MEMBER,
                        KEY_CONSUMER_GROUP_REGULAR_EXPRESSION -> new IgnoredInternalRecord(keyType, "kip848_aux");
                case 10, 11, 12, 13, 14, 15 -> new IgnoredInternalRecord(keyType, "share_group");
                case 17, 19, 20, 21, 22, 23 -> new IgnoredInternalRecord(keyType, "streams_group");
                default -> new DecodeFailureRecord(keyType, "unsupported_key_version");
            };
        } catch (DecodeException e) {
            return new DecodeFailureRecord(keyType, e.getMessage());
        }
    }

    private DecodedRecord decodeOffsetCommit(short keyType, KafkaByteReader keyReader, byte[] value) throws DecodeException {
        String group = keyReader.readString("offset_key_group");
        String topic = keyReader.readString("offset_key_topic");
        int partition = keyReader.readInt32("offset_key_partition");
        if (partition < 0) {
            return new DecodeFailureRecord(keyType, "negative_partition");
        }
        if (value == null || value.length == 0) {
            return new OffsetTombstoneRecord(group, topic, partition);
        }

        KafkaByteReader valReader = new KafkaByteReader(value, maxFieldBytes);
        short valVersion = valReader.readInt16("offset_value_version");
        return switch (valVersion) {
            case 0, 1, 2 -> {
                long offset = valReader.readInt64("offset");
                String metadata = valReader.readString("metadata");
                long commitTimestamp = valReader.readInt64("commit_timestamp");
                if (valVersion == 1 && valReader.remaining() >= 8) {
                    valReader.readInt64("expire_timestamp");
                }
                yield new OffsetCommitRecord(group, topic, partition, offset, -1, metadata, commitTimestamp, null);
            }
            case 3 -> {
                long offset = valReader.readInt64("offset");
                int leaderEpoch = valReader.readInt32("leader_epoch");
                String metadata = valReader.readString("metadata");
                long commitTimestamp = valReader.readInt64("commit_timestamp");
                yield new OffsetCommitRecord(group, topic, partition, offset, leaderEpoch, metadata, commitTimestamp, null);
            }
            case 4 -> {
                long offset = valReader.readInt64("offset");
                int leaderEpoch = valReader.readInt32("leader_epoch");
                String metadata = valReader.readCompactString("metadata");
                long commitTimestamp = valReader.readInt64("commit_timestamp");
                UUID topicId = readOffsetCommitV4TaggedTopicId(valReader);
                yield new OffsetCommitRecord(group, topic, partition, offset, leaderEpoch, metadata, commitTimestamp, topicId);
            }
            default -> new DecodeFailureRecord(keyType, "unsupported_offset_value_version_" + valVersion);
        };
    }

    private UUID readOffsetCommitV4TaggedTopicId(KafkaByteReader reader) throws DecodeException {
        if (reader.remaining() == 0) {
            return null;
        }
        long numTags = reader.readUnsignedVarint("offset_v4_num_tags");
        if (numTags > 1024) {
            throw new DecodeException("offset_v4: excessive tagged fields " + numTags);
        }
        UUID topicId = null;
        for (int i = 0; i < numTags; i++) {
            long tag = reader.readUnsignedVarint("offset_v4_tag");
            long size = reader.readUnsignedVarint("offset_v4_tag_size");
            if (size > maxFieldBytes || size > reader.remaining()) {
                throw new DecodeException("offset_v4: tag size out of bounds " + size);
            }
            if (tag == 0L && size == 16L) {
                topicId = reader.readUuid("offset_v4_topic_id");
            } else {
                reader.skipRaw((int) size, "offset_v4_unknown_tag");
            }
        }
        return topicId;
    }

    private DecodedRecord decodeClassicGroupMetadata(short keyType, KafkaByteReader keyReader, byte[] value) throws DecodeException {
        String group = keyReader.readString("group_metadata_key_group");
        if (value == null || value.length == 0) {
            return new ClassicGroupTombstoneRecord(group);
        }

        KafkaByteReader valReader = new KafkaByteReader(value, maxFieldBytes);
        short valVersion = valReader.readInt16("group_metadata_value_version");
        boolean flexible = valVersion >= 4;
        if (valVersion < 0 || valVersion > 4) {
            return new DecodeFailureRecord(keyType, "unsupported_group_metadata_version_" + valVersion);
        }

        String protocolType = flexible
                ? valReader.readCompactString("protocol_type")
                : valReader.readString("protocol_type");
        int generation = valReader.readInt32("generation");
        // protocol & leader
        if (flexible) {
            valReader.readCompactNullableString("protocol");
            valReader.readCompactNullableString("leader");
        } else {
            valReader.readNullableString("protocol");
            valReader.readNullableString("leader");
        }
        if (valVersion >= 2) {
            valReader.readInt64("current_state_timestamp");
        }

        int memberCount = flexible
                ? Math.max(0, valReader.readCompactArrayLength("members"))
                : valReader.readInt32("members_count");
        if (memberCount < 0 || memberCount > KafkaByteReader.DEFAULT_MAX_ARRAY_ELEMENTS) {
            throw new DecodeException("invalid_member_count_" + memberCount);
        }

        boolean isConsumerProtocol = "consumer".equals(protocolType);
        if (!isConsumerProtocol || memberCount == 0) {
            return new ClassicGroupMetadataRecord(group, protocolType, generation, List.of());
        }

        List<MemberAssignment> members = new ArrayList<>(memberCount);
        for (int i = 0; i < memberCount; i++) {
            String memberId = flexible ? valReader.readCompactString("member_id") : valReader.readString("member_id");
            String instanceId = "";
            if (valVersion >= 3) {
                String rawInst = flexible
                        ? valReader.readCompactNullableString("group_instance_id")
                        : valReader.readNullableString("group_instance_id");
                instanceId = rawInst == null ? "" : rawInst;
            }
            String clientId = flexible ? valReader.readCompactString("client_id") : valReader.readString("client_id");
            String clientHost = flexible ? valReader.readCompactString("client_host") : valReader.readString("client_host");
            if (valVersion >= 1) {
                valReader.readInt32("rebalance_timeout");
            }
            valReader.readInt32("session_timeout");
            if (flexible) {
                valReader.skipCompactBytes("subscription");
            } else {
                valReader.skipBytes("subscription");
            }
            byte[] assignmentBytes = flexible
                    ? valReader.readCompactBytes("assignment")
                    : valReader.readBytes("assignment");
            if (flexible) {
                valReader.skipTaggedFields("member_tags");
            }

            Map<String, int[]> assignment = decodeClassicConsumerAssignment(assignmentBytes);
            members.add(new MemberAssignment(memberId, instanceId, clientId, clientHost, assignment));
        }
        return new ClassicGroupMetadataRecord(group, protocolType, generation, Collections.unmodifiableList(members));
    }

    private Map<String, int[]> decodeClassicConsumerAssignment(byte[] assignmentBytes) throws DecodeException {
        if (assignmentBytes == null || assignmentBytes.length == 0) {
            return Map.of();
        }
        KafkaByteReader reader = new KafkaByteReader(assignmentBytes, maxFieldBytes);
        short version = reader.readInt16("consumer_protocol_version");
        if (version < 0) {
            throw new DecodeException("negative_consumer_protocol_version_" + version);
        }
        int numTopics = reader.readInt32("assignment_topic_count");
        if (numTopics < 0 || numTopics > KafkaByteReader.DEFAULT_MAX_ARRAY_ELEMENTS) {
            throw new DecodeException("invalid_assignment_topic_count_" + numTopics);
        }
        Map<String, int[]> topics = new LinkedHashMap<>(numTopics);
        for (int i = 0; i < numTopics; i++) {
            String topicName = reader.readString("assignment_topic_name");
            int numPartitions = reader.readInt32("assignment_partition_count");
            if (numPartitions < 0 || numPartitions > KafkaByteReader.DEFAULT_MAX_ARRAY_ELEMENTS) {
                throw new DecodeException("invalid_assignment_partition_count_" + numPartitions);
            }
            int[] partitions = new int[numPartitions];
            for (int j = 0; j < numPartitions; j++) {
                partitions[j] = reader.readInt32("assignment_partition_id");
            }
            topics.put(topicName, partitions);
        }
        return Collections.unmodifiableMap(topics);
    }

    private DecodedRecord decodeKip848GroupMetadata(KafkaByteReader keyReader, byte[] value) throws DecodeException {
        String groupId = keyReader.readString("kip848_group_id");
        if (value == null || value.length == 0) {
            return new Kip848GroupTombstoneRecord(groupId);
        }
        return new Kip848GroupMetadataRecord(groupId);
    }

    private DecodedRecord decodeKip848MemberMetadata(short keyType, KafkaByteReader keyReader, byte[] value) throws DecodeException {
        String groupId = keyReader.readString("kip848_member_group_id");
        String memberId = keyReader.readString("kip848_member_id");
        if (value == null || value.length == 0) {
            return new Kip848MemberTombstoneRecord(groupId, memberId);
        }

        KafkaByteReader valReader = new KafkaByteReader(value, maxFieldBytes);
        short valVersion = valReader.readInt16("kip848_member_value_version");
        if (valVersion < 0) {
            return new DecodeFailureRecord(keyType, "invalid_kip848_member_version_" + valVersion);
        }
        // ConsumerGroupMemberMetadataValue (flexible v0+):
        // InstanceId, RackId, ClientId, ClientHost, ...
        String instanceId = valReader.readCompactNullableString("instance_id");
        valReader.readCompactNullableString("rack_id");
        String clientId = valReader.readCompactString("client_id");
        String clientHost = valReader.readCompactString("client_host");
        return new Kip848MemberMetadataRecord(
                groupId,
                memberId,
                instanceId == null ? "" : instanceId,
                clientId,
                clientHost
        );
    }

    private DecodedRecord decodeKip848CurrentAssignment(short keyType, KafkaByteReader keyReader, byte[] value) throws DecodeException {
        String groupId = keyReader.readString("kip848_assign_group_id");
        String memberId = keyReader.readString("kip848_assign_member_id");
        if (value == null || value.length == 0) {
            return new Kip848AssignmentTombstoneRecord(groupId, memberId);
        }

        KafkaByteReader valReader = new KafkaByteReader(value, maxFieldBytes);
        short valVersion = valReader.readInt16("kip848_assign_value_version");
        if (valVersion < 0) {
            return new DecodeFailureRecord(keyType, "invalid_kip848_assign_version_" + valVersion);
        }
        // ConsumerGroupCurrentMemberAssignmentValue (flexible v0+):
        // MemberEpoch(int32), PreviousMemberEpoch(int32), State(int8), AssignedPartitions([]TopicPartitions)
        int memberEpoch = valReader.readInt32("member_epoch");
        valReader.readInt32("previous_member_epoch");
        valReader.readInt8("member_state");

        int topicCount = valReader.readCompactArrayLength("assigned_partitions");
        if (topicCount <= 0) {
            return new Kip848CurrentAssignmentRecord(groupId, memberId, memberEpoch, List.of());
        }
        List<TopicIdPartitions> assigned = new ArrayList<>(topicCount);
        for (int i = 0; i < topicCount; i++) {
            UUID topicId = valReader.readUuid("assigned_topic_id");
            int partCount = valReader.readCompactArrayLength("assigned_partition_ids");
            int[] partitions;
            if (partCount <= 0) {
                partitions = new int[0];
            } else {
                partitions = new int[partCount];
                for (int j = 0; j < partCount; j++) {
                    partitions[j] = valReader.readInt32("assigned_partition_id");
                }
            }
            valReader.skipTaggedFields("topic_partitions_tags");
            assigned.add(new TopicIdPartitions(topicId, partitions));
        }
        return new Kip848CurrentAssignmentRecord(groupId, memberId, memberEpoch, Collections.unmodifiableList(assigned));
    }
}
