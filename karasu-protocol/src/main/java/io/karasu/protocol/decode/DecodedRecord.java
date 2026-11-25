/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.protocol.decode;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Algebraic hierarchy representing all decoded outcomes from Kafka's {@code __consumer_offsets} topic.
 */
public sealed interface DecodedRecord {

    record OffsetCommitRecord(
            String group,
            String topic,
            int partition,
            long offset,
            int leaderEpoch,
            String metadata,
            long commitTimestampMs,
            UUID topicId
    ) implements DecodedRecord {
    }

    record OffsetTombstoneRecord(
            String group,
            String topic,
            int partition
    ) implements DecodedRecord {
    }

    record MemberAssignment(
            String memberId,
            String groupInstanceId,
            String clientId,
            String clientHost,
            Map<String, int[]> assignment
    ) {
    }

    record ClassicGroupMetadataRecord(
            String group,
            String protocolType,
            int generation,
            List<MemberAssignment> members
    ) implements DecodedRecord {
    }

    /**
     * Key-2 tombstone: classic group metadata deleted (including during Classic -> KIP-848 online upgrade).
     * Does NOT imply committed partition offsets (Key 0/1) were deleted.
     */
    record ClassicGroupTombstoneRecord(
            String group
    ) implements DecodedRecord {
    }

    /**
     * Key-3 tombstone: KIP-848 consumer group metadata deleted.
     * Does NOT imply committed partition offsets (Key 0/1) were deleted.
     */
    record Kip848GroupTombstoneRecord(
            String group
    ) implements DecodedRecord {
    }

    record Kip848GroupMetadataRecord(
            String group
    ) implements DecodedRecord {
    }

    record Kip848MemberMetadataRecord(
            String group,
            String memberId,
            String instanceId,
            String clientId,
            String clientHost
    ) implements DecodedRecord {
    }

    record Kip848MemberTombstoneRecord(
            String group,
            String memberId
    ) implements DecodedRecord {
    }

    record TopicIdPartitions(
            UUID topicId,
            int[] partitions
    ) {
    }

    record Kip848CurrentAssignmentRecord(
            String group,
            String memberId,
            int memberEpoch,
            List<TopicIdPartitions> assignedPartitions
    ) implements DecodedRecord {
    }

    record Kip848AssignmentTombstoneRecord(
            String group,
            String memberId
    ) implements DecodedRecord {
    }

    record IgnoredInternalRecord(
            short recordType,
            String category
    ) implements DecodedRecord {
    }

    record DecodeFailureRecord(
            short recordType,
            String reason
    ) implements DecodedRecord {
    }
}
