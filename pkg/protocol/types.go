// Package protocol is a hand-written, zero external dependency implementation
// of the Apache Kafka binary wire protocol. It provides BigEndian binary
// readers/writers, frame slicing, a RecordBatch v2 codec with Castagnoli
// CRC-32C, and request/response codecs for the API keys the broker supports.
package protocol

// Kafka API keys (ApiKey values in the request header).
const (
	APKProduce         int16 = 0
	APKFetch           int16 = 1
	APKListOffsets     int16 = 2
	APKMetadata        int16 = 3
	APKOffsetCommit    int16 = 8
	APKOffsetFetch     int16 = 9
	APKFindCoordinator int16 = 10
	APKJoinGroup       int16 = 11
	APKHeartbeat       int16 = 12
	APKLeaveGroup      int16 = 13
	APKSyncGroup       int16 = 14
	APKApiVersions     int16 = 18
	APKCreateTopics    int16 = 19
	APKDeleteTopics    int16 = 20
)

// RecordBatch magic byte for the modern (v2) record format.
const RecordMagic = int8(2)

// Error codes used by the broker engine (see docs/ERROR_CODES_AND_PROTOCOLS.md).
const (
	ErrNone                        int16 = 0
	ErrUnknownServerError          int16 = -1
	ErrOffsetOutOfRange            int16 = 1
	ErrCorruptMessage              int16 = 2
	ErrUnknownTopicOrPartition     int16 = 3
	ErrInvalidFetchSize            int16 = 4
	ErrNotLeaderOrFollower         int16 = 6
	ErrRequestTimedOut             int16 = 7
	ErrOffsetMetadataTooLarge      int16 = 12
	ErrCoordinatorLoadInProgress   int16 = 14
	ErrCoordinatorNotAvailable     int16 = 15
	ErrNotCoordinator              int16 = 16
	ErrInvalidTopic                int16 = 17
	ErrIllegalGeneration           int16 = 22
	ErrInconsistentGroupProtocol   int16 = 23
	ErrUnknownMemberID             int16 = 25
	ErrInvalidSessionTimeout       int16 = 26
	ErrRebalanceInProgress         int16 = 27
	ErrInvalidCommitOffsetSize     int16 = 28
	ErrTopicAuthorizationFailed    int16 = 29
	ErrGroupAuthorizationFailed    int16 = 30
	ErrUnsupportedVersion          int16 = 35
	ErrTopicAlreadyExists          int16 = 36
	ErrInvalidPartitions           int16 = 37
	ErrInvalidReplicationFactor    int16 = 38
	ErrInvalidReplicaAssignment    int16 = 39
	ErrInvalidRequest              int16 = 42
	ErrUnsupportedForMessageFormat int16 = 43
	ErrMemberIDRequired            int16 = 79
)

// maxVersions advertises the maximum API version the broker supports for each
// key. All chosen maxima are at or below the "flexible" protocol threshold, so
// the codecs only need to handle the classic (non-compact) wire format.
var maxVersions = map[int16]int16{
	APKProduce:         3, // v3 makes clients use RecordBatch v2 (magic 2)
	APKFetch:           5,
	APKListOffsets:     5,
	APKMetadata:        8,
	APKOffsetCommit:    7,
	APKOffsetFetch:     5, // v6+ is flexible
	APKFindCoordinator: 2, // v3+ is flexible
	APKJoinGroup:       5, // v6+ is flexible (cooperative-sticky needs v5)
	APKHeartbeat:       3,
	APKLeaveGroup:      3,
	APKSyncGroup:       3, // v4+ is flexible
	APKApiVersions:     2,
	APKCreateTopics:    4,
	APKDeleteTopics:    3,
}

// SupportsKey reports whether the broker implements the given API key.
func SupportsKey(key int16) bool {
	_, ok := maxVersions[key]
	return ok
}

// MaxVersion returns the maximum supported version for a key, or -1 if unknown.
func MaxVersion(key int16) int16 {
	if v, ok := maxVersions[key]; ok {
		return v
	}
	return -1
}
