package protocol

const (
	APKProduce          int16 = 0
	APKFetch            int16 = 1
	APKListOffsets      int16 = 2
	APKMetadata         int16 = 3
	APKOffsetCommit     int16 = 8
	APKOffsetFetch      int16 = 9
	APKFindCoordinator  int16 = 10
	APKJoinGroup        int16 = 11
	APKHeartbeat        int16 = 12
	APKLeaveGroup       int16 = 13
	APKSyncGroup        int16 = 14
	APKDescribeGroups   int16 = 15
	APKListGroups       int16 = 16
	APKSaslHandshake    int16 = 17
	APKApiVersions      int16 = 18
	APKCreateTopics     int16 = 19
	APKDeleteTopics     int16 = 20
	APKInitProducerID   int16 = 22
	APKSaslAuthenticate int16 = 36
	APKDeleteGroups     int16 = 42
)

const (
	APKAddPartitionsToTxn int16 = 24
	APKAddOffsetsToTxn    int16 = 25
	APKEndTxn             int16 = 26
)

var DisabledTransactionAPIKeys = []int16{
	APKAddPartitionsToTxn,
	APKAddOffsetsToTxn,
	APKEndTxn,
}

const RecordMagic = int8(2)

const (
	ErrNone                               int16 = 0
	ErrUnknownServerError                 int16 = -1
	ErrOffsetOutOfRange                   int16 = 1
	ErrCorruptMessage                     int16 = 2
	ErrUnknownTopicOrPartition            int16 = 3
	ErrInvalidFetchSize                   int16 = 4
	ErrNotLeaderOrFollower                int16 = 6
	ErrRequestTimedOut                    int16 = 7
	ErrOffsetMetadataTooLarge             int16 = 12
	ErrCoordinatorLoadInProgress          int16 = 14
	ErrCoordinatorNotAvailable            int16 = 15
	ErrNotCoordinator                     int16 = 16
	ErrInvalidTopic                       int16 = 17
	ErrIllegalGeneration                  int16 = 22
	ErrInconsistentGroupProtocol          int16 = 23
	ErrUnknownMemberID                    int16 = 25
	ErrInvalidSessionTimeout              int16 = 26
	ErrRebalanceInProgress                int16 = 27
	ErrInvalidCommitOffsetSize            int16 = 28
	ErrTopicAuthorizationFailed           int16 = 29
	ErrGroupAuthorizationFailed           int16 = 30
	ErrClusterAuthorizationFailed         int16 = 31
	ErrIllegalSaslState                   int16 = 34
	ErrUnsupportedVersion                 int16 = 35
	ErrTopicAlreadyExists                 int16 = 36
	ErrInvalidPartitions                  int16 = 37
	ErrInvalidReplicationFactor           int16 = 38
	ErrInvalidReplicaAssignment           int16 = 39
	ErrInvalidRequest                     int16 = 42
	ErrUnsupportedForMessageFormat        int16 = 43
	ErrDuplicateSequenceNumber            int16 = 46
	ErrInvalidProducerEpoch               int16 = 47
	ErrInvalidTxnState                    int16 = 48
	ErrInvalidProducerIDMapping           int16 = 49
	ErrTransactionCoordinatorFenced       int16 = 52
	ErrTransactionalIDAuthorizationFailed int16 = 53
	ErrSaslAuthenticationFailed           int16 = 58
	ErrGroupIDNotFound                    int16 = 69
	ErrNonEmptyGroup                      int16 = 68
	ErrMemberIDRequired                   int16 = 79
)

var maxVersions = map[int16]int16{
	APKProduce:          3,
	APKFetch:            5,
	APKListOffsets:      5,
	APKMetadata:         8,
	APKOffsetCommit:     7,
	APKOffsetFetch:      5,
	APKFindCoordinator:  2,
	APKJoinGroup:        5,
	APKHeartbeat:        3,
	APKLeaveGroup:       3,
	APKSyncGroup:        3,
	APKDescribeGroups:   3,
	APKListGroups:       2,
	APKSaslHandshake:    1,
	APKApiVersions:      2,
	APKCreateTopics:     4,
	APKDeleteTopics:     3,
	APKInitProducerID:   2,
	APKSaslAuthenticate: 1,
	APKDeleteGroups:     1,
}

func SupportsKey(key int16) bool {
	_, ok := maxVersions[key]
	return ok
}

func MaxVersion(key int16) int16 {
	if v, ok := maxVersions[key]; ok {
		return v
	}
	return -1
}
