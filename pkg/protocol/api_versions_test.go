package protocol

import "testing"

func TestDisabledTransactionKeysAreNotAdvertised(t *testing.T) {
	for _, key := range DisabledTransactionAPIKeys {
		if SupportsKey(key) {
			t.Errorf("SupportsKey(%d) = true, want false (no transaction state)", key)
		}
		if got := MaxVersion(key); got != -1 {
			t.Errorf("MaxVersion(%d) = %d, want -1", key, got)
		}
	}
}

func TestAdvertisedKeysRemainSupported(t *testing.T) {
	for _, key := range []int16{
		APKProduce, APKFetch, APKListOffsets, APKMetadata, APKOffsetCommit,
		APKOffsetFetch, APKFindCoordinator, APKJoinGroup, APKHeartbeat,
		APKLeaveGroup, APKSyncGroup, APKDescribeGroups, APKListGroups,
		APKSaslHandshake, APKApiVersions, APKCreateTopics, APKDeleteTopics,
		APKInitProducerID, APKSaslAuthenticate, APKDeleteGroups,
	} {
		if !SupportsKey(key) {
			t.Errorf("SupportsKey(%d) = false, want true", key)
		}
		if MaxVersion(key) < 0 {
			t.Errorf("MaxVersion(%d) = %d, want >= 0", key, MaxVersion(key))
		}
	}
}

func TestApiVersionsResponseCarriesNoTransactionKeys(t *testing.T) {
	resp := &ApiVersionsResponse{
		Version:   0,
		ErrorCode: ErrNone,
		ApiKeys: []ApiKeySupport{
			{ApiKey: APKProduce, MinVersion: 0, MaxVersion: MaxVersion(APKProduce)},
			{ApiKey: APKInitProducerID, MinVersion: 0, MaxVersion: MaxVersion(APKInitProducerID)},
		},
	}
	body, err := EncodeApiVersionsResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeApiVersionsResponse(0, body)
	if err != nil {
		t.Fatal(err)
	}
	disabled := map[int16]bool{}
	for _, k := range DisabledTransactionAPIKeys {
		disabled[k] = true
	}
	for _, k := range decoded.ApiKeys {
		if disabled[k.ApiKey] {
			t.Errorf("encoded ApiVersions advertises disabled key %d", k.ApiKey)
		}
	}
}
