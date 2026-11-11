package protocol

import "testing"

var flexibleFrom = map[int16]int16{
	APKProduce:          9,
	APKFetch:            12,
	APKListOffsets:      6,
	APKMetadata:         9,
	APKOffsetCommit:     8,
	APKOffsetFetch:      6,
	APKFindCoordinator:  3,
	APKJoinGroup:        6,
	APKHeartbeat:        4,
	APKLeaveGroup:       4,
	APKSyncGroup:        4,
	APKDescribeGroups:   5,
	APKListGroups:       3,
	APKApiVersions:      3,
	APKCreateTopics:     5,
	APKDeleteTopics:     4,
	APKInitProducerID:   3,
	APKSaslAuthenticate: 2,
	APKDeleteGroups:     2,
}

func TestMaxVersionsStayClassic(t *testing.T) {
	for key, max := range maxVersions {
		flexible, known := flexibleFrom[key]
		if !known {
			if key == APKSaslHandshake {
				continue
			}
			t.Errorf("api key %d has no flexible threshold recorded; add it to this test", key)
			continue
		}
		if max >= flexible {
			t.Errorf("api key %d advertised as max v%d, but flexible starts at v%d", key, max, flexible)
		}
	}
}

func TestMaxVersionsCoversKnownAPIs(t *testing.T) {
	for key := range flexibleFrom {
		if MaxVersion(key) < 0 {
			t.Errorf("api key %d has a flexible threshold but no advertised maximum", key)
		}
	}
}

func TestClassicListGroupsEmptyBody(t *testing.T) {
	if MaxVersion(APKListGroups) != 2 {
		t.Fatalf("ListGroups max version = %d, want 2 (v3 is flexible)", MaxVersion(APKListGroups))
	}
	body, err := EncodeListGroupsRequest(&ListGroupsRequest{Version: MaxVersion(APKListGroups)})
	if err != nil {
		t.Fatal(err)
	}
	if len(body) != 0 {
		t.Fatalf("classic ListGroups v2 body = %d bytes, want an empty body", len(body))
	}
}
