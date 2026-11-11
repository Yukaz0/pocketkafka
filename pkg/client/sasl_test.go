package client

import (
	"bytes"
	"errors"
	"testing"

	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

func TestPlainTokenLayout(t *testing.T) {
	got := plainToken("admin", "admin-secret")
	want := []byte("\x00admin\x00admin-secret")
	if !bytes.Equal(got, want) {
		t.Fatalf("token = %q, want %q", got, want)
	}
}

func TestAuthenticateRejectsUnknownMechanism(t *testing.T) {
	c := &KafkaClient{sasl: SASL{Mechanism: "SCRAM-SHA-512", Username: "u", Password: "p"}}
	err := c.authenticate()
	if err == nil {
		t.Fatal("an unimplemented mechanism was accepted")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("SCRAM-SHA-512")) {
		t.Fatalf("error does not name the mechanism: %v", err)
	}
}

func TestAuthenticateIsSkippedWithoutMechanism(t *testing.T) {
	c := &KafkaClient{}
	if err := c.authenticate(); err != nil {
		t.Fatalf("empty mechanism returned %v, want no-op", err)
	}
}

func TestReadOnlyRefusesMutatingAPIs(t *testing.T) {
	ro := &KafkaClient{readOnly: true}
	for _, key := range []int16{
		protocol.APKProduce, protocol.APKCreateTopics, protocol.APKDeleteTopics,
		protocol.APKOffsetCommit, protocol.APKJoinGroup, protocol.APKSyncGroup,
		protocol.APKInitProducerID, protocol.APKDeleteGroups,
	} {
		if err := ro.checkAPI(key); !errors.Is(err, ErrReadOnly) {
			t.Errorf("api key %d was allowed on a read-only client: %v", key, err)
		}
	}
	for _, key := range []int16{
		protocol.APKMetadata, protocol.APKListOffsets, protocol.APKListGroups,
		protocol.APKDescribeGroups, protocol.APKOffsetFetch, protocol.APKFindCoordinator,
		protocol.APKApiVersions, protocol.APKSaslHandshake, protocol.APKSaslAuthenticate,
		protocol.APKFetch,
	} {
		if err := ro.checkAPI(key); err != nil {
			t.Errorf("api key %d was refused on a read-only client: %v", key, err)
		}
	}

	rw := &KafkaClient{}
	if err := rw.checkAPI(protocol.APKProduce); err != nil {
		t.Fatalf("a non-read-only client refused Produce: %v", err)
	}
}
