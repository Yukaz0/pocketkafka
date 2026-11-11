package web

import (
	"net/http"
	"strings"
	"testing"

	"github.com/Yukaz0/pocketkafka/internal/authz"
	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

func assignmentBytes(topics ...string) []byte {
	w := protocol.NewWriter(32)
	w.WriteArrayLen(len(topics))
	for _, topic := range topics {
		w.WriteString(topic)
		w.WriteArrayLen(1)
		w.WriteInt32(0)
	}
	return w.Bytes()
}

func TestGroupHandlersScopePerTopicRows(t *testing.T) {
	f := newAuthzFixture(t, []authz.Rule{
		{Principal: "groupAdmin", ResourceType: authz.ResourceGroup, ResourceName: "g1", Operations: []string{"Admin"}},
		{Principal: "groupAdmin", ResourceType: authz.ResourceTopic, ResourceName: "orders", Operations: []string{"Describe"}},
		{Principal: "clusterAdmin", ResourceType: authz.ResourceCluster, ResourceName: "*", Operations: []string{"Admin"}},
	})
	join := f.server.gm.JoinGroup(&protocol.JoinGroupRequest{
		Version:            5,
		Group:              "g1",
		SessionTimeoutMs:   3000,
		RebalanceTimeoutMs: 3000,
		ProtocolType:       "consumer",
		Protocols:          []protocol.JoinGroupRequestProtocol{{Name: "range", Metadata: []byte("m")}},
	})
	if join.ErrorCode != protocol.ErrNone {
		t.Fatalf("join group: error code %d", join.ErrorCode)
	}
	if resp := f.server.gm.SyncGroup(&protocol.SyncGroupRequest{
		Version:      5,
		Group:        "g1",
		GenerationID: join.GenerationID,
		MemberID:     join.MemberID,
		Assignments: []protocol.SyncGroupRequestAssignment{{
			MemberID:   join.MemberID,
			Assignment: assignmentBytes("orders", "secrets"),
		}},
	}); resp.ErrorCode != protocol.ErrNone {
		t.Fatalf("sync group: error code %d", resp.ErrorCode)
	}
	for _, topic := range []string{"orders", "secrets"} {
		if err := f.server.gm.ResetOffsets("g1", topic, 0, 42); err != nil {
			t.Fatalf("seed offsets for %s: %v", topic, err)
		}
	}
	cookie := f.session(t, "groupAdmin", "group-admin-password")

	code, body := f.do(t, "GET", "/api/v1/groups/g1", cookie, "")
	if code != http.StatusOK {
		t.Fatalf("group detail = %d body=%s", code, body)
	}
	if !strings.Contains(body, `"Topic":"orders"`) && !strings.Contains(body, `"topic":"orders"`) {
		t.Fatalf("group detail should include the visible topic: %s", body)
	}
	if strings.Contains(body, "secrets") {
		t.Fatalf("group detail leaked a topic the principal may not see: %s", body)
	}

	code, body = f.do(t, "GET", "/api/v1/groups", cookie, "")
	if code != http.StatusOK {
		t.Fatalf("group list = %d body=%s", code, body)
	}
	if strings.Contains(body, "secrets") {
		t.Fatalf("group list leaked a topic the principal may not see: %s", body)
	}

	code, body = f.do(t, "GET", "/api/v1/groups/g1/offsets/export", cookie, "")
	if code != http.StatusOK {
		t.Fatalf("export = %d body=%s", code, body)
	}
	if strings.Contains(body, "secrets") {
		t.Fatalf("export leaked a topic the principal may not see: %s", body)
	}

	code, _ = f.do(t, "POST", "/api/v1/groups/g1/offsets/reset", cookie,
		`{"topic":"secrets","partition":0,"strategy":"to_latest"}`)
	if code != http.StatusNotFound {
		t.Fatalf("reset on an invisible topic = %d, want 404", code)
	}
	code, body = f.do(t, "POST", "/api/v1/groups/g1/offsets/reset", cookie,
		`{"topic":"orders","partition":0,"strategy":"to_latest"}`)
	if code != http.StatusOK {
		t.Fatalf("reset on a visible topic = %d body=%s", code, body)
	}

	code, body = f.do(t, "POST", "/api/v1/groups/g1/offsets/import", cookie,
		`{"offsets":[{"topic":"secrets","partition":0,"offset":7}]}`)
	if code != http.StatusForbidden {
		t.Fatalf("import touching an invisible topic = %d body=%s, want 403", code, body)
	}
	code, body = f.do(t, "POST", "/api/v1/groups/g1/offsets/import", cookie,
		`{"offsets":[{"topic":"orders","partition":0,"offset":7}]}`)
	if code != http.StatusOK {
		t.Fatalf("import within the visible topic = %d body=%s", code, body)
	}

	before := f.server.gm.GroupOffsetsSnapshot("g1")["orders"][0]
	code, body = f.do(t, "POST", "/api/v1/groups/g1/offsets/import", cookie,
		`{"offsets":[{"topic":"orders","partition":0,"offset":12345},{"topic":"secrets","partition":0,"offset":5}]}`)
	if code != http.StatusForbidden {
		t.Fatalf("mixed import = %d body=%s, want 403", code, body)
	}
	if after := f.server.gm.GroupOffsetsSnapshot("g1")["orders"][0]; after != before {
		t.Fatalf("import applied orders=%d before rejecting the invisible row (was %d)", after, before)
	}

	clusterAdmin := f.session(t, "clusterAdmin", "cluster-admin-password")
	code, body = f.do(t, "GET", "/api/v1/groups/g1", clusterAdmin, "")
	if code != http.StatusOK || !strings.Contains(body, "secrets") {
		t.Fatalf("cluster admin group detail = %d body=%s, want both topics", code, body)
	}
}
