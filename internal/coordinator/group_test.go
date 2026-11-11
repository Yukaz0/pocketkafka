package coordinator

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

func testJoinReq(group, memberID string) *protocol.JoinGroupRequest {
	return &protocol.JoinGroupRequest{
		Version:            5,
		Group:              group,
		SessionTimeoutMs:   3000,
		RebalanceTimeoutMs: 3000,
		MemberID:           memberID,
		ProtocolType:       "consumer",
		Protocols: []protocol.JoinGroupRequestProtocol{
			{Name: "range", Metadata: []byte("m")},
		},
	}
}

func TestGroupRebalanceSettlesWithTwoMembers(t *testing.T) {
	s, err := NewOffsetStore(BackendInMemory, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gm := NewGroupManager(s, 1, "h", 9092, 3000)

	var joins atomic.Int32
	stop := make(chan struct{})
	var wg sync.WaitGroup
	run := func() {
		defer wg.Done()
		memberID := ""
		gen := int32(-1)
		for {
			select {
			case <-stop:
				return
			default:
			}
			jr := gm.JoinGroup(testJoinReq("g", memberID))
			if jr.ErrorCode != protocol.ErrNone {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			joins.Add(1)
			memberID = jr.MemberID
			gen = jr.GenerationID

			var assign []protocol.SyncGroupRequestAssignment
			if jr.LeaderID == memberID {
				for _, m := range jr.Members {
					assign = append(assign, protocol.SyncGroupRequestAssignment{
						MemberID:   m.MemberID,
						Assignment: []byte{0, 0, 0, 0},
					})
				}
			}
			gm.SyncGroup(&protocol.SyncGroupRequest{
				Version: 3, Group: "g", GenerationID: gen, MemberID: memberID, Assignments: assign,
			})

			for {
				select {
				case <-stop:
					return
				default:
				}
				time.Sleep(5 * time.Millisecond)
				hr := gm.Heartbeat(&protocol.HeartbeatRequest{Version: 3, Group: "g", GenerationID: gen, MemberID: memberID})
				if hr.ErrorCode == protocol.ErrNone {
					continue
				}
				break
			}
		}
	}
	wg.Add(2)
	go run()
	go run()

	deadline := time.Now().Add(3 * time.Second)
	stable := false
	for time.Now().Before(deadline) {
		if g := gm.get("g"); g != nil {
			g.mu.Lock()
			if g.State == StateStable && len(g.Members) == 2 {
				stable = true
			}
			g.mu.Unlock()
		}
		if stable {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	if !stable {
		t.Fatal("two-member group never reached a stable state")
	}
	if n := joins.Load(); n > 12 {
		t.Fatalf("rebalance churned: %d joins for a two-member group", n)
	}
}

func TestSessionReaperRemovesDeadMember(t *testing.T) {
	s, err := NewOffsetStore(BackendInMemory, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gm := NewGroupManager(s, 1, "h", 9092, 1000)
	jr := gm.JoinGroup(testJoinReq("g", ""))
	if jr.ErrorCode != protocol.ErrNone {
		t.Fatalf("join failed: %d", jr.ErrorCode)
	}
	g := gm.get("g")
	if g == nil {
		t.Fatal("group missing after join")
	}
	g.mu.Lock()
	g.Members[jr.MemberID].LastHeartbeat = time.Now().Add(-time.Minute)
	g.mu.Unlock()

	if n := gm.ReapExpiredSessions(time.Now()); n != 1 {
		t.Fatalf("reaped %d members, want 1", n)
	}
	g.mu.Lock()
	members, state := len(g.Members), g.State
	g.mu.Unlock()
	if members != 0 || state != StateEmpty {
		t.Fatalf("after reap: members=%d state=%v, want 0/Empty", members, state)
	}
}

func TestSyncGroupRejectsStaleGeneration(t *testing.T) {
	s, _ := NewOffsetStore(BackendInMemory, t.TempDir())
	gm := NewGroupManager(s, 1, "h", 9092, 1000)
	jr := gm.JoinGroup(testJoinReq("g", ""))
	if jr.ErrorCode != protocol.ErrNone {
		t.Fatalf("join failed: %d", jr.ErrorCode)
	}
	resp := gm.SyncGroup(&protocol.SyncGroupRequest{
		Version: 3, Group: "g", GenerationID: jr.GenerationID + 1, MemberID: jr.MemberID,
	})
	if resp.ErrorCode != protocol.ErrIllegalGeneration {
		t.Fatalf("stale sync error = %d, want IllegalGeneration", resp.ErrorCode)
	}
}

func TestOffsetCommitFencesZombie(t *testing.T) {
	s, _ := NewOffsetStore(BackendInMemory, t.TempDir())
	gm := NewGroupManager(s, 1, "h", 9092, 1000)
	jr := gm.JoinGroup(testJoinReq("g", ""))
	if jr.ErrorCode != protocol.ErrNone {
		t.Fatalf("join failed: %d", jr.ErrorCode)
	}

	req := &protocol.OffsetCommitRequest{
		Version: 3, Group: "g", Generation: jr.GenerationID + 1, MemberID: jr.MemberID,
		Topics: []protocol.OffsetCommitRequestTopic{{
			Topic:      "t",
			Partitions: []protocol.OffsetCommitRequestPartition{{Partition: 0, Offset: 5}},
		}},
	}
	resp := gm.OffsetCommit(req)
	if resp.Topics[0].Partitions[0].ErrorCode != protocol.ErrIllegalGeneration {
		t.Fatalf("zombie commit error = %d, want IllegalGeneration", resp.Topics[0].Partitions[0].ErrorCode)
	}
	if _, ok := s.Fetch("g", "t", 0); ok {
		t.Fatal("fenced commit was persisted")
	}

	good := &protocol.OffsetCommitRequest{
		Version: 3, Group: "g", Generation: jr.GenerationID, MemberID: jr.MemberID,
		Topics: []protocol.OffsetCommitRequestTopic{{
			Topic: "t",
			Partitions: []protocol.OffsetCommitRequestPartition{
				{Partition: 0, Offset: 5}, {Partition: 1, Offset: 6},
			},
		}},
	}
	resp = gm.OffsetCommit(good)
	for _, p := range resp.Topics[0].Partitions {
		if p.ErrorCode != protocol.ErrNone {
			t.Fatalf("commit partition %d error = %d", p.Partition, p.ErrorCode)
		}
	}
	if off, ok := s.Fetch("g", "t", 1); !ok || off.Offset != 6 {
		t.Fatalf("commit not persisted: %+v ok=%v", off, ok)
	}
}

func TestDeleteGroupRemovesOffsets(t *testing.T) {
	s, _ := NewOffsetStore(BackendInMemory, t.TempDir())
	gm := NewGroupManager(s, 1, "h", 9092, 1000)
	if err := gm.ResetOffsets("g", "t", 0, 11); err != nil {
		t.Fatal(err)
	}

	jr := gm.JoinGroup(testJoinReq("g", ""))
	if jr.ErrorCode != protocol.ErrNone {
		t.Fatalf("join failed: %d", jr.ErrorCode)
	}
	gm.LeaveGroup(&protocol.LeaveGroupRequest{Version: 3, Group: "g", Members: []protocol.LeaveGroupRequestMember{{MemberID: jr.MemberID}}})
	if err := gm.DeleteGroup("g"); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}
	if _, ok := s.Fetch("g", "t", 0); ok {
		t.Fatal("offsets survived group deletion")
	}
	if gm.get("g") != nil {
		t.Fatal("group still registered after deletion")
	}
}
