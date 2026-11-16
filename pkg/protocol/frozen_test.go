package protocol

import (
	"os"
	"regexp"
	"strconv"
	"testing"
)

// frozenV1 is the Kafka wire surface promised stable from v1.0. Changing it is
// a deliberate act: edit this table, test/interop/compatibility.yaml and
// docs/STABILITY.md together. Raising a max version is additive; lowering one
// or removing a key is breaking and needs a major version.
var frozenV1 = map[string]struct {
	key int16
	max int16
}{
	"Produce":          {APKProduce, 3},
	"Fetch":            {APKFetch, 5},
	"ListOffsets":      {APKListOffsets, 5},
	"Metadata":         {APKMetadata, 8},
	"OffsetCommit":     {APKOffsetCommit, 7},
	"OffsetFetch":      {APKOffsetFetch, 5},
	"FindCoordinator":  {APKFindCoordinator, 2},
	"JoinGroup":        {APKJoinGroup, 5},
	"Heartbeat":        {APKHeartbeat, 3},
	"LeaveGroup":       {APKLeaveGroup, 3},
	"SyncGroup":        {APKSyncGroup, 3},
	"DescribeGroups":   {APKDescribeGroups, 3},
	"ListGroups":       {APKListGroups, 2},
	"SaslHandshake":    {APKSaslHandshake, 1},
	"ApiVersions":      {APKApiVersions, 2},
	"CreateTopics":     {APKCreateTopics, 4},
	"DeleteTopics":     {APKDeleteTopics, 3},
	"InitProducerID":   {APKInitProducerID, 1},
	"SaslAuthenticate": {APKSaslAuthenticate, 1},
	"DeleteGroups":     {APKDeleteGroups, 1},
}

func TestWireSurfaceFrozen(t *testing.T) {
	if len(maxVersions) != len(frozenV1) {
		t.Errorf("advertised %d api keys, frozen surface has %d", len(maxVersions), len(frozenV1))
	}
	for name, f := range frozenV1 {
		if got := MaxVersion(f.key); got != f.max {
			t.Errorf("%s: max version %d, frozen at %d", name, got, f.max)
		}
	}
	// Transactions stay fail-closed: never advertised (see docs/STABILITY.md).
	for _, k := range []int16{APKAddPartitionsToTxn, APKAddOffsetsToTxn, APKEndTxn} {
		if SupportsKey(k) {
			t.Errorf("transactional api key %d is advertised; the v1.0 contract is fail-closed", k)
		}
	}
}

// The manifest is the public claim; it must equal what the broker advertises.
func TestCompatibilityManifestMatchesWire(t *testing.T) {
	raw, err := os.ReadFile("../../test/interop/compatibility.yaml")
	if err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(`(?m)^  ([A-Za-z]+): (\d+)\b`)
	seen := map[string]bool{}
	for _, m := range re.FindAllStringSubmatch(string(raw), -1) {
		f, ok := frozenV1[m[1]]
		if !ok {
			t.Errorf("manifest lists %s, which the broker does not advertise", m[1])
			continue
		}
		seen[m[1]] = true
		if v, _ := strconv.Atoi(m[2]); int16(v) != f.max {
			t.Errorf("manifest says %s: %s, broker advertises %d", m[1], m[2], f.max)
		}
	}
	for name := range frozenV1 {
		if !seen[name] {
			t.Errorf("manifest is missing %s", name)
		}
	}
}
