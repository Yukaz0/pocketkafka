package fleet

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/credential"
)

const testMasterKey = "master-key-32-chars-long-secret!"

func newTestRegistry(t *testing.T, dir string) *Registry {
	t.Helper()
	r, err := NewRegistry(dir, testMasterKey)
	if err != nil {
		t.Fatalf("newTestRegistry: %v", err)
	}
	return r
}

func TestEnrollTokenSingleUse(t *testing.T) {
	r := newTestRegistry(t, t.TempDir())
	tok, err := r.Enroll("srv-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ConsumeToken(tok); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ConsumeToken(tok); err == nil {
		t.Fatal("reuse must fail")
	}
}

func TestRevokeClosesAccess(t *testing.T) {
	r := newTestRegistry(t, t.TempDir())
	tok, err := r.Enroll("srv-1")
	if err != nil {
		t.Fatal(err)
	}
	agent, err := r.ConsumeToken(tok)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := r.AuthorizeStream(agent.ID); err != nil {
		t.Fatalf("expected authorized stream before revoke: %v", err)
	}

	if err := r.Revoke(agent.ID); err != nil {
		t.Fatalf("revoke failed: %v", err)
	}

	if _, err := r.AuthorizeStream(agent.ID); err == nil {
		t.Fatal("stream terbuka agent tercabut tak boleh terima job")
	}

	lookedUp, err := r.Lookup(agent.ID)
	if err != nil {
		t.Fatalf("lookup failed: %v", err)
	}
	if !lookedUp.Revoked {
		t.Fatal("expected looked up agent to be revoked")
	}
}

func TestSecretStoredAsHash(t *testing.T) {
	dir := t.TempDir()
	r := newTestRegistry(t, dir)
	tok, err := r.Enroll("srv-1")
	if err != nil {
		t.Fatal(err)
	}
	agent, err := r.ConsumeToken(tok)
	if err != nil {
		t.Fatal(err)
	}

	if agent.RawSecret == "" {
		t.Fatal("expected non-empty plaintext RawSecret on newly enrolled agent")
	}
	if !credential.LooksLikeHash(agent.Secret) {
		t.Fatalf("expected agent.Secret to be hashed, got %q", agent.Secret)
	}
	if !credential.VerifyPassword(agent.Secret, agent.RawSecret) {
		t.Fatal("expected PBKDF2 hash to match raw secret")
	}

	filePath := filepath.Join(dir, "__fleet_agents.json")
	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("read registry file: %v", err)
	}
	fileStr := string(data)

	if strings.Contains(fileStr, agent.RawSecret) {
		t.Fatal("plaintext raw secret must NOT appear in persisted __fleet_agents.json")
	}
	if !strings.Contains(fileStr, agent.Secret) {
		t.Fatal("hashed secret must appear in persisted __fleet_agents.json")
	}

	r2 := newTestRegistry(t, dir)
	loaded, err := r2.Lookup(agent.ID)
	if err != nil {
		t.Fatalf("lookup from reloaded registry: %v", err)
	}
	if !credential.LooksLikeHash(loaded.Secret) {
		t.Fatalf("expected loaded secret to be hashed: %q", loaded.Secret)
	}
	if !credential.VerifyPassword(loaded.Secret, agent.RawSecret) {
		t.Fatal("expected reloaded hash to match raw secret")
	}
}

func TestEnrollExpiredToken(t *testing.T) {
	r := newTestRegistry(t, t.TempDir())
	tok, err := r.enrollWithExpiry("srv-1", -time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ConsumeToken(tok); err == nil {
		t.Fatal("consuming expired token must fail")
	}
}

func TestLookupNoRawSecretLeak(t *testing.T) {
	r := newTestRegistry(t, t.TempDir())
	tok, err := r.Enroll("srv-1")
	if err != nil {
		t.Fatal(err)
	}
	agent, err := r.ConsumeToken(tok)
	if err != nil {
		t.Fatal(err)
	}
	if agent.RawSecret == "" {
		t.Fatal("expected ConsumeToken to return RawSecret")
	}

	lookedUp, err := r.Lookup(agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if lookedUp.RawSecret != "" {
		t.Fatalf("expected Lookup to return empty RawSecret, got %q", lookedUp.RawSecret)
	}

	streamAg, err := r.AuthorizeStream(agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if streamAg.RawSecret != "" {
		t.Fatalf("expected AuthorizeStream to return empty RawSecret, got %q", streamAg.RawSecret)
	}

	agents := r.ListAgents()
	if len(agents) != 1 {
		t.Fatalf("expected 1 agent, got %d", len(agents))
	}
	if agents[0].RawSecret != "" {
		t.Fatalf("expected ListAgents to return empty RawSecret, got %q", agents[0].RawSecret)
	}
}

func TestReturnMutationDoesNotAffectRegistry(t *testing.T) {
	r := newTestRegistry(t, t.TempDir())
	tok, err := r.Enroll("srv-1")
	if err != nil {
		t.Fatal(err)
	}
	agent, err := r.ConsumeToken(tok)
	if err != nil {
		t.Fatal(err)
	}

	origSecret := agent.Secret

	agent.Revoked = true
	agent.ServerID = "mutated-server"
	agent.Secret = "corrupted-secret"

	cur, err := r.Lookup(agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cur.Revoked {
		t.Fatal("mutating ConsumeToken return must not mutate registry state (Revoked)")
	}
	if cur.ServerID != "srv-1" {
		t.Fatalf("mutating ConsumeToken return must not mutate registry state (ServerID: %s)", cur.ServerID)
	}
	if cur.Secret != origSecret {
		t.Fatal("mutating ConsumeToken return must not mutate registry state (Secret)")
	}

	cur.Revoked = true
	cur2, err := r.Lookup(agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cur2.Revoked {
		t.Fatal("mutating Lookup return must not mutate registry state")
	}

	streamAg, err := r.AuthorizeStream(agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	streamAg.Revoked = true
	if _, err := r.AuthorizeStream(agent.ID); err != nil {
		t.Fatalf("mutating AuthorizeStream return must not revoke agent: %v", err)
	}

	list := r.ListAgents()
	if len(list) == 0 {
		t.Fatal("expected non-empty agent list")
	}
	list[0].Revoked = true
	cur3, err := r.Lookup(agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cur3.Revoked {
		t.Fatal("mutating ListAgents return must not mutate registry state")
	}
}

func TestMasterKeyFailClosed(t *testing.T) {
	dir := t.TempDir()
	if _, err := NewRegistry(dir, ""); err == nil {
		t.Fatal("empty master key must fail")
	}
	if _, err := NewRegistry(dir, "short"); err == nil {
		t.Fatal("short master key must fail")
	}
}

func TestRegistryRestartHMACKeyPersistsAndVerifies(t *testing.T) {
	dir := t.TempDir()
	masterKey := "master-key-32-chars-long-secret!"

	r1, err := NewRegistry(dir, masterKey)
	if err != nil {
		t.Fatalf("r1: %v", err)
	}
	tok, err := r1.Enroll("srv-1")
	if err != nil {
		t.Fatal(err)
	}
	agent, err := r1.ConsumeToken(tok)
	if err != nil {
		t.Fatal(err)
	}
	if agent.RawSecret == "" {
		t.Fatal("expected non-empty raw secret upon ConsumeToken")
	}

	r2, err := NewRegistry(dir, masterKey)
	if err != nil {
		t.Fatalf("r2: %v", err)
	}

	loaded, err := r2.Lookup(agent.ID)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if loaded.RawSecret != "" {
		t.Fatal("raw secret must not be exposed in loaded agent struct")
	}
	if loaded.SealedKey == "" {
		t.Fatal("expected sealed key in loaded agent")
	}

	decKey, err := r2.AgentKey(agent.ID)
	if err != nil {
		t.Fatalf("AgentKey: %v", err)
	}
	if decKey != agent.RawSecret {
		t.Fatalf("decrypted key %q != raw secret %q", decKey, agent.RawSecret)
	}

	controlSide, agentSide := NewChannelTransportPair(16)
	defer controlSide.Close()
	defer agentSide.Close()

	mgr := NewAgentManager(r2, nil)
	sess, err := mgr.RegisterSession(agent.ID, "", controlSide)
	if err != nil {
		t.Fatalf("RegisterSession after restart: %v", err)
	}

	env, err := Sign(agent.RawSecret, DirectionAgentToControl, "ping", []byte("hello"), time.Minute)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	env.AgentID = agent.ID

	if err := sess.verifier.Verify(env); err != nil {
		t.Fatalf("envelope verification failed after restart: %v", err)
	}

	rWrong, err := NewRegistry(dir, "wrong-master-key-with-32-chars!")
	if err != nil {
		t.Fatalf("rWrong: %v", err)
	}
	if _, err := rWrong.AgentKey(agent.ID); err == nil {
		t.Fatal("opening sealed key with wrong master key must fail")
	}

	mgrWrong := NewAgentManager(rWrong, nil)
	if _, err := mgrWrong.RegisterSession(agent.ID, "", controlSide); err == nil {
		t.Fatal("RegisterSession with wrong master key must fail")
	}
}

func TestDeleteServer(t *testing.T) {
	r := newTestRegistry(t, t.TempDir())
	tok, err := r.Enroll("srv-delete-me")
	if err != nil {
		t.Fatal(err)
	}
	agent, err := r.ConsumeToken(tok)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r.GetServer("srv-delete-me"); !ok {
		t.Fatal("expected server to exist")
	}
	if err := r.DeleteServer("srv-delete-me"); err != nil {
		t.Fatalf("delete server failed: %v", err)
	}
	if _, ok := r.GetServer("srv-delete-me"); ok {
		t.Fatal("expected server to be deleted")
	}
	if _, err := r.Lookup(agent.ID); err == nil {
		t.Fatal("expected agent to be deleted")
	}
}

func TestRevokeAndReissueEnrollment(t *testing.T) {
	r := newTestRegistry(t, t.TempDir())
	tok, err := r.Enroll("srv-tok")
	if err != nil {
		t.Fatal(err)
	}
	prefix := tok[:8]
	if err := r.RevokeEnrollment(prefix); err != nil {
		t.Fatalf("revoke enrollment: %v", err)
	}
	if _, err := r.ConsumeToken(tok); err == nil {
		t.Fatal("consumed revoked token must fail")
	}

	tok2, err := r.Enroll("srv-reissue")
	if err != nil {
		t.Fatal(err)
	}
	p2 := tok2[:8]
	newTok, srvID, err := r.ReissueEnrollment(p2, 30*time.Minute)
	if err != nil {
		t.Fatalf("reissue: %v", err)
	}
	if srvID != "srv-reissue" {
		t.Fatalf("expected srv-reissue, got %q", srvID)
	}
	if _, err := r.ConsumeToken(tok2); err == nil {
		t.Fatal("consumed old token after reissue must fail")
	}
	if _, err := r.ConsumeToken(newTok); err != nil {
		t.Fatalf("consume new token must succeed: %v", err)
	}
}
