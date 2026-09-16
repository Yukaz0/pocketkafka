package authz

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestEmptyStoreDeniesEverything(t *testing.T) {
	s := NewInMemory()
	err := s.Authorize("alice", OpRead, Resource{Type: ResourceTopic, Name: "orders"})
	if err == nil {
		t.Fatal("empty store allowed a request; want default-deny")
	}
}

func TestAllowAllAuthorizer(t *testing.T) {
	var a AllowAllAuthorizer
	if err := a.Authorize("anyone", OpAdmin, Resource{Type: ResourceTopic, Name: "t"}); err != nil {
		t.Fatalf("AllowAllAuthorizer denied: %v", err)
	}
}

func TestExactAndWildcardPrecedence(t *testing.T) {
	s := NewInMemory()
	// Wildcard grants Read on any topic...
	if err := s.Upsert(Rule{Principal: "alice", ResourceType: ResourceTopic, ResourceName: "*", Operations: []string{"Read"}}); err != nil {
		t.Fatal(err)
	}
	// ...but the exact rule for "secret" grants only Write.
	if err := s.Upsert(Rule{Principal: "alice", ResourceType: ResourceTopic, ResourceName: "secret", Operations: []string{"Write"}}); err != nil {
		t.Fatal(err)
	}

	// Wildcard applies where no exact rule matches.
	if err := s.Authorize("alice", OpRead, Resource{Type: ResourceTopic, Name: "orders"}); err != nil {
		t.Fatalf("wildcard Read should be allowed: %v", err)
	}
	// Exact rule wins: Read on "secret" is denied even though wildcard grants it.
	if err := s.Authorize("alice", OpRead, Resource{Type: ResourceTopic, Name: "secret"}); err == nil {
		t.Fatal("exact rule should have shadowed the wildcard Read grant")
	}
	// Exact rule grants Write.
	if err := s.Authorize("alice", OpWrite, Resource{Type: ResourceTopic, Name: "secret"}); err != nil {
		t.Fatalf("exact Write should be allowed: %v", err)
	}
}

func TestPrincipalIsolation(t *testing.T) {
	s := NewInMemory()
	_ = s.Upsert(Rule{Principal: "alice", ResourceType: ResourceTopic, ResourceName: "*", Operations: []string{"Read"}})
	if err := s.Authorize("bob", OpRead, Resource{Type: ResourceTopic, Name: "orders"}); err == nil {
		t.Fatal("bob should not inherit alice's rule")
	}
}

func TestPersistenceRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "__acls.json")
	s, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Upsert(Rule{Principal: "alice", ResourceType: "topic", ResourceName: "orders", Operations: []string{"Read", "Write"}}); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := reopened.Authorize("alice", OpWrite, Resource{Type: ResourceTopic, Name: "orders"}); err != nil {
		t.Fatalf("persisted rule not applied after reopen: %v", err)
	}
	if err := reopened.Authorize("alice", OpAdmin, Resource{Type: ResourceTopic, Name: "orders"}); err == nil {
		t.Fatal("unlisted operation was allowed")
	}
}

func TestDeleteRule(t *testing.T) {
	path := filepath.Join(t.TempDir(), "__acls.json")
	s, _ := NewStore(path)
	_ = s.Upsert(Rule{Principal: "alice", ResourceType: "topic", ResourceName: "orders", Operations: []string{"Read"}})
	if err := s.Delete("alice", "topic", "orders"); err != nil {
		t.Fatal(err)
	}
	if err := s.Authorize("alice", OpRead, Resource{Type: ResourceTopic, Name: "orders"}); err == nil {
		t.Fatal("deleted rule still grants access")
	}
	reopened, _ := NewStore(path)
	if err := reopened.Authorize("alice", OpRead, Resource{Type: ResourceTopic, Name: "orders"}); err == nil {
		t.Fatal("deleted rule came back after reload")
	}
}

// TestCorruptFileFailsLoudly guards the "never silently become allow-all"
// requirement.
func TestCorruptFileFailsLoudly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "__acls.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStore(path); err == nil {
		t.Fatal("corrupt ACL file did not produce an error")
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := NewInMemory()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = s.Upsert(Rule{Principal: "p", ResourceType: "topic", ResourceName: "t", Operations: []string{"Read"}})
				_ = s.Rules()
				_ = s.Authorize("p", OpRead, Resource{Type: ResourceTopic, Name: "t"})
				_ = s.Authorize("q", OpWrite, Resource{Type: ResourceGroup, Name: "g"})
			}
		}(i)
	}
	wg.Wait()
}
