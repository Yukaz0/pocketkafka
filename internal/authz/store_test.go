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

	if err := s.Upsert(Rule{Principal: "alice", ResourceType: ResourceTopic, ResourceName: "*", Operations: []string{"Read"}}); err != nil {
		t.Fatal(err)
	}

	if err := s.Upsert(Rule{Principal: "alice", ResourceType: ResourceTopic, ResourceName: "secret", Operations: []string{"Write"}}); err != nil {
		t.Fatal(err)
	}

	if err := s.Authorize("alice", OpRead, Resource{Type: ResourceTopic, Name: "orders"}); err != nil {
		t.Fatalf("wildcard Read should be allowed: %v", err)
	}

	if err := s.Authorize("alice", OpRead, Resource{Type: ResourceTopic, Name: "secret"}); err == nil {
		t.Fatal("exact rule should have shadowed the wildcard Read grant")
	}

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

func TestDescribeIsImpliedByOtherOperations(t *testing.T) {
	cases := []struct {
		grant string
		op    Operation
		want  bool
	}{
		{"Read", OpDescribe, true},
		{"Write", OpDescribe, true},
		{"Admin", OpDescribe, true},
		{"Admin", OpRead, true},
		{"Admin", OpWrite, true},
		{"Read", OpWrite, false},
		{"Write", OpRead, false},
		{"Describe", OpRead, false},
		{"Describe", OpDescribe, true},
		{"*", OpAdmin, true},
	}
	for _, tc := range cases {
		s := NewInMemory()
		if err := s.Upsert(Rule{Principal: "app", ResourceType: ResourceTopic, ResourceName: "orders", Operations: []string{tc.grant}}); err != nil {
			t.Fatalf("grant %q: %v", tc.grant, err)
		}
		err := s.Authorize("app", tc.op, Resource{Type: ResourceTopic, Name: "orders"})
		if got := err == nil; got != tc.want {
			t.Fatalf("grant %q vs %s = %v, want %v", tc.grant, tc.op, got, tc.want)
		}
	}
}

func TestEmptyPrincipalIsNeverAuthorized(t *testing.T) {
	s := NewInMemory()
	if err := s.Upsert(Rule{Principal: "*", ResourceType: ResourceCluster, ResourceName: "*", Operations: []string{"Write"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Authorize("", OpWrite, Resource{Type: ResourceCluster, Name: "cluster"}); err == nil {
		t.Fatal("an empty principal was authorized by a wildcard rule")
	}
	if l := s.CanDescribeAny("", ResourceTopic); l {
		t.Fatal("an empty principal may list resources")
	}
}

func TestWildcardAdminNeedsOptIn(t *testing.T) {
	s := NewInMemory()
	wildcardAdmin := Rule{Principal: "*", ResourceType: ResourceCluster, ResourceName: "*", Operations: []string{"Admin"}}
	if err := s.Upsert(wildcardAdmin); err == nil {
		t.Fatal("a wildcard Admin rule was accepted without the opt-in")
	}
	if err := s.Upsert(Rule{Principal: "*", ResourceType: ResourceCluster, ResourceName: "*", Operations: []string{"Read"}}); err != nil {
		t.Fatalf("a wildcard Read rule must stay allowed: %v", err)
	}
	s.SetAllowWildcardAdmin(true)
	if err := s.Upsert(wildcardAdmin); err != nil {
		t.Fatalf("opt-in did not accept the rule: %v", err)
	}
	if got := len(s.WildcardAdminRules()); got != 1 {
		t.Fatalf("WildcardAdminRules = %d, want 1", got)
	}
}

func TestRuleValidationRejectsTypos(t *testing.T) {
	s := NewInMemory()
	cases := []Rule{
		{Principal: "app", ResourceType: "topics", ResourceName: "t", Operations: []string{"Read"}},
		{Principal: "app", ResourceType: ResourceTopic, ResourceName: "t", Operations: []string{"reed"}},
		{Principal: "", ResourceType: ResourceTopic, ResourceName: "t", Operations: []string{"Read"}},
	}
	for _, rule := range cases {
		if err := s.Upsert(rule); err == nil {
			t.Fatalf("rule %+v was accepted", rule)
		}
	}
}

func TestSuperUserAuthorizerGrantsEverything(t *testing.T) {
	inner := NewInMemory()
	a := NewSuperUserAuthorizer(inner, []string{"admin"})
	if err := a.Authorize("admin", OpAdmin, Resource{Type: ResourceCluster, Name: "cluster"}); err != nil {
		t.Fatalf("super user denied: %v", err)
	}
	if err := a.Authorize("app", OpRead, Resource{Type: ResourceTopic, Name: "t"}); err == nil {
		t.Fatal("a non-super user was allowed by an empty store")
	}
	if err := a.Authorize("", OpRead, Resource{Type: ResourceTopic, Name: "t"}); err == nil {
		t.Fatal("an empty principal was treated as a super user")
	}
}
