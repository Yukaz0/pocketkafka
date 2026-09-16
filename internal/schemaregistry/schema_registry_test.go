package schemaregistry

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }

func decodeJSON(t *testing.T, data []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("decode response %q: %v", string(data), err)
	}
}

func registerSchema(t *testing.T, r *Registry, subject, schemaType, schema string) (int, map[string]any) {
	t.Helper()
	body := `{"schema":` + quote(schema) + `,"schemaType":"` + schemaType + `"}`
	req := httptest.NewRequest(http.MethodPost, "/subjects/"+subject+"/versions", strings.NewReader(body))
	req.SetPathValue("subject", subject)
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, req)

	var out map[string]any
	decodeJSON(t, rec.Body.Bytes(), &out)
	return rec.Code, out
}

func quote(s string) string {
	b, _ := jsonMarshal(s)
	return string(b)
}

// TestPersistAcrossRestart registers a schema, reopens the registry from disk,
// and confirms the ID/version are preserved.
func TestPersistAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "__schemas.json")
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	code, out := registerSchema(t, r, "orders-value", "AVRO", `{"type":"record","name":"O","fields":[]}`)
	if code != 200 {
		t.Fatalf("register status = %d (%v)", code, out)
	}
	id := int(out["id"].(float64))
	if id != 1 {
		t.Fatalf("id = %d, want 1", id)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	versions := reopened.SubjectVersions("orders-value")
	if len(versions) != 1 || versions[0].ID != id {
		t.Fatalf("versions after restart = %+v, want one entry with id %d", versions, id)
	}
}

// TestDuplicateReturnsSameID checks dedup survives a restart too.
func TestDuplicateReturnsSameID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "__schemas.json")
	r, _ := Open(path)
	schema := `{"type":"string"}`
	_, first := registerSchema(t, r, "s", "AVRO", schema)
	_, second := registerSchema(t, r, "s", "AVRO", schema)
	if first["id"] != second["id"] {
		t.Fatalf("duplicate schema returned different ids: %v vs %v", first["id"], second["id"])
	}
	reopened, _ := Open(path)
	if got := len(reopened.SubjectVersions("s")); got != 1 {
		t.Fatalf("versions after restart = %d, want 1", got)
	}
}

// TestVersionsStayMonotonic registers two distinct schemas and checks the
// version numbers increment.
func TestVersionsStayMonotonic(t *testing.T) {
	r, _ := Open(filepath.Join(t.TempDir(), "__schemas.json"))
	registerSchema(t, r, "s", "AVRO", `{"type":"string"}`)
	registerSchema(t, r, "s", "AVRO", `{"type":"int"}`)
	versions := r.SubjectVersions("s")
	if len(versions) != 2 || versions[0].Version != 1 || versions[1].Version != 2 {
		t.Fatalf("versions = %+v, want [1 2]", versions)
	}
}

func TestCorruptStorageFailsLoudly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "__schemas.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("Open accepted a corrupt registry file")
	}
}

func TestValidateSchemaTypes(t *testing.T) {
	r, _ := Open(filepath.Join(t.TempDir(), "__schemas.json"))
	cases := []struct {
		name       string
		schemaType string
		schema     string
		wantStatus int
	}{
		{"valid avro", "AVRO", `{"type":"string"}`, 200},
		{"valid json", "JSON", `{"a":1}`, 200},
		{"invalid json avro", "AVRO", `not json`, 422},
		{"invalid json type", "JSON", `{`, 422},
		{"unsupported protobuf", "PROTOBUF", `syntax = "proto3";`, 422},
		{"unknown type", "THRIFT", `{}`, 422},
		{"empty schema", "AVRO", ``, 422},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			subject := "s-" + strings.ReplaceAll(tc.name, " ", "-")
			code, out := registerSchema(t, r, subject, tc.schemaType, tc.schema)
			if code != tc.wantStatus {
				t.Fatalf("status = %d (%v), want %d", code, out, tc.wantStatus)
			}
		})
	}
}
