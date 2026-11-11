package fleet

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestFakeDockerLifecycle(t *testing.T) {
	ctx := context.Background()
	fd := NewFakeDocker()

	fd.AddContainer(&ContainerInspect{
		ID:    "c-1",
		Name:  "/redis-test",
		Image: "redis:7",
		State: ContainerState{
			Status:  "exited",
			Running: false,
		},
	})

	list, err := fd.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 container, got %d", len(list))
	}
	if list[0].ID != "c-1" {
		t.Fatalf("expected c-1, got %s", list[0].ID)
	}

	if err := fd.Start(ctx, "c-1"); err != nil {
		t.Fatal(err)
	}
	ins, err := fd.Inspect(ctx, "c-1")
	if err != nil {
		t.Fatal(err)
	}
	if !ins.State.Running || ins.State.Status != "running" {
		t.Fatalf("expected running, got %+v", ins.State)
	}

	if err := fd.Restart(ctx, "redis-test", 5); err != nil {
		t.Fatal(err)
	}

	fd.SetLogs("c-1", "ready to accept connections\n")
	logs, err := fd.Logs(ctx, "c-1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs, "ready to accept") {
		t.Fatalf("unexpected logs: %s", logs)
	}

	if err := fd.Stop(ctx, "c-1", 5); err != nil {
		t.Fatal(err)
	}
	ins, err = fd.Inspect(ctx, "c-1")
	if err != nil {
		t.Fatal(err)
	}
	if ins.State.Running {
		t.Fatal("expected stopped container")
	}

	calls := fd.Calls()
	if len(calls) < 4 {
		t.Fatalf("expected at least 4 calls, got %d", len(calls))
	}
}

func TestFakeDockerNotFound(t *testing.T) {
	ctx := context.Background()
	fd := NewFakeDocker()

	methods := []struct {
		name string
		fn   func() error
	}{
		{"Inspect", func() error { _, err := fd.Inspect(ctx, "nonexistent"); return err }},
		{"Start", func() error { return fd.Start(ctx, "nonexistent") }},
		{"Stop", func() error { return fd.Stop(ctx, "nonexistent", 5) }},
		{"Restart", func() error { return fd.Restart(ctx, "nonexistent", 5) }},
		{"Logs", func() error { _, err := fd.Logs(ctx, "nonexistent", 10); return err }},
	}

	for _, m := range methods {
		err := m.fn()
		if err == nil {
			t.Fatalf("%s: expected error for nonexistent container", m.name)
		}
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s: expected ErrNotFound, got %v", m.name, err)
		}
	}
}

func TestDecodeDockerMultiplexedStream(t *testing.T) {

	var payload bytes.Buffer
	h1 := make([]byte, 8)
	h1[0] = 1
	binary.BigEndian.PutUint32(h1[4:8], uint32(len("line1\n")))
	payload.Write(h1)
	payload.WriteString("line1\n")

	h2 := make([]byte, 8)
	h2[0] = 2
	binary.BigEndian.PutUint32(h2[4:8], uint32(len("err2\n")))
	payload.Write(h2)
	payload.WriteString("err2\n")

	res, err := decodeDockerMultiplexedStream(&payload)
	if err != nil {
		t.Fatal(err)
	}
	if res != "line1\nerr2\n" {
		t.Fatalf("unexpected decoded: %q", res)
	}

	plain := "plain text log without docker header\nsecond line"
	res, err = decodeDockerMultiplexedStream(strings.NewReader(plain))
	if err != nil {
		t.Fatal(err)
	}
	if res != plain {
		t.Fatalf("unexpected plain text result: %q", res)
	}

	short := "abc\n"
	res, err = decodeDockerMultiplexedStream(strings.NewReader(short))
	if err != nil {
		t.Fatal(err)
	}
	if res != short {
		t.Fatalf("unexpected short result: %q", res)
	}

	var trunc bytes.Buffer
	ht := make([]byte, 8)
	ht[0] = 1
	binary.BigEndian.PutUint32(ht[4:8], 100)
	trunc.Write(ht)
	trunc.WriteString("only 15 bytes!!")

	res, err = decodeDockerMultiplexedStream(&trunc)
	if err != nil {
		t.Fatal(err)
	}
	if res != "only 15 bytes!!" {
		t.Fatalf("expected only 15 bytes, got %q (len %d)", res, len(res))
	}
}
