package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestFollowCopies(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Linux example uses directory fsync")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, sqlDB := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqlDB)
	if _, err := sqlDB.Exec(`CREATE TABLE data(value TEXT); INSERT INTO data VALUES('A')`); err != nil {
		t.Fatal(err)
	}
	c := file.NewReplicaClient(t.TempDir())
	publisher := litestream.NewReplicaWithClient(db, c)
	sync := func() {
		t.Helper()
		if err := db.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		if err := publisher.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	sync()
	if _, err := db.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	sync()
	dir := t.TempDir()
	done := make(chan error, 1)
	go func() { done <- run(ctx, litestream.NewReplicaWithClient(nil, c), dir) }()
	// Join before TempDir cleanup, including failed assertions.
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("follow: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("follow did not stop")
		}
	}()
	first := waitReady(t, ctx, dir, "")
	old := openCopy(t, filepath.Join(dir, first.File))
	defer old.Close()
	assertValue(t, old, "A")
	if _, err := sqlDB.Exec(`UPDATE data SET value='B'`); err != nil {
		t.Fatal(err)
	}
	sync()
	second := waitReady(t, ctx, dir, first.File)
	current := openCopy(t, filepath.Join(dir, second.File))
	defer current.Close()
	assertValue(t, current, "B")
	assertValue(t, old, "A")
	if second.TXID <= first.TXID {
		t.Fatal("position did not advance")
	}
	// Copy errors and older positions must not replace the completed descriptor.
	if err := publishCopy(ctx, dir, filepath.Join(dir, "missing.db"), second.TXID+1); err == nil {
		t.Fatal("expected copy failure")
	}
	if err := publishCopy(ctx, dir, filepath.Join(dir, "missing.db"), first.TXID); err != nil {
		t.Fatal(err)
	}
	got := waitReady(t, ctx, dir, "")
	if got != second {
		t.Fatal("ready descriptor changed")
	}
}

func TestCopyRejectsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dir := t.TempDir()
	if err := publishCopy(ctx, dir, "missing", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "ready.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ready unexpectedly exists: %v", err)
	}
}

func waitReady(t *testing.T, ctx context.Context, dir, previous string) ready {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var next ready
		b, err := os.ReadFile(filepath.Join(dir, "ready.json"))
		if err == nil {
			if err = json.Unmarshal(b, &next); err != nil {
				t.Fatal(err)
			}
			if next.File != previous {
				return next
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for ready copy")
		case <-ticker.C:
		}
	}
}
func openCopy(t *testing.T, path string) *sql.DB {
	t.Helper()
	u := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro&immutable=1"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	return db
}
func assertValue(t *testing.T, db *sql.DB, want string) {
	t.Helper()
	var got string
	if err := db.QueryRow("SELECT value FROM data").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("value=%q, want %q", got, want)
	}
}
