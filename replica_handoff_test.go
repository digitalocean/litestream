package litestream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

func TestReplica_OnApplied_Options(t *testing.T) {
	callback := func(context.Context, string, ltx.TXID) error { t.Fatal("unexpected callback"); return nil }
	r := NewReplicaWithClient(nil, &followTestReplicaClient{})
	path := filepath.Join(t.TempDir(), "working.db")
	if err := r.Restore(context.Background(), RestoreOptions{OutputPath: path, OnApplied: callback}); err == nil || !strings.Contains(err.Error(), "requires follow") {
		t.Fatalf("error=%v", err)
	}
	if err := os.WriteFile(path, []byte("uncertain previous session"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := r.Restore(context.Background(), RestoreOptions{OutputPath: path, Follow: true, OnApplied: callback}); err == nil || !strings.Contains(err.Error(), "fresh working database") {
		t.Fatalf("error=%v", err)
	}
}

// The first delta completes, but the second writes a page before its checksum
// fails. The working file is no longer a snapshot of either completed position.
func TestReplica_OnApplied_PartialPass(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "checksum"
		if canceled {
			name = "cancellation"
		}
		t.Run(name, func(t *testing.T) {
			path := handoffWorkingFile(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client := &followTestReplicaClient{}
			client.LTXFilesFunc = func(context.Context, int, ltx.TXID, bool) (ltx.FileIterator, error) {
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{{Level: 0, MinTXID: 11, MaxTXID: 11}, {Level: 0, MinTXID: 12, MaxTXID: 12}}), nil
			}
			client.OpenLTXFileFunc = func(_ context.Context, _ int, min, _ ltx.TXID, _, _ int64) (io.ReadCloser, error) {
				data := mustBuildIncrementalLTX(t, min, min, 4096, 2, byte(min))
				if min == 12 {
					if canceled {
						cancel()
						return nil, ctx.Err()
					}
					data[len(data)-1] ^= 0xff
				}
				return io.NopCloser(bytes.NewReader(data)), nil
			}
			var calls []ltx.TXID
			err := NewReplicaWithClient(nil, client).follow(ctx, path, 10, time.Millisecond, func(_ context.Context, _ string, pos ltx.TXID) error { calls = append(calls, pos); return nil })
			if err == nil {
				t.Fatal("expected apply failure")
			}
			if canceled && !errors.Is(err, context.Canceled) {
				t.Fatalf("error=%v", err)
			}
			if len(calls) != 1 || calls[0] != 10 {
				t.Fatalf("published incomplete pass: %v", calls)
			}
			if got, err := ReadTXIDFile(path); err != nil || got != 10 {
				t.Fatalf("saved position=%s, error=%v", got, err)
			}
		})
	}
}

func TestReplica_OnApplied_WaitsForCallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	path := handoffWorkingFile(t)
	entered, release := make(chan struct{}), make(chan struct{})
	polled, done := make(chan struct{}, 1), make(chan error, 1)
	stopErr := errors.New("stop after update")
	client := &followTestReplicaClient{}
	client.LTXFilesFunc = func(context.Context, int, ltx.TXID, bool) (ltx.FileIterator, error) {
		polled <- struct{}{}
		return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{{Level: 0, MinTXID: 11, MaxTXID: 11}}), nil
	}
	data := mustBuildIncrementalLTX(t, 11, 11, 4096, 2, 0xab)
	client.OpenLTXFileFunc = func(context.Context, int, ltx.TXID, ltx.TXID, int64, int64) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	go func() {
		done <- NewReplicaWithClient(nil, client).follow(ctx, path, 10, time.Millisecond, func(_ context.Context, _ string, pos ltx.TXID) error {
			if pos == 10 {
				close(entered)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return stopErr
		})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("callback did not start")
	}
	select {
	case <-polled:
		t.Fatal("follow advanced while callback was blocked")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, stopErr) {
			t.Fatalf("error=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("follow did not stop")
	}
	if got, err := ReadTXIDFile(path); err != nil || got != 11 {
		t.Fatalf("saved position=%s, error=%v", got, err)
	}
}

func TestReplica_OnApplied_InitialError(t *testing.T) {
	want := errors.New("copy failed")
	r := NewReplicaWithClient(nil, &followTestReplicaClient{})
	err := r.follow(context.Background(), handoffWorkingFile(t), 10, time.Millisecond, func(context.Context, string, ltx.TXID) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("error=%v", err)
	}
}

func handoffWorkingFile(t *testing.T) string {
	t.Helper()
	f := mustCreateWritableDBFile(t)
	if _, err := f.WriteAt(newSQLiteHeaderPage(4096), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := WriteTXIDFile(f.Name(), 10); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}
