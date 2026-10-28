// snapshot-follow demonstrates copying a continuously followed database into
// immutable files. It is a Linux example, not a production sidecar.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
	"github.com/benbjohnson/litestream/s3"
)

// ready names a completed file; readers open it with mode=ro&immutable=1.
// TXID is only comparable within the single source database used by this example.
type ready struct {
	File string   `json:"file"`
	TXID ltx.TXID `json:"txid"`
}

func main() {
	bucket := flag.String("bucket", "", "S3 bucket")
	prefix := flag.String("prefix", "", "S3 object prefix")
	endpoint := flag.String("endpoint", "", "S3 endpoint; empty uses AWS")
	region := flag.String("region", "us-east-1", "S3 region")
	dir := flag.String("dir", "", "persistent output directory, owned by one process")
	flag.Parse()
	if *bucket == "" || *dir == "" {
		flag.Usage()
		os.Exit(2)
	}
	c := s3.NewReplicaClient()
	c.Bucket, c.Path, c.Endpoint, c.Region = *bucket, *prefix, *endpoint, *region
	c.ForcePathStyle = true
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, litestream.NewReplicaWithClient(nil, c), *dir); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("follow stopped; existing ready copy retained", "err", err)
		os.Exit(1)
	}
}

// run always creates a new working file. An interrupted one may contain a
// partial update, even when its saved transaction ID looks valid.
func run(ctx context.Context, r *litestream.Replica, dir string) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	session, err := os.MkdirTemp(dir, "working-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(session)
	return r.Restore(ctx, litestream.RestoreOptions{
		OutputPath: filepath.Join(session, "working.db"), Follow: true, FollowInterval: time.Second,
		OnApplied: func(ctx context.Context, path string, pos ltx.TXID) error { return publishCopy(ctx, dir, path, pos) },
	})
}

// publishCopy must run inside OnApplied. Returning from the callback before
// copying finishes would let the follower change the source during the copy.
func publishCopy(ctx context.Context, dir, path string, pos ltx.TXID) error {
	// Keep a newer serving copy if a restart temporarily reads a stale origin.
	var previous ready
	b, err := os.ReadFile(filepath.Join(dir, "ready.json"))
	if err == nil {
		if err = json.Unmarshal(b, &previous); err != nil {
			return err
		}
		if previous.File == "" || filepath.Base(previous.File) != previous.File || previous.TXID == 0 {
			return fmt.Errorf("invalid ready descriptor")
		}
		if _, err = os.Stat(filepath.Join(dir, previous.File)); err != nil {
			return err
		}
		if pos <= previous.TXID {
			return nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	in, err := os.Open(path)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.CreateTemp(dir, "snapshot-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(out.Name())
	defer out.Close()
	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	if err = out.Sync(); err != nil {
		return err
	}
	if err = out.Close(); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	// A unique filename prevents overwriting an old file still held by a reader,
	// including when a previous process died between file and ready publication.
	final := out.Name() + ".db"
	if err = os.Rename(out.Name(), final); err != nil {
		return err
	}
	if err = internal.FsyncDir(dir); err != nil {
		return err
	}
	descriptor, err := json.Marshal(ready{File: filepath.Base(final), TXID: pos})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "ready-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	if _, err = tmp.Write(descriptor); err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp.Name(), filepath.Join(dir, "ready.json")); err != nil {
		return err
	}
	return internal.FsyncDir(dir)
}
