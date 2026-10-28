# Follow into immutable copies

This Linux example uses the Go library to follow a single source in S3. The
working database is private. Readers use separate files that never change.

```text
S3 changes -> working.db -> synchronous copy -> immutable file -> reader
                          (follow waits)       ready.json
```

Run from this fork with S3 credentials supplied through the normal AWS SDK
credential chain:

```sh
go run ./examples/snapshot-follow \
  -bucket example -prefix database -region us-east-1 \
  -endpoint http://localhost:9000 -dir /tmp/serving-copies
```

Omit `-endpoint` for AWS. The example calls `Replica.Restore` with `Follow` and
`OnApplied`; it does not launch the Litestream CLI.

After bootstrap and each successful apply pass, the callback copies the working
file, syncs it, renames it to a unique filename, and atomically replaces
`ready.json`. The follow loop waits until the callback returns. A reader polls
that descriptor, opens the named file with SQLite `mode=ro&immutable=1`, and
holds that connection for the request. Old requests can continue reading the
previous file. Never open the working database from the reader.

If applying or copying fails, the process exits. Its last ready file remains
available. Restarting restores a fresh working database and then continues
following; it does not resume an uncertain file. A lower or equal transaction
ID does not replace the existing ready descriptor. This assumes one unchanged
source database history. Source replacement and branch detection require an
application identity and validation policy.

## Limits of the example

- Exactly one writer must own the output directory. This example does not
  implement process locking, automatic restart/backoff, or an HTTP endpoint.
- All serving files are retained. A production reader must coordinate retention
  with its open handles and enforce a disk budget. After an unclean exit,
  temporary files and old working directories also need cleanup.
- Litestream verifies the applied stream. The application still needs schema
  and content validation before allowing its readers to switch.
- Updates can be coalesced into one callback. There is no freshness deadline or
  synchronization across replicas. Missing origin history may require a new
  full snapshot and a fresh session.
- Each copy pauses following and temporarily needs another full database-sized
  file. Every restart downloads a fresh baseline. Directory sync calls assume a
  local Linux filesystem; power-loss behavior and PVC performance need testing.

## Tests

```sh
go test -race -count=1 -run '^TestReplica_OnApplied' .
go test -race -count=1 ./examples/snapshot-follow
```

The example test publishes A and B through a local file replica, holds an old A
reader while B becomes ready, and checks that copy failure and an older position
do not replace B. The library tests check callback blocking, apply errors after
partial progress, callback errors, and rejection of reused working databases.
