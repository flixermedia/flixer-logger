# Flixer Logger

This repository is under development and must not be used for production logging.

The machine-readable OpenAPI 3.1 contract is available at
`GET /api/openapi.json` and in [`internal/httpapi/openapi.json`](internal/httpapi/openapi.json).
It describes the current experimental API, not unimplemented specification
endpoints.

The current storage package provides deployment-scoped Pebble persistence,
durable batch commits, event ID deduplication, bounded recent-event reads,
single-field indexed queries and bounded manual retention.
The experimental retention worker only deletes events for deployments with an
explicitly enabled policy; no policy means cleanup is disabled. A
deployment-scoped `manage` credential can read or replace its policy via
`GET`/`PUT /api/logger/retention` with
`{"enabled":true,"days":14,"severity_days":{"trace":2,"error":30}}`.
Overrides are optional, accept the six standard levels and 1–3650 days, and
replace rather than merge with the existing policy; unspecified levels use
the default. Older clients that omit overrides clear them on update.
The worker checks up to 100 deployments per minute and deletes at most 500
expired events per deployment per pass, using ingestion time. Disabling a
policy prevents subsequent batches but cannot restore events already deleted.
Deletion does not guarantee immediate physical disk reclamation: Pebble may
reclaim space later during compaction. Automated Flixer-managed provisioning
is not yet implemented.
The deployment management credential can read or set the quota using
`GET /api/logger/quota` and `PUT /api/logger/quota`, with `{"max_bytes":0}`
for unlimited or a whole-byte limit
up to 1 PiB. The response includes `used_bytes`: the serialized event-body bytes
currently retained for the deployment. The quota excludes indexes, database
files and other physical disk use; it is not a disk-space limit. Ingestion that
would exceed it is rejected atomically with HTTP 429 and `Retry-After`, and
producers may drop rejected events. Retention reduces the logical usage; Pebble
may reclaim physical space later.
Batch ingestion accepts uncompressed JSON or `Content-Encoding: gzip`; both
compressed and decompressed request bodies are limited to 2 MiB.
Producers can send `X-Flixer-Log-Schema: 1`; the response advertises schema 1
in the same header. For compatibility the request header may be omitted, while
unsupported or repeated versions return HTTP 409 without storing the batch.
The experimental server accepts at most 16 concurrent ingest requests and
eight concurrent historical query/statistics requests. Each deployment can
use at most four concurrent requests in each category; excess requests return
HTTP 429, while exhausted global capacity returns HTTP 503. Both provide
`Retry-After`. Separately, per-deployment token-bucket ingestion limits default
to 2,000 events per second with a 5,000-event burst. A `manage` credential can
read or set these with `GET`/`PUT /api/logger/ingestion-policy` using
`{"events_per_second":2000,"burst":5000}`. Rates range from 1 to 100,000
events/second and burst sizes from 1,000 to 1,000,000 events. The policy is
persisted; bucket state is in memory and starts full on restart. Rejected
batches return 429 and `Retry-After`, and retried duplicate events consume
capacity. Optional `credential_events_per_second` and `credential_burst` limits
apply independently to each ingest credential in addition to the deployment
limit; both must be set together. Their valid ranges match the deployment
limits. Omit both (or set both to zero) to disable them. Policy updates
replace the entire policy, so older clients that omit these fields disable
credential limits. State is process-local and restarts full; rotating a
credential creates a fresh bucket, while the deployment limit still applies.
Optional `service_events_per_second` and `service_burst` limits work the same
way, but share a bucket across all credentials bound to the same application.
When enabled, unbound credentials are rejected with HTTP 403 rather than being
allowed to bypass the service limit by changing their claimed application.
Migrate all producers to application-bound credentials before enabling this
option. Rotating a bound credential does not reset its service bucket.
There is no hard physical disk quota.
New events additionally have a case-insensitive, exact single-token message and
structured-error
index exposed through `GET /api/logs?field=text&value=token`. This is not
full-text search: phrase, substring, Boolean, and error/field searching are not
available on the indexed `GET` route. Long tokens use SHA-256 index keys;
existing shorter-token keys remain compatible. All distinct tokens in an
accepted event are indexed. To bound write amplification, a batch may contain
at most 128,000 distinct per-event token postings; an over-budget batch is
rejected atomically and must be split. Events remain limited to 64 KiB.
On first startup, existing events are indexed in resumable batches.
`POST /api/logs/search` browses events or intersects exact indexed fields and up to eight
case-insensitive message/error tokens, within an explicitly supplied ingestion-time
window of at most 31 days. It returns pages of at most 200 events, newest-first
by default; `order: "oldest"` selects oldest-first. Cursors are bound to the
deployment, filters, order and time window. Searches examine
at most 10,000 candidates and run for at most five seconds; exceeding the
candidate budget returns HTTP 422 instead of partial or unbounded results.
The optional `task_queue` field requires `task` and matches the exact string in
`fields.task_queue` after indexed task lookup. It is not itself indexed, and
events without that string do not match. Candidate limits still apply.
With `order: "oldest"`, `follow: true` returns a continuation cursor on every
non-empty page, including the last currently available page. Poll that cursor
later for newly ingested events; on an empty page retain the previous cursor.
The time window remains fixed and still limits which future events can match.
An optional `projection` array selects response fields; `event_id` is always
included, and missing optional fields remain omitted. Omit it or use `null` for
full events; an empty array returns only IDs. Projection reduces response size,
not the work needed to read or search stored event bodies. It can change
between pages without invalidating the cursor.
`GET /api/logs/trace/{trace_id}` uses the trace index for deployment-scoped,
newest-first correlation; `limit` is 1–1,000 and `cursor` pages older events.
The optional `text_mode` selects `all` (default token intersection), `phrase`
(token-indexed then checked for a contiguous case-insensitive match),
`substring`, or `regex`. Substring and regex use a bounded scan after other
indexed filters and may return HTTP 422 when the candidate budget is exceeded.
Regex patterns use Go's RE2-compatible syntax, are case-sensitive unless
`(?i)` is specified, and are limited to 256 UTF-8 bytes. Invalid patterns
return HTTP 400. Matches cover the message and structured error type, code and
stack.
`GET /api/logs/stats?start=<RFC3339>&end=<RFC3339>` returns sparse, UTC
minute buckets of counts by level, based on ingestion time. Bounds must be
minute-aligned and no more than 31 days apart. Counts are updated atomically
with ingestion and retention, and are rebuilt on startup if their schema
marker is absent. `GET /api/logs/sources?start=<RFC3339>&end=<RFC3339>`
uses separate minute aggregates for complete counts by application and source.
It limits the scan to 100,000 minute entries and 1,000 distinct groups;
exceeding either limit returns HTTP 422 rather than partial totals. Existing
stores rebuild these aggregates before accepting traffic on first upgrade,
which can delay startup on large stores. `GET /api/logs/errors?start=<RFC3339>&end=<RFC3339>`
groups error and fatal events by exact app, source, error type, code and
message, ranking by count. It uses severity indexes, scans at most 10,000
events, supports at most 1,000 groups and shares the five-second query budget;
exceeding a limit returns HTTP 422 rather than incomplete counts. Results use
successive snapshots and are not a transactional view while retention or
ingestion is changing the window. Other analytics and facets are not yet
implemented.
`GET /api/health/live` checks process liveness, while `/api/health/ready`
also checks that the required Pebble index metadata can be read. Readiness is
not a comprehensive disk-space or corruption check.
`GET /api/status` requires a deployment `manage` credential and reports only
that deployment's last retained ingestion time, current live subscriber count,
and active and maximum ingestion/query requests. It also reports the most
recent retention worker attempt, its deletion count and whether the batch limit
was reached. A full batch does not prove more expired events remain. Status is
in memory, limited to 10,000 recently swept deployments, and resets on restart;
null means no recorded attempt, not that retention is complete. These activity counts are
instantaneous and do not provide global disk usage, capacity, or retention
progress; exposing global metrics to deployment administrators would leak
information about other tenants.
An operator can separately provision a bootstrap credential with `scope` set to
`owner` and a nonempty `deployment_id` in the protected digest file. The
deployment ID is required by the bootstrap format but does not grant tenant
access. Owner credentials cannot query, ingest or use tenant management
endpoints. `GET /api/owner/capacity` returns filesystem size and available
space, Pebble's reported database disk usage, estimated compaction debt and
compactions in progress. These global metrics are intentionally unavailable to
tenant credentials. Pebble's database metric includes live and obsolete files
but is not an accounting of other files, checkpoints or all filesystem usage.

The owner may set a persisted global best-effort ingestion guard through
`PUT /api/owner/capacity-policy` with both `max_database_bytes` and
`min_free_bytes`; `GET` reads it. Zero disables a threshold, and the maximum
for either is 1 PiB. Before committing a batch containing new events, the
logger compares current Pebble-reported disk usage and available filesystem
space plus the encoded Pebble batch size against those limits. A rejected
batch returns HTTP 503 with `Retry-After`; queries and retention remain
available. Duplicate-only retries remain accepted. This is **not a hard disk
quota**: batch encoding is not the eventual physical size, Pebble compaction,
checkpoints and other processes can consume space after the check, and
deletions may not reclaim disk immediately. Configure and monitor filesystem
limits independently. An administrator may manually register a separate
owner credential in Flixer Web's Logging panel to view these metrics and
manage the global guard. Registration stores the credential encrypted in
Flixer, binds it to the configured logger origin and never displays it again.
An optional private-file bootstrap can generate the initial owner token, but
connecting it to Flixer remains manual. Go producers can obtain application-bound
credentials from Flixer over HTTPS, literal loopback, or a protected local
Unix socket once it is paired.
The experimental HTTP handler authenticates deployment-scoped ingest and query
credentials and applies best-effort redaction. Its internal store does not
perform authentication or redaction; producers must avoid sensitive values.
The experimental Go exporter offers bounded, non-blocking enqueue and bounded
retry; it drops events on queue overflow or delivery exhaustion and does not
provide an on-disk spool. Its `slog` adapter forwards records to the existing
handler and separately applies best-effort redaction before export. Its error
callback must be non-blocking and must not log through the same adapter.
The managed `slog` service refreshes signed producer configuration every five
seconds, suspends export when confirmation fails and creates a separate
exporter when a credential changes. Upon a change or failed confirmation, it
discards the previous exporter's queued events and cancels in-flight requests.
Events can still be sent to a previous deployment before the next refresh;
requests already accepted by the old endpoint cannot be recalled. The control signature uses the shared application
secret, not an individual service identity. Streamer, Indexer and Updater
carry checked-in snapshots of this SDK for independently reproducible builds
while the logger module is unpublished. Refresh a snapshot with
`bash scripts/vendor-go-sdk.sh /path/to/producer`; the script refuses to
overwrite locally modified snapshot files. The Flixer Compose configuration
uses a signed Unix socket on its shared secrets volume for local producer
enrollment; it does not send those credentials over the plaintext API network.
The logger endpoint itself still requires HTTPS except on literal loopback.
This is not a complete production deployment: Compose does not yet provision
or pair a logger, and legacy logs and task diagnostics remain active.
Producer credentials issued with a recorded ID are cleared when revoked through
Flixer administration and reissued on the next enrollment refresh. Revocations
made directly at the logger, or credentials issued before Flixer recorded IDs,
are not automatically detected; reconnect or re-pair Flixer to replace them.
Delivery retries use bounded exponential backoff with jitter and honor
`Retry-After` up to a 30-second ceiling. An invalid batch is split to isolate
rejected events; valid neighboring events can still be delivered.
After three consecutively exhausted transient batch deliveries, its circuit
opens for 30 seconds by default; batches during that cooldown are dropped,
counted and reported rather than sent. The threshold and cooldown are
configurable in the exporter. A later successful delivery closes the circuit.
The Go exporter also accepts optional per-level sample fractions from 0 to 1;
levels omitted from the policy are retained. Sampling hashes the event ID, so
the same ID and policy produce the same decision. Intentional samples are
counted separately from delivery drops. Configuration is currently available
only to Go exporter callers, not yet through Flixer administration.
The directory passed to `store.Open` is a data root, with the database stored
under `logs/logger/pebble`.
For safe storage, `DATA_DIR` and its `logs` directory must be real directories
owned by the logger user or root, with no group or world write permission.
`logs/logger` and `logs/logger/pebble` must be real directories owned by the
logger user; `logs/logger` must be inaccessible to other users. The logger refuses unsafe
permissions rather than changing them. Ancestors above `DATA_DIR` must also
be protected from untrusted users. The existing Flixer Compose stacks make
`logs` world-writable for legacy file logs and are not compatible with this
storage layout; do not mount logger data there until the stacks are migrated.

Run tests with `go test -race ./...`. The experimental server requires
`--data-dir` and exactly one bootstrap method:

- `--credentials-file` points to an owner-only regular file containing
  provisioned credential digests.
- `--bootstrap-owner-file` points to a file in an existing private directory
  owned by the logger user. For a new store only, the logger creates a random
  owner token there with mode 0600. The file contains the **reusable plaintext
  owner credential**: keep it private, back it up securely and never log it or
  expose it to untrusted services. Its parent and ancestors must be protected.
  The token is not emitted on stdout or placed in process arguments. If the
  token is missing for an existing store, startup fails instead of silently
  changing the logger identity. A failed or interrupted initial file write
  must be investigated before a new store is initialized.

Neither file may be a symbolic link. Update the selected file atomically at the
same path before requesting a reload. Non-loopback listeners require
`--tls-cert` and `--tls-key`. Creating the owner secret does not automatically
connect Flixer or provision producer credentials; pairing remains necessary.
For example, after creating a private directory for the logger user:

```sh
mkdir -m 700 "$HOME/.flixer-logger"
flixer-logger --data-dir "$DATA_DIR" --bootstrap-owner-file "$HOME/.flixer-logger/owner"
```

The default listener is loopback-only. For a container, the private directory
and data root must be writable by UID 65531 and persist across container
restarts; protect the owner file independently of the Pebble data.
`go test ./internal/store -run '^$' -bench '^BenchmarkConcurrentBatchIngestion$' -benchtime=3s`
measures concurrent 100-event durable storage batches on the current machine.
It excludes HTTP, authentication, redaction and cross-service traffic and is
not an end-to-end throughput acceptance test.
`go test ./internal/httpapi -run '^$' -bench '^BenchmarkHTTPBatchIngestion$' -benchtime=3s`
adds loopback HTTP, authentication and redaction with four producers. It is
still not a multi-host, TLS, mixed-query or sustained deployment acceptance
test; results depend on the machine and storage.
On the current development machine (Intel Xeon E5-2690 v2), a short run of
these benchmarks measured approximately 1,160 HTTP events/second and 1,375
storage events/second using its default temporary filesystem. A 10-batch
storage-only run in tmpfs measured about 27,755 events/second, but tmpfs does
not demonstrate durable storage. These measurements do not establish the
5,000 events/second burst target or the required mixed-load acceptance.
The separate `BenchmarkHTTPBurstBatchIngestion` uses ten 500-event batches
with four loopback clients; three runs measured approximately 5,335, 4,882,
and 4,173 events/second. Its `-benchtime=10x` workload fits the default
5,000-event token-bucket burst, but has no concurrent queries or live
subscribers and does not model actual producer batching. Larger batches
alone therefore do not establish the burst acceptance target.
`go test ./internal/httpapi -run '^$' -bench '^BenchmarkHTTPMixedBurst$' -benchtime=10x -count=3`
adds a continuously consumed live stream and indexed historical searches at
roughly 10 requests/second while four producers send ten 500-event batches.
It checks every live event ID and validates the search response; its elapsed
time includes waiting for the live subscriber to receive all 5,000 events.
After optimizing redaction without changing its matching behavior, three local
runs measured approximately 1,724, 5,548 and 1,981 events/second with 9–24
searches per run. These highly variable loopback measurements do not
establish the target reliably, nor do they cover TLS, multiple hosts, sustained
traffic, realistic producer batch sizes, or deployment recovery.
`go test ./internal/httpapi -run '^TestAcknowledgedIngestionSurvivesProcessCrash$'`
checks that an HTTP-acknowledged event, its index, deduplication marker and
source aggregate survive killing the logger process and reopening Pebble.
This does not simulate power loss, filesystem failure or corruption.
Sending SIGHUP reloads the digest file atomically; an invalid reload is logged
and leaves the previous credentials active. Existing live streams revalidate
on each event or heartbeat and close when a credential is revoked, expires or
is reassigned to another deployment.
An operator-provisioned `manage` credential can issue expiring `ingest` or
`query` credentials for its own deployment using
`POST /api/logger/credentials` with `scope` and an RFC3339 `expires_at`
(maximum lifetime one year). The token is returned only at issuance; only
its digest is stored in Pebble. `GET /api/logger/credentials` lists metadata,
and `DELETE /api/logger/credentials/{id}` revokes an issued credential.
An ingest credential can specify an `app` restriction, a lowercase application
name starting with a letter and containing up to 64 letters, digits or hyphens.
Every event in its batch must use that exact app; a mismatch rejects the entire
batch with HTTP 403. Credentials without `app` remain unrestricted for
compatibility, including legacy credentials. Listing metadata includes `app`
when restricted. Bootstrap ingest credentials may also specify `app`.
These operations require TLS outside loopback and tenant credentials cannot
create management credentials. Bootstrap credentials remain managed through
the digest file. A logger owner may separately issue a deployment management
credential with `POST /api/owner/credentials`, supplying `deployment_id`
and `expires_at` (maximum lifetime one year). The token is returned once and
must be transferred securely to that deployment. `GET /api/owner/credentials`
and `DELETE /api/owner/credentials/{id}` require a `deployment_id` query
parameter and list or revoke only owner-issued management credentials. They
cannot revoke bootstrap credentials. Tenant credential endpoints cannot list,
create or revoke these management credentials. Revoking a management
credential does not revoke ingest or query credentials it issued; review and
revoke those separately when responding to compromise. This is a manual
operator workflow, not automated enrollment or pairing.
Issued credentials survive restarts and bootstrap reloads;
expired entries are removed atomically when another credential is issued.
For a one-time enrollment, an owner can create a pairing via
`POST /api/owner/pairings` with `deployment_id`, `expires_at` (at most
15 minutes) and `management_expiry` (later than pairing expiry and at most
one year). The response contains a short-lived secret only once. The owner
can list unexpired metadata or revoke a pending pairing using
`GET /api/owner/pairings?deployment_id=...` and
`DELETE /api/owner/pairings/{id}?deployment_id=...`.
Transfer the pairing secret through a trusted channel, then the receiving
deployment can exchange it at `POST /api/logger/pairings/redeem` with
`{"token":"..."}`. This response contains the deployment-bound management
credential only once. Never put either token in a URL or logs. TLS is required
outside loopback. Pairing consumption and credential issuance are committed
in the same durable batch, so concurrent or replayed redemptions cannot
produce multiple credentials. If the response is lost, the pairing cannot be
retried; an owner must create another and revoke any unwanted credentials.
Flixer Web can create and revoke owner invitations and can submit an
invitation to a receiving Flixer instance. Flixer exchanges the invitation
directly with the logger and encrypts the resulting management credential;
it never returns that management credential to the browser. If a later
registration step fails, Flixer keeps the redeemed credential encrypted and
offers a retry. If the exchange response is lost before Flixer can persist
it, the owner must create a new invitation. Automatic Flixer connection and
management credential renewal are not implemented.
Flixer's experimental `/api/admin/logger` endpoints can store an operator-
provisioned management token encrypted in its configuration store, manage the
deployment's retention policy, and broker 60-second query tokens for an admin.
Pairing is available as described above, but connecting Flixer is not
automatic. Once paired, supported producers can enroll over a protected
control connection.
After registration, Flixer issues and encrypts an ingestion credential and
exports its own logs through a bounded in-memory queue while keeping the legacy
appenders. It renews the credential before expiration when the logger is
reachable. Registration and renewal require a logger version that supports
application-bound credentials: Flixer requests an ingest credential bound to
`flixer` and verifies the returned binding. Existing legacy tokens are renewed
to obtain a binding; if renewal fails, the existing token remains configured.
Flixer checks its locally stored credential every minute and retries a failed
renewal after roughly one minute with jitter. A healthy credential does not
cause a network request on each check. Expired credentials cannot deliver until renewal
succeeds; queued or rejected events can be lost.
Renewal does not automatically revoke the previous token, which remains valid
until its expiry or explicit revocation. Renewal for the same deployment
preserves queued records; an in-flight batch may still use the old credential,
and a rejected in-flight batch is retried with a renewed token when possible.
Keep both credentials valid during the transition. A manual configuration change
discards pending records to avoid sending them to a different deployment.
Delivery is best effort: records can be dropped on queue overflow,
repeated delivery failures, shutdown, or target changes.
On normal Flixer shutdown, the API attempts to drain buffered events for up to
two seconds; a stalled delivery is aborted and any remaining events are dropped.
Abrupt process termination can lose unflushed events.
The producer exports
only safe-shaped event, task, request and trace identifiers, bounded duration,
and valid HTTP status codes from structured context; other context is not yet exported. Producer-side redaction is best
effort; do not put sensitive data in log messages or identifiers.
Browser-facing query routes allow cross-origin requests using explicit bearer
credentials and never enable cookie credentials. Ingestion and management
routes do not enable CORS.

For an operator-controlled, consistent backup, start the logger with
`--checkpoint-dir /absolute/path` and send its process `SIGUSR1`; completion or
failure is logged on stdout/stderr. The directory is created with mode 0700
and must remain owned by the logger user and inaccessible to other users.
Each signal creates a new Pebble checkpoint in that directory. Checkpoints
contain **all deployments**, may contain sensitive data, and can retain disk
space as the live database compacts; protect and expire them separately.
Restore only while the logger is stopped: put a complete checkpoint at
`DATA_DIR/logs/logger/pebble` and start the logger. Do not copy a live Pebble
directory directly. Restore orchestration and managed backup scheduling are
not yet implemented.

The deployment-scoped live stream emits an SSE `id` for each committed event.
At most 10 streams per deployment and 100 streams globally may be open.
Exceeding the deployment limit returns HTTP 429 with `Retry-After`; global
exhaustion returns HTTP 503.
Optional exact `app`, `instance`, `level`, `source`, `event`, `trace`, `request`,
`task`, and `task_queue` query parameters filter both replay and new deliveries;
`task_queue` requires `task` and matches the string in `fields.task_queue`. Each may be
specified once. Replay budgets apply to all events scanned, including
nonmatching events.
Send the last processed ID as `Last-Event-ID` when reconnecting. A Pebble
snapshot replays up to 1,000 later events and 8 MiB within five seconds,
then transitions to live delivery. Replay returns HTTP 410 if its cursor was
removed by retention or either replay size limit is exceeded; clients must
show a gap and use historical search. Connections lost before receiving their
first event have no cursor and cannot be replayed. A slow subscriber receives
an `overflow` event and disconnects; it can attempt replay from its last ID.
Delivery is ordered by server ingestion time and event ID, not producer time.
Each live subscriber buffers at most 1,024 events and 4 MiB of serialized
event data and cursors, excluding queue and object overhead. The event-count
limit accommodates a 1,000-event HTTP batch in an empty queue, but unusually
large serialized batches can exceed the byte limit. Sustained slow readers can
also overflow and must replay or search for missing events.
Concurrent commits and subscription snapshots are serialized to prevent a gap
between replay and live delivery; this overhead has not been benchmarked under
the full target workload.

The repository includes an experimental non-root Docker image. It deliberately
has no default credentials or deployment configuration; the writable data
directory and owner-only credential digest file must be provisioned for UID
65531. This UID differs from the Indexer and Streamer images' UID 65532 so
their processes cannot access private logger files just because they share a
host. Earlier experimental logger images used UID 65532; stop the logger and
securely update ownership of its existing private data and credential file
before upgrading, without changing unrelated files. Non-loopback listeners
require a certificate and private key readable by that user. This image is not
yet integrated with Flixer provisioning or Compose.

The Go exporter supports `RotateToken` for an unchanged destination. New
delivery attempts use the replacement token; an already in-flight request can
still use the previous token, so allow overlap before revocation. If an
in-flight request returns 401 after rotation, it is retried with the replacement
when the batch has attempts remaining. Syntactically invalid
replacements leave the previous token in place. Callers must verify that a
replacement belongs to the same deployment and application before rotating:
the exporter cannot verify this from the token. This method does not provision
or watch credentials, reset an open circuit, or change the logger endpoint.

Flixer Web includes an experimental Logging page for connection, retention, logical quotas,
recent and indexed historical events, severity totals and live viewing. Live
complete full-text search and analytics, pairing, physical disk quotas,
multi-service integration and deployment described in the logging
specification are not yet implemented.
