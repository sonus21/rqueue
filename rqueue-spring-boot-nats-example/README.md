# rqueue-spring-boot-nats-example

Spring Boot example app using **NATS / JetStream** as the Rqueue backend, mirroring
[`rqueue-spring-boot-example`](../rqueue-spring-boot-example) (which uses Redis).

The application code is identical to the redis example modulo two small things:

- **Delayed and periodic enqueue endpoints are removed.** The v1 NATS broker doesn't model
  delayed or scheduled delivery — the redis backend's ZSET schedulers don't exist on the NATS
  side, and the broker throws `UnsupportedOperationException` for those calls.
- **`application.properties` selects the NATS backend** via `rqueue.backend=nats` and points
  `rqueue.nats.connection.url` at a JetStream-enabled `nats-server`.

Backend selection is a property switch only — the listener / controller / domain code is
unchanged from the redis example, which is the whole point of the pluggable-backend split.

## Running locally

Start a JetStream-enabled NATS server (any one of these works):

```sh
# native binary
nats-server -js

# docker
docker run -p 4222:4222 nats:latest -js
```

Then:

```sh
./gradlew :rqueue-spring-boot-nats-example:bootRun
```

Once the app is up:

```sh
# enqueue a String to a queue (default queue is "simple-queue", from application.properties)
curl 'http://localhost:8080/push?q=simple-queue&msg=hello'

# enqueue a Job object to job-queue (with DLQ wired to job-morgue)
curl http://localhost:8080/job
```

Watch the logs for `simple: hello` / `job-queue: Job(id=…)` from `MessageListener`.

## Inspecting the JetStream state

`nats stream ls` will show:

```
rqueue-js-simple-queue
rqueue-js-job-queue
rqueue-js-job-queue-dlq
rqueue-js-job-morgue
```

(The `rqueue-js-` prefix is the default; configure via `rqueue.nats.naming.streamPrefix`.)

`nats kv ls` will show the six shared KV buckets used by the NATS-backed daos
(`rqueue-jobs`, `rqueue-locks`, `rqueue-message-metadata`, etc.). See the README's
"NATS backend" section in the repo root for the full table.

## Locked-down JetStream accounts

If your NATS account can't run `add_stream` / `kv_create` at runtime, set:

```properties
rqueue.nats.auto-create-streams=false
rqueue.nats.auto-create-consumers=false
rqueue.nats.auto-create-dlq-stream=false
rqueue.nats.auto-create-kv-buckets=false
```

…and pre-create the streams + buckets per the root README. `NatsStreamValidator` /
`NatsKvBucketValidator` will fail boot deterministically with the list of missing
streams / buckets if any are not present.
