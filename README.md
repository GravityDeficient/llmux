# llmux

[![Crates.io](https://img.shields.io/crates/v/llmux)](https://crates.io/crates/llmux)
[![GitHub](https://img.shields.io/badge/GitHub-GravityDeficient%2Fllmux-blue)](https://github.com/GravityDeficient/llmux)

LLM multiplexer. Routes OpenAI-compatible requests to model backends and
switches between them on demand using user-provided scripts.

When a request arrives for a model that isn't currently loaded, llmux drains
in-flight requests, runs your **sleep** script on the active model, then runs
your **wake** script on the requested model. The API stays up throughout —
clients just change the `model` field.

llmux doesn't manage model processes directly. You provide three shell scripts
per model (**wake**, **sleep**, **alive**) and llmux calls them at the right
time. This means it works with any backend — vLLM, SGLang, llama.cpp, Ollama,
or anything else that speaks HTTP.

## Install

```sh
cargo install llmux
```

## Quick start

Create a `config.yaml`:

```yaml
models:
  llama:
    port: 8001
    wake: ./scripts/wake-llama.sh
    sleep: ./scripts/sleep-llama.sh
    alive: curl -sf http://localhost:8001/health

  mistral:
    port: 8002
    wake: ./scripts/wake-mistral.sh
    sleep: ./scripts/sleep-mistral.sh
    alive: curl -sf http://localhost:8002/health

bind_address: 127.0.0.1
port: 18080

auth:
  # `env:NAME` resolves the secret without placing it in this file.
  inference_bearer_token: env:LLMUX_INFERENCE_TOKEN
  control_bearer_token: env:LLMUX_CONTROL_TOKEN

orchestration:
  interactive_lease_secs: 600
  background_max_wait_secs: 1800
  state_path: /var/lib/llmux/state.json
```

Run it:

```sh
llmux -c config.yaml
```

Send requests:

```sh
curl http://localhost:18080/v1/chat/completions \
  -H "Authorization: Bearer $LLMUX_INFERENCE_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"model": "llama", "messages": [{"role": "user", "content": "Hi"}]}'
```

When a request comes in for `mistral`, llmux will drain active requests,
run `sleep-llama.sh`, then `wake-mistral.sh`, and proxy the request through.

## How it works

```
Client requests
     |
+---------+
|  llmux  |   port 3000 (OpenAI-compatible proxy)
+---------+
 /         \
[8001]    [8002]
 llama     mistral
(active)   (sleeping)
```

1. **Middleware** extracts the `model` field from the request JSON body
2. **Switcher** checks if that model is active. If not, triggers a switch:
   - Drains in-flight requests for the current model
   - Runs the **sleep** hook on the current model
   - Runs the **wake** hook on the target model
3. **Proxy** forwards the request to `localhost:<model_port>`
4. In-flight tracking uses RAII guards that hold through streaming responses

## Configuration

### Models

Each model needs a `port` and three hooks:

```yaml
models:
  my-model:
    port: 8001
    wake: |
      # Bring the model to a ready state (must be idempotent).
      # Exit 0 when the model is ready to serve requests.
      docker start my-model-container
      for i in $(seq 1 60); do
        curl -sf http://localhost:8001/health && exit 0
        sleep 1
      done
      exit 1
    sleep: |
      # Free resources. Exit 0 when done.
      docker stop my-model-container
    alive: |
      # Health check. Exit 0 = healthy, non-zero = unhealthy.
      curl -sf http://localhost:8001/health
    metadata:
      description: Qwen interactive model
      topology: tp1
      context_length: 262144
      quantization: nvfp4
      speculative_decoding: mtp-3
      impact: []
```

Hooks are executed via `sh -c` with `LLMUX_MODEL` set in the environment.
They can be inline scripts (YAML `|` syntax) or paths to executables.
`metadata` is optional and is returned verbatim as typed operator-facing data
by the control API; it does not change scheduling.

### Authentication

Inference and control routes have independent optional bearer tokens. Tokens
may be literal strings or `env:VARIABLE_NAME` references. When inference auth
is configured, authentication occurs before the request body is inspected, so
an unauthorized request cannot start or stop a model. `/metrics` intentionally
remains unauthenticated for a loopback Prometheus scrape.

### Leases, priority, and pins

Every completed interactive response renews a rolling lease for its model
(10 minutes by default). An interactive request for another model preempts an
unpinned lease immediately. A background request for another model waits until
the lease expires and the active model drains; after
`background_max_wait_secs` it receives `503 Service Unavailable` with
`Retry-After: 30`.

Traffic is interactive unless the trusted front proxy sets:

```http
X-LLMux-Priority: background
```

The header is removed before proxying to the model backend. Do not expose
llmux directly to untrusted clients when priority enforcement matters; have
the front proxy overwrite the header.

A pin prevents all inference-triggered switches. It persists when
`orchestration.state_path` is configured. An authenticated control-plane
switch moves an existing pin to the selected model; unpinning allows queued
work to resume.

### Control API

All model-changing bodies have the form `{"model":"<configured-id>"}`:

```text
GET    /control/v1/state
POST   /control/v1/switch
POST   /control/v1/pin
DELETE /control/v1/pin
```

Example:

```sh
curl -fsS http://127.0.0.1:18080/control/v1/pin \
  -H "Authorization: Bearer $LLMUX_CONTROL_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"model":"qwen3.8-27b"}'
```

The state response contains flat `state`, `active_model`, `target_model`,
`pinned`, `pinned_model`, lease deadline/countdown, per-model `queues` and
`in_flight` maps, `last_switch`, `last_error`, and a flattened model catalog.

At startup llmux runs every `alive` hook. Zero healthy models reconciles to
idle, one becomes active, and more than one aborts startup. A persisted pin is
then restored. A failed sleep is fail-closed: the target is never woken. A
failed wake is cleaned up and the previous model is restored when possible.

### Policy

```yaml
policy:
  # Max time a request waits for a switch. Omit for unlimited.
  request_timeout_secs: 300

  # Wait for in-flight requests to finish before switching. Default: true.
  drain_before_switch: true

  # Minimum seconds a model stays active before it can be switched out.
  # Prevents rapid thrashing. Default: 0.
  min_active_secs: 5
```

### Full config reference

```yaml
models:
  <model-name>:
    port: <u16>         # Where the backend listens
    wake: <string>      # Script to start/restore the model
    sleep: <string>     # Script to stop/checkpoint the model
    alive: <string>     # Health check script

policy:
  request_timeout_secs: <u64 | null>   # null = unlimited
  drain_before_switch: <bool>          # default: true
  min_active_secs: <u64>               # default: 0

auth:
  inference_bearer_token: <string | env:NAME | null>
  control_bearer_token: <string | env:NAME | null>

orchestration:
  interactive_lease_secs: <u64>        # default: 600
  background_max_wait_secs: <u64>      # default: 1800
  state_path: <path | null>             # configure for persistent pins

bind_address: <string>                  # default: 0.0.0.0
port: <u16>                             # default: 3000
```

Both YAML and JSON configs are supported (detected by file extension).

## Metrics

Prometheus metrics are available at `/metrics` on the same listener. The
controller-specific series are:

- `llmux_active_model_info{model}`
- `llmux_pin_info{model}`
- `llmux_lease_remaining_seconds`
- `llmux_request_queue_depth{model,priority}`
- `llmux_pending_request_cancellations_total`
- `llmux_background_wait_timeouts_total`
- `llmux_reconciliation_total{result}`
- `llmux_lifecycle_state_info{state}`

Existing switch, drain, hook, request, queue-wait, and in-flight series remain
available. Request counters and duration histograms now include a `priority`
label.

## Container

The production image is multi-architecture (`linux/amd64` and `linux/arm64`),
runs as UID/GID 65532, and includes `curl` and CA certificates for hooks. Mount
the config read-only and a writable state directory, then use host networking
when hooks address host-local lifecycle services:

```sh
docker run --rm --network host \
  -e LLMUX_INFERENCE_TOKEN -e LLMUX_CONTROL_TOKEN \
  -v ./config.yaml:/etc/llmux/config.yaml:ro \
  -v llmux-state:/var/lib/llmux \
  ghcr.io/gravitydeficient/llmux:latest
```

## Examples

### Podman + CRIU (GPU checkpoint/restore)

The [`examples/podman-criu/`](examples/podman-criu/) directory shows how to
use CRIU to checkpoint and restore vLLM containers, achieving ~3x faster
model switches vs. cold start. See the [example README](examples/podman-criu/README.md)
for setup instructions and timings.

## License

MIT


## Manual model control

For a dedicated GPU pair, set:

```yaml
orchestration:
  manual: true
  active_alias: spark-active
  drain_timeout_secs: 600
  state_path: /var/lib/llmux/state.json
```

Only authenticated control requests can load or stop models. Inference for an
inactive model returns 503 immediately. There are no automatic model queues,
lease switches, startup loads, or fallbacks in this mode. `/control/v1/switch`
loads the selection; `/control/v1/stop` unloads it. The old pin endpoint remains
an alias for Load; unpin is rejected. Startup only adopts an already healthy
model and never wakes a saved selection.

A switch blocks new requests, waits for response streams to finish, stops the
old model, then loads the selected one. If draining times out, the old model
stays running. If stop fails, no new model starts. If load fails, cleanup runs;
failed cleanup must succeed before a later Load can proceed. Lifecycle hooks
must verify process exit and memory release on all participating hosts.

Status includes `manual` and `phase` (draining, stopping, loading, ready,
failed, stopped), plus existing request counts and errors. `active_alias`
rewrites the request's model field to the active model ID before proxying.
Responses retain the backend's real model ID. Explicit model names keep their
normal meaning. A per-model `host` selects a remote backend; default is
`127.0.0.1`.

For an upgrade without stopping inference, run a second manual router on a
spare loopback port. Lock model controls, move new traffic using a graceful
reverse-proxy reload, and wait for the old router's requests to finish before
stopping it. Do not enable model controls while both routers serve requests:
in-flight counts belong to each process.
