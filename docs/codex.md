# Codex

Use GPT through your ChatGPT subscription and custom models through Responses-compatible upstreams. Both appear in the model picker.

## Setup

1. Adapt [router-config.toml](../examples/router-config.toml) to your upstream and model. The supplied example discovers Qwen models from SGLang's OpenAI-compatible `http://localhost:30000/v1/models` endpoint and inherits a full model entry from the official ChatGPT catalog.
2. [Start the router](../README.md#run).
3. Merge the following into `~/.codex/config.toml`. Put `model_provider` before any tables and merge existing tables rather than duplicating them.

```toml
model_provider = "router"

[agents]
default_subagent_model = "vendor/model"

[model_providers.router]
name = "OpenAI"
base_url = "http://localhost:8080"
model_catalog_url = "http://localhost:8080/models"
requires_openai_auth = true
supports_websockets = true
http_headers = { "x-router-client" = "codex" }
```

Replace `vendor/model` with the exact model ID in your catalog. Keep your ChatGPT login, or run `codex login`; no OpenAI API key is needed. Start `codex` and use `/model` to select the main model. Workers default to the configured model unless an explicit model or agent-role setting overrides it.

## Multi-agent v2 compatibility

For HTTP and WebSocket Model routes carrying `x-router-client: codex`, the router adapts Codex multi-agent v2 JSON messages for compatible Responses upstreams. It renames the `collaboration` tool namespace to `collaboration_compat` upstream, removes `message.encrypted` from `spawn_agent`, `send_message`, and `followup_task` schemas, then restores the original tool identities in returned calls. It maps collaboration tool identities in replayed input as well. Plaintext `agent_message` content is relabeled as `input_text`, including on marked requests whose upstream preserves native `agent_message` items. This does not decrypt `encrypted_content` or rewrite nested code-mode calls. Payloads that need no JSON edit retain their bytes. The WebSocket adaptation handles JSON messages only.

When an actual input contains `agent_message`, the router validates support with a POST to the same provider, path, and model. If an HTTP request omits `model`, the probe omits it too so the provider chooses its default. The probe appends an invalid trailing sentinel and uses `max_output_tokens: 1` and `store: false`. Only an explicit HTTP 400 or 422 reporting unsupported `agent_message` causes conversion to an ordinary user-message envelope; other outcomes preserve native `agent_message` items. Validation runs when needed and is not cached. Mentions in text or unrelated nested fields do not trigger a probe.

Subagent model listings use non-hidden entries from the latest successful catalog snapshots. Retired IDs remain routable but are not advertised. Before remote discovery succeeds, the client's cached model list is preserved; file-only catalogs are available at startup. A successful empty catalog clears the old choices, while a failed refresh preserves the previous listing.

For WebSocket Model routes, the router reads `x-codex-routing-hint` during the handshake, selects the corresponding upstream provider, and keeps it fixed for that connection. The adapted handshake does not negotiate `permessage-deflate`, which the WebSocket library does not support; generic raw tunnels preserve negotiated extensions. Compression may remain enabled for HTTP requests; the adapter decodes and rewrites JSON when needed. Start a fresh Codex session after enabling this compatibility.

The catalog retains the inherited `multi_agent_version`. Remove earlier v1 pins or configuration that disables Codex's multi-agent v2 feature.

The isolated experiment observed correct initial task results in 4/4 runs for the full transformation. Same-child lifecycle replies completed in 2/3 runs; one delivered both messages but omitted the requested result. These results do not validate the router implementation or establish reliability. See [the experiment notes](../experiments/multiagent-v2/README.txt).

The Rust adapter is covered by mock parent/worker exchanges, compressed JSON and streamed-event tests, and a live Qwen message-conversion check. A fresh GPT-to-Qwen delegation run through this adapter has not been repeated. Relabeling cannot decrypt ciphertext saved by an earlier session.

## Model metadata

The example puts `name`, `description`, and `reasoning_levels` under shared provider model settings. The running SGLang `/models` response does not advertise reasoning choices, so the list remains explicit. Optional `reasoning_level` replaces the inherited default; when omitted, the referenced model's default is kept. The Codex adapter translates these shared names to Codex's catalog fields. Metadata values are not semantically validated.

The example's `extends` reference copies the selected ChatGPT catalog row in full, including its instructions and native Codex settings, then applies local shared settings and discovered SGLang context metadata. Its display name remains the local model's configured name. An omitted name falls back to the local native model ID. Catalog references resolve directly against the current refresh: the source provider and exact native model ID must be present, inherited entries are not followed recursively, and hidden donor rows remain usable. A missing donor or invalid reference makes the refresh fail while preserving the previous routes. Generic native-field overrides are deferred; discovered capabilities can adjust selected fields, and the first GPT model identity in inherited instructions is rewritten for the local model.

Output in another format can inherit shared donor metadata while preserving local source fields. Full catalog copying preserves arbitrary fields and prompt subtrees. GPT token-budget behavior has History/Notes dependencies and should not be assumed portable from normal compaction support alone.

On each catalog request, the SGLang example queries model info, server info, and OpenAPI; it also fetches any needed advertised tool-list endpoints as part of that refresh. Its image-detail enum excludes `original`, so discovery sets `supports_image_detail_original` to false. Native tool-search remains inherited when no reliable declaration is available; a `tool_search` schema enum is not proof of runtime support. Other unknown capabilities also inherit from Luna. When the projected Codex row enables Responses Lite and discovery has not declared support, the router probes `base_url + /responses` using `input` items `[{"type":"additional_tools","role":"developer","tools":[]},{"type":"__model_router_capability_check__"}]`, `max_output_tokens: 1`, and `store: false`. Only an HTTP 400 or 422 with an error message beginning `Unsupported Responses API input item type: 'additional_tools'` marks it unsupported; all other outcomes remain unknown. The sentinel aims to trigger validation, though a loosely validating server could generate up to one token. Optional discovery calls time out after three seconds. A fresh negative tool-inventory declaration suppresses tool-list queries for that request.

## Text compaction

The router does not emulate remote compaction yet. Codex 0.162.1 can instead summarize context through an ordinary model request. To use that option, merge these settings into the client configuration and start a new session:

```toml
[features.token_budget]
enabled = false

[model_providers.router.capabilities]
remote_compaction = "unsupported"
```

This selects text compaction for **every model using the router provider**, including GPT. Without it, the setup requests remote compaction, which the tested SGLang endpoint does not provide; Qwen can fail when its context fills. Summaries are lossy and do not reproduce OpenAI's encrypted compaction state. See the [feature plan](roadmap.md#context-compaction) for the per-provider fallback work.

## WebSocket routing

With WebSockets, Codex uses `name = "OpenAI"` and ChatGPT sign-in to supply the model routing hint.

Keep `name = "OpenAI"` and ChatGPT sign-in so Codex supplies its routing hint. If the selected upstream rejects WebSockets, Codex may retry and fall back to HTTP; the previously tested SGLang endpoint rejected seven handshakes before that fallback. This is an endpoint limitation, and the router leaves the upstream handshake result intact. A provider is selected once per WebSocket connection, so reconnect after changing providers.

[Codex configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference)
