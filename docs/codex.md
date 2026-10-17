# Codex

Use GPT through your ChatGPT subscription and custom models through Responses-compatible upstreams. Both appear in the model picker.

## Setup

1. Adapt [router-config.toml](../examples/router-config.toml) to your upstream and model. The supplied example discovers Qwen models from SGLang's OpenAI-compatible `http://localhost:30000/v1/models` endpoint and inherits a full model entry from the official ChatGPT catalog.
2. [Start the router](../README.md#run).
3. Set this top-level option in `~/.codex/config.toml`:

```toml
openai_base_url = "http://localhost:4134"
```

Keep your ChatGPT login, or run `codex login`; no OpenAI API key is needed. When migrating, remove `model_provider = "router"` and its old custom provider table so Codex uses the built-in OpenAI provider. Select models with `/model`; worker defaults remain optional agent settings. This one-line setup uses ChatGPT authentication. API-key catalog discovery still needs an explicit catalog URL in Codex.

## Multi-agent v2 compatibility

Client detection checks whether the `originator` value contains `codex`, ignoring ASCII case. Other values remain unadapted.

For HTTP and WebSocket Model routes identified by recognized Codex `originator` values, the router adapts Codex multi-agent v2 JSON messages for compatible Responses upstreams. It renames the `collaboration` tool namespace to `collaboration_compat` upstream, removes `message.encrypted` from `spawn_agent`, `send_message`, and `followup_task` schemas, then restores the original tool identities in returned calls. It maps collaboration tool identities in replayed input as well. Plaintext `agent_message` content is relabeled as `input_text`, including on marked requests whose upstream preserves native `agent_message` items. This does not decrypt `encrypted_content` or rewrite nested code-mode calls. Payloads that need no JSON edit retain their bytes. The WebSocket adaptation handles JSON messages only.

Codex SSE streams use LF framing and forward comments promptly. Rewritten JSON data removes literal CR/LF whitespace while preserving strings and escaped newlines.

When an actual input contains `agent_message`, the router validates support with a POST to the same provider, path, and model. If an HTTP request omits `model`, the probe omits it too so the provider chooses its default. The probe appends an invalid trailing sentinel and uses `max_output_tokens: 1` and `store: false`. Only an explicit HTTP 400 or 422 reporting unsupported `agent_message` causes conversion to an ordinary user-message envelope; other outcomes preserve native `agent_message` items. Validation runs when needed and is not cached. Mentions in text or unrelated nested fields do not trigger a probe.

Subagent model listings use non-hidden entries from the latest successful catalog snapshots. Retired IDs remain routable but are not advertised. Before remote discovery succeeds, the client's cached model list is preserved; file-only catalogs are available at startup. A successful empty catalog clears the old choices, while a failed refresh preserves the previous listing.

For Codex WebSocket requests, the handshake routing hint selects the initial provider. Every subsequent model request is resolved again, so a socket prewarmed for GPT can safely serve a custom model. Supported upstreams use WebSocket; an explicit upgrade refusal selects HTTP for that request while the client remains connected by WebSocket. The adapted handshake does not negotiate `permessage-deflate`; generic raw tunnels retain their negotiated extensions.

The catalog retains the inherited `multi_agent_version`. Remove earlier v1 pins or configuration that disables Codex's multi-agent v2 feature.

The isolated experiment observed correct initial task results in 4/4 runs for the full transformation. Same-child lifecycle replies completed in 2/3 runs; one delivered both messages but omitted the requested result. These results do not validate the router implementation or establish reliability. See [the experiment notes](../experiments/multiagent-v2/README.txt).

The Rust adapter is covered by mock parent/worker exchanges, compressed JSON and streamed-event tests, and a live Qwen message-conversion check. A fresh GPT-to-Qwen delegation run through this adapter has not been repeated. Relabeling cannot decrypt ciphertext saved by an earlier session.

## Model metadata

The example puts `name`, `description`, and `reasoning_levels` under shared provider model settings. The running SGLang `/models` response does not advertise reasoning choices, so the list remains explicit. Optional `reasoning_level` replaces the inherited default; when omitted, the referenced model's default is kept. The Codex adapter translates these shared names to Codex's catalog fields. Metadata values are not semantically validated.

The example's `extends` reference copies the selected ChatGPT catalog row in full, including its instructions and native Codex settings, then applies local shared settings and discovered SGLang context metadata. Its display name remains the local model's configured name. An omitted name falls back to the local native model ID. Catalog references resolve directly against the current refresh: the source provider and exact native model ID must be present, inherited entries are not followed recursively, and hidden donor rows remain usable. A missing donor or invalid reference makes the refresh fail while preserving the previous routes. Generic native-field overrides are deferred; discovered capabilities can adjust selected fields, and the first GPT model identity in inherited instructions is rewritten for the local model.

Output in another format can inherit shared donor metadata while preserving local source fields. Full catalog copying preserves arbitrary fields and prompt subtrees. GPT token-budget behavior has History/Notes dependencies and should not be assumed portable from normal compaction support alone. Keep `[features.token_budget] enabled = false` for Remote V2; History and Notes are separate features.

On each catalog request, the SGLang example queries model info, server info, and OpenAPI; it also fetches any needed advertised tool-list endpoints as part of that refresh. Its image-detail enum excludes `original`, so discovery sets `supports_image_detail_original` to false. Native tool-search remains inherited when no reliable declaration is available; a `tool_search` schema enum is not proof of runtime support. Other unknown capabilities also inherit from Luna. When the projected Codex row enables Responses Lite and discovery has not declared support, the router optionally POSTs to `base_url + /responses` with `input` containing `[{"type":"additional_tools","role":"developer","tools":[]},{"type":"__model_router_capability_check__"}]`, `max_output_tokens: 1`, and `store: false`. Only an HTTP 400 or 422 whose error message begins `Unsupported Responses API input item type: 'additional_tools'` records unsupported; every other outcome remains unknown. The sentinel is intended to trigger validation, though a loosely validating server could generate up to one token. Discovery calls time out after three seconds. Queries use provider credentials only on the provider's origin; explicitly configured external URLs are queried anonymously.

## Remote V2 compaction

Codex Remote V2 compaction is detected only when a normal Responses request contains an input item whose type is `compaction_trigger`. The router does not probe ordinary requests, checkpoints, or text mentions, and `/responses/compact` support does not establish trigger support. No provider configuration is required. For each trigger, it sends a probe for that item to the current model's `base_url + /responses` endpoint through shared HTTP/WebSocket handling, followed by a deliberately invalid trailing sentinel. The probe uses `max_output_tokens: 1` and `store: false`. Only an HTTP 400 or 422 whose error message begins `Unsupported Responses API input item type: 'compaction_trigger'` selects synthetic text compaction. Every other result leaves support unknown and delegates natively. There is no support cache, so every trigger is checked.

Native requests and genuine checkpoints pass through unchanged. If the provider explicitly rejects the trigger, the router sends the full input history, with tools disabled, to the same provider's ordinary HTTP Responses endpoint using the fetched Codex compaction prompt unchanged. Only a completed, nonempty text summary with no tools, refusal, or error becomes a synthetic checkpoint. No summary is stored separately or encoded.

HTTP JSON and SSE are supported; disconnecting during SSE cancels summary generation. Codex WebSocket connections remain WebSockets, but summary generation uses HTTP to the same provider, which must expose `POST /responses`. A request using `previous_response_id` cannot supply enough history for text compaction: Codex must retry with full history. The router also asks Codex for a full-history retry when a local synthetic response ID is referenced.

On later requests, the router hydrates its own synthetic checkpoint into a text message. Existing router-owned checkpoint hydration is preserved. Unknown opaque checkpoints on ordinary inference requests pass through to the upstream without requiring a compaction setting; if one is present in a trigger request that must be summarized, the router returns an incompatibility error rather than dropping it. Reasoning with visible content or summary text is preserved. Text summaries are lossy. Codex still chooses retained history and owns history persistence. On an explicit context-length HTTP 400 error, the router retries after removing the oldest safe, complete non-instruction history group while preserving paired calls/results and the latest user segment.

The prompt and summary-prefix templates plus their license are fetched at build time from pinned Codex commit [`092d3acd6bec3e3a14bdc7e7a2810ab628ab759d`](https://github.com/openai/codex/tree/092d3acd6bec3e3a14bdc7e7a2810ab628ab759d) into Cargo's `OUT_DIR`. A clean first build needs network access and `curl`; incremental builds reuse the files, while `cargo --offline` only governs Cargo dependencies and cannot supply a missing prompt cache. Live Codex 0.163.0 and Qwen completed two automatically detected compactions without a compaction setting and recalled the supplied facts after each continuation. WebSocket behavior, failure handling, and overflow recovery are covered by local integration tests. See the [implementation notes](roadmap.md#context-compaction-native-and-synthetic-codex-v2-compaction).

## WebSocket routing

With WebSockets, Codex uses `name = "OpenAI"` and ChatGPT sign-in to supply the model routing hint.

No provider WebSocket flag is required. The router attempts the real upstream handshake and falls back on a successful non-upgrade response or HTTP 404, 405, 426, or 501. Authentication, rate-limit, and server errors remain errors. The original working upstream socket remains open; requests routed elsewhere use a separate upstream WebSocket when available, or streamed HTTP otherwise. Connection-local `previous_response_id` values require a full-history replay when changing upstream connections. Codex handles that retry automatically.

[Codex configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference)
