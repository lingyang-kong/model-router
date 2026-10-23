# Codex

Use GPT through a ChatGPT subscription and custom models through Responses-compatible upstreams, with both available in the model picker.

## Setup

1. Adapt [router-config.toml](../examples/router-config.toml) to your upstream and model, then [start the router](../README.md#run).
2. Set this top-level option in `~/.codex/config.toml`:

   ```toml
   openai_base_url = "http://localhost:4134"
   ```

3. Keep your ChatGPT login, or run `codex login`, and select models with `/model`.

This setup uses Codex's built-in OpenAI provider and ChatGPT authentication. Remove a custom `model_provider` selection if it would bypass that setup. API-key catalog discovery requires an explicit catalog URL in Codex.

For other client options, see the [Codex configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference).

The router selects Codex handling when the `originator` header contains `codex`, ignoring ASCII case. It adapts HTTP and WebSocket `model` routes; other clients and `forward` routes remain unadapted. No router-specific client header is needed. `[clients.codex]` associates Codex with its provider and catalog format; the example uses ChatGPT and `codex`. The same catalog route serves OpenAI and Anthropic clients through their own profiles. The ChatGPT catalog source declares its source format independently.

## Model metadata

Use [catalog inheritance](../README.md#metadata-and-inheritance) to supply a custom model with a native Codex catalog entry. The example inherits a ChatGPT row, including instructions and native settings, then applies local shared settings and discovered capabilities.

Identity rewriting replaces the first `GPT-…` token in inherited `base_instructions` and `model_messages.instructions_template` with the local display name or ID. Explicit local instruction fields in a Codex-format source are preserved. This is a targeted template rewrite, not a rewrite of every donor reference in arbitrary metadata.

| Discovery or setting | Codex projection |
| --- | --- |
| OpenAI-source `max_model_len` | Sets `max_context_window` and removes inherited `context_window`. |
| Closed Responses `service_tier` enum | Replaces inherited tier choices with the provider's IDs; `auto` and `default` are implicit. Labels carry no inherited pricing or speed claims. |
| `service_tiers = false` | Removes tier choices and defaults. |
| `access_programs = false` | Emits `available_access_programs = {"cyber": []}`; omission would mean unknown. |
| `history_notes` | Controls `supports_experimental_context`, independently of compaction. |
| `responses_lite`, `tool_search` | Control `use_responses_lite` and `supports_search_tool`, subject to the probes below when unknown. |
| `images`, `image_detail_original` | Refine image input modalities and original-detail support. |
| Responses `reasoning.summary` and `text.verbosity` enums/defaults | Refine support and defaults that Codex can express. Reasoning effort choices still come from shared settings or the donor. |

Schema deductions use relevant Responses request fields and supported references/compositions. Open schemas and missing fields do not prove lack of support. A tool-type enum can refine hosted web-search metadata, but `web_search = false` cannot disable Codex's standalone search tool: its catalog field has no disabled value, and omission means text search.

The adapter omits values equivalent to known Codex defaults, including false Lite/search/original-image/experimental-context flags, empty tier lists, and reasoning-summary `true`/`auto`. Explicit empty access-program lists are retained. Verbosity support and reasoning choices remain explicit; Codex has no field for a restricted list of verbosity choices. See `apply_capabilities` and `prune_defaults` in [the adapter](../src/adapters/codex.rs) when changing these mappings.

Unknown capabilities continue to inherit. Catalog format alone does not establish provider identity or compatibility. Token-budget behavior depends on History/Notes, not just compaction. For providers without that protocol, keep `[features.token_budget]` with `enabled = false` in Codex configuration.

## Capability probes

Some compatibility decisions use a POST to the selected provider's Responses endpoint:

| Tested input item | When tested | Effect of explicit rejection |
| --- | --- | --- |
| `additional_tools` | Catalog projection enables Responses Lite and its capability is unknown. | Disable Lite in the projected row. |
| `tool_search_output` | Catalog projection enables tool search and its capability is unknown. | Disable search support in the projected row. |
| `agent_message` | An inference request contains an actual item of that type. | Convert it to an ordinary user-message envelope. |
| `compaction_trigger` | A request contains an actual trigger item. | Generate a synthetic text summary. |

Each probe appends an invalid sentinel, uses `max_output_tokens: 1` and `store: false`, and has a three-second timeout. Only HTTP 400/422 with an error message beginning `Unsupported Responses API input item type: '<tested type>'` establishes rejection. Other outcomes remain unknown and retain inherited or native behavior. A loosely validating provider could generate up to one token. Results are not cached; catalog probes repeat on refresh and request probes repeat on applicable inference requests.

Catalog probes use `base_url + /responses`; request probes use the incoming path and effective model, omitting the model when HTTP leaves it to the provider. Mentions in text do not trigger request probes. A schema tool declaration does not prove acceptance of a separate input item. The current parser nevertheless treats a native inventory entry of type `tool_search` as a known positive capability, which skips that probe.

## Multi-agent v2 compatibility

The adapter renames the `collaboration` namespace to `collaboration_compat` upstream, removes `message.encrypted` from the `spawn_agent`, `send_message`, and `followup_task` schemas, maps replayed tool identities, and restores identities in returned calls. Agent-message content parts tagged `encrypted_content` are relabeled as `input_text` without decryption; genuine ciphertext remains unreadable. The outer envelope changes only after the rejection described above. Nested code-mode calls are not rewritten.

Restored collaboration message calls receive `encrypted_function_args: []` if that metadata is absent. Codex uses an explicit empty list for plaintext delivery and persistence; absence means encrypted handling. Preserve this distinction and any existing encryption metadata. The adapter does not rewrite saved history files, and cross-provider history portability is not guaranteed.

Subagent guidance lists non-hidden, currently listed source IDs from the routing catalog. Before remote discovery is ready it leaves the client's existing listing intact; a successful empty refresh clears the choices. Guidance currently reads source descriptions and reasoning levels, so donor-only inherited fields shown in the picker may be missing from it. IDs can also be listed even when they cannot be projected into the Codex picker.

Payloads needing no edit retain their bytes. The implementation opens only relevant JSON envelopes with `RawValue` to preserve opaque schema and payload depth. Its byte prefilter admits escaped names for correctness. Adapted SSE uses LF framing, forwards comments promptly, and removes literal CR/LF whitespace from rewritten JSON data without changing string contents; unmarked SSE passes through unchanged.

## Remote V2 compaction

Compaction is detected through `compaction_trigger` in normal Responses input; `/responses/compact` support does not establish trigger support. Providers without an explicit probe rejection receive native compaction requests. No compaction setting is required.

For synthetic compaction, the router sends full input history to the same provider over HTTP with tools disabled and the pinned Codex compaction prompt. Only a completed, nonempty text summary without tool calls, refusal, or errors becomes a checkpoint. The summary is stored as plaintext inside the router-marked checkpoint; there is no separate router-side history store. Prompt provenance and fetching are defined in [build.rs](../build.rs).

On continuation, the router hydrates its own checkpoints into text messages. Unknown opaque checkpoints pass through ordinary inference, but synthetic summarization rejects unknown checkpoints or unreadable encrypted reasoning rather than silently dropping them. Router checkpoints require the router for hydration and are not portable directly to OpenAI. Text summaries are lossy; Codex owns retained history and persistence.

Full history is required for synthetic compaction. Requests relying on `previous_response_id` may require a full-history retry. On explicit context-length HTTP 400 errors, the router retries after removing the oldest removable non-instruction item or call/result group, preserving paired calls/results and the latest user segment. This can omit earlier information.

HTTP JSON, SSE, and WebSocket replies are supported. Summary generation always uses HTTP, including for WebSocket clients, so the provider must expose the corresponding Responses HTTP endpoint. Disconnecting during SSE or WebSocket summary generation cancels the work.

## WebSocket routing

Codex's native routing hint selects the initial provider; absent a hint, `[clients.codex].provider` is used. The router requires a real upstream handshake before accepting the client socket, including prewarm connections. Successful non-upgrade responses and HTTP 404/405/426/501 become HTTP 426 so the client can fall back to HTTP. Authentication, rate-limit, and server errors remain errors.

Every subsequent model request is routed again. The initial upstream connection is reused while available; requests to another provider open a separate connection for that response. Those requests may require full-history replay because connection-local `previous_response_id` state is unavailable. A rejected alternate-provider upgrade emits an error and closes the client socket.

There is no WebSocket-to-HTTP bridge for ordinary inference, so seamless switching to an HTTP-only provider is not guaranteed. Adapted handshakes disable `permessage-deflate`; request-body compression can remain enabled. Generic raw WebSocket routes retain negotiated extensions.
