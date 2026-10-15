# Codex

Use GPT through your ChatGPT subscription and custom models through Responses-compatible upstreams. Both appear in the model picker.

## Setup

1. Adapt [router-config.toml](../examples/router-config.toml) and the [model catalog](../examples/codex-model-catalog.json) to your upstream and model. The supplied example connects ChatGPT and Qwen served by SGLang at `http://localhost:30000/v1`.
2. [Start the router](../README.md#run).
3. Merge the following into `~/.codex/config.toml`. Put `model_provider` before any tables and merge existing tables rather than duplicating them.

```toml
model_provider = "router"

[agents]
default_subagent_model = "vendor/model"

[features]
enable_request_compression = false

[model_providers.router]
name = "OpenAI" # Enables Codex's WebSocket routing hint
base_url = "http://localhost:8080"
model_catalog_url = "http://localhost:8080/models"
requires_openai_auth = true
supports_websockets = true
http_headers = { "x-router-client" = "codex" }
```

Replace `vendor/model` with the exact model ID in your catalog. Keep your ChatGPT login, or run `codex login`; no OpenAI API key is needed. Start `codex` and use `/model` to select the main model. Workers default to the configured model unless an explicit model or agent-role setting overrides it.

## SGLang compatibility

Tested with Codex 0.162.1 and the example Qwen deployment:

- Keep request compression disabled as shown above. The client header selects v1 catalog settings for both parent and worker models; remove any configuration or launch option that forces v2.
- Start a new Codex session after switching to v1. Restart the router and Codex after configuration or catalog edits; rebuild the router after code changes.

## Qwen context handoffs

The example Qwen catalog enables Codex 0.162.1's model-owned `model_messages.token_budget` mode. Fresh Qwen workers are instructed to return a final handoff when their working context budget is exhausted. The parent agent or user decides whether to start a fresh worker with that summary. GPT keeps its provider's remote compaction. Leave `features.token_budget` unset in client configuration, since an explicit setting overrides model-owned activation. Spawn Qwen workers without inherited history (`fork_turns = "none"`); full-history forks retain the parent's activation.

The reminder and fallback adapt Codex's token-budget prompts from the cached GPT model catalog. Both preserve the context-window reminder wrapper and request the goal, decisions, progress, learnings, next steps, unresolved requests, and important actions/tool calls. The reminder reports the remaining budget; the fallback declares exhaustion, says not to continue the task, requests the handoff now, and prohibits tool calls. Notes storage, window/item references, and reset instructions are replaced with returning the summary as a final response that explains the context limit. Hosted Notes/History integration is disabled. `guidance_message` only asks a worker that has unexpectedly been reset to report the reset and request restoration of its task context.

For the supplied 262,144-token model with Codex's default context settings:

| Event | Active context tokens |
| --- | --- |
| Reminder (`reminder_threshold_tokens = 6144`) | 229,785, approximately 87.7% |
| Fallback prompt | 235,929, the 90% compaction threshold |
| Forced-reset boundary with a 16,384-token fallback buffer | 249,036, capped by Codex's 95% usable-context ceiling |
| Advertised model context limit | 262,144 |

`reminder_threshold_tokens` is a remaining-token trigger, not a context limit: the reminder is due when at most that many tokens remain before the compaction threshold. Codex 0.162.1 requires a positive value; `0` is invalid. The 6,144-token reminder (6 × 1,024) and 16,384-token fallback buffer (16 × 1,024) match the fixed values in the cached GPT model metadata. The client copies these values rather than deriving them from context size; no rationale for their selection was found in the inspected code. For Qwen, the reminder is due at `235929 - 6144 = 229785` tokens. The nominal buffered boundary is `235929 + 16384 = 252313`, but the 95% ceiling caps it at 249,036, allowing 13,107 tokens after the 90% threshold.

The buffer reserves space for the handoff after the normal compaction threshold. It cannot move the forced-reset boundary beyond the usable-context ceiling. Thresholds are checked between model steps, so a large response or tool result can cross more than one boundary. A normal final response ends the worker turn; token-budget mode skips post-turn summarization. Reusing an exhausted worker can trigger a pre-turn reset, so continue in a fresh worker.

This is a prompt-directed handoff, not an enforced stop mode: Codex can still reset a worker that keeps running past the boundary. The post-reset instruction limits the intended behavior but cannot guarantee compliance. A guaranteed stop before reset requires client-side enforcement.

## Text compaction

Codex 0.162.1 can summarize context through an ordinary model request, without a router compaction endpoint. To use this instead of the handoff mode, merge these settings into the client configuration and start a new session:

```toml
[features.token_budget]
enabled = false

[model_providers.router.capabilities]
remote_compaction = "unsupported"
```

This selects text compaction for **every model using the router provider**, including GPT. Leave these settings unset to keep the example's Qwen handoffs and GPT compaction behavior. Summaries are lossy; they do not reproduce OpenAI's encrypted compaction state. See the [feature plan](roadmap.md#context-compaction) for the per-provider fallback work.

## WebSocket routing

With WebSockets, Codex uses `name = "OpenAI"` and ChatGPT sign-in to supply the model routing hint.

In the test, GPT can use WebSockets while Qwen falls back to HTTP after connection retries because SGLang doesn't support WebSocket. Set `supports_websockets = false` to avoid those retries and use HTTP for both. Reconnect when changing providers.

[Codex configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference)
