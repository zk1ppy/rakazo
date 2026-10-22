import { DEFAULT_MODEL_MAX_TOKENS } from "@rakazo/contracts";

/** Hung completions must fail before the typical 5-minute run lease. */
export const MODEL_STREAM_TIMEOUT_MS = 120_000;
/** Allow slow DNS/TLS handshakes without consuming the entire model request budget. */
export const MODEL_CONNECT_TIMEOUT_MS = 30_000;
/**
 * `timeoutMs` bounds only time-to-headers; the SSE body that follows is
 * unbounded. Bound silence for every provider so a stalled stream can fail
 * and advance the configured fallback chain.
 */
export const MODEL_STREAM_IDLE_TIMEOUT_MS = 180_000;
export const DEFAULT_MODEL_STREAM_MAX_RETRIES = 1;

/** More retries extend the worst-case turn; the executor heartbeat renews its leases. */
export function modelStreamMaxRetries(): number {
  const value = process.env.MODEL_STREAM_MAX_RETRIES?.trim();
  const raw = Number(value || DEFAULT_MODEL_STREAM_MAX_RETRIES);
  return Number.isInteger(raw) && raw >= 0 && raw <= 5 ? raw : DEFAULT_MODEL_STREAM_MAX_RETRIES;
}

export const TOOL_RESULT_TEXT_LIMIT = 12_000;

export function clipToolResultText(text: string, limit: number = TOOL_RESULT_TEXT_LIMIT): string {
  return text.length > limit ? `${text.slice(0, limit)}…` : text;
}

/**
 * Share one remaining character budget across all text parts.
 * Later text is omitted once the aggregate limit is exhausted; non-text parts stay.
 */
export function clipToolResultContent<T>(
  content: T[],
  limit: number = TOOL_RESULT_TEXT_LIMIT,
): T[] {
  let remaining = limit;
  const clipped: T[] = [];
  for (const part of content) {
    if (
      !part ||
      typeof part !== "object" ||
      !("type" in part) ||
      (part as { type?: unknown }).type !== "text" ||
      !("text" in part)
    ) {
      clipped.push(part);
      continue;
    }
    if (remaining <= 0) continue;
    const text = String((part as { text: unknown }).text);
    if (text.length <= remaining) {
      clipped.push(part);
      remaining -= text.length;
      continue;
    }
    clipped.push({ ...part, text: clipToolResultText(text, remaining) });
    remaining = 0;
  }
  return clipped;
}

/**
 * Prompt tokens providers bill, including cache read/write rather than the uncached remainder.
 * The cache halves are reported alongside so downstream views can show what a cache hit saved.
 */
export function billedPromptTokens(usage: {
  input?: number;
  output?: number;
  cacheRead?: number;
  cacheWrite?: number;
}): {
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWriteTokens: number;
} {
  const cacheReadTokens = nonNegativeCount(usage.cacheRead);
  const cacheWriteTokens = nonNegativeCount(usage.cacheWrite);
  return {
    inputTokens: nonNegativeCount(usage.input) + cacheReadTokens + cacheWriteTokens,
    outputTokens: nonNegativeCount(usage.output),
    cacheReadTokens,
    cacheWriteTokens,
  };
}

/**
 * A reasoning model spends this same budget on its thinking, so the modest default
 * can be consumed before the reply starts. Wide enough for thinking plus an answer,
 * still far below a model card's 128k ceiling.
 */
export const REASONING_MODEL_MAX_TOKENS = 32_768;

/** Visible JSON budget for one memory.extract chunk. */
export const EXTRACT_ANSWER_TOKENS = 1_024;

const EXTRACTION_EFFORTS = ["minimal", "low", "medium", "high", "xhigh", "max"] as const;
type ExtractionEffort = (typeof EXTRACTION_EFFORTS)[number];

/** Same default chat uses when a reasoning connection leaves effort unset. */
const DEFAULT_EXTRACTION_EFFORT: ExtractionEffort = "medium";

function storedEffort(value: string | null | undefined): ExtractionEffort | "off" | undefined {
  if (!value) return undefined;
  if (value === "off") return "off";
  if ((EXTRACTION_EFFORTS as readonly string[]).includes(value)) return value as ExtractionEffort;
  return undefined;
}

/**
 * Effort extraction should request. A stored minimum is the effort used for
 * extraction, ahead of the chat effort. Otherwise an explicit effort is used
 * as set. Leaving effort unset on a reasoning connection is the default
 * effort, not off: off is only an explicit "off" or a connection that does
 * not support thinking.
 */
export function extractionThinkingLevel(model: {
  reasoning?: boolean;
  thinkingLevel?: string | null;
  minReasoningEffort?: string | null;
}): "off" | ExtractionEffort {
  if (!model.reasoning) return "off";
  const floor = storedEffort(model.minReasoningEffort);
  if (floor && floor !== "off") return floor;
  const requested = storedEffort(model.thinkingLevel);
  if (requested === "off") return "off";
  return requested ?? DEFAULT_EXTRACTION_EFFORT;
}

/**
 * These APIs bill thinking beside the answer (Anthropic and Bedrock add a
 * budget on top of maxTokens; Google uses a separate thinking budget). The
 * others, including OpenAI-compatible providers, count thinking inside the
 * same completion cap.
 */
const SEPARATED_THINKING_APIS = new Set([
  "anthropic-messages",
  "bedrock-converse-stream",
  "google-generative-ai",
  "google-vertex",
]);

/**
 * Size one extraction call. A separated provider bills thinking beside the
 * answer, and a connection that can disable thinking sends effort off, so
 * both keep the visible budget. A requested effort means thinking cannot be
 * disabled and shares this cap. An effort allowance is not a reliable budget
 * for that case, so the call uses the model's max output — the connection's
 * own cap when one is stored — and never asks for more than that.
 */
/**
 * Cap one background task that brings its own budget. When thinking is on,
 * `maxOutputTokens` is the whole shared completion cap, so it has to leave room
 * for reasoning and the answer. Thinking that is off stays within the visible
 * answer. The connection max, when set, is still a ceiling.
 */
export function taskCompletionTokens(
  model: { maxTokens?: number },
  thinkingLevel: string,
  maxOutputTokens: number,
  answerTokens: number,
): number {
  const answer = Math.max(1, Math.floor(answerTokens));
  const requested = Math.max(1, Math.floor(maxOutputTokens));
  const budget =
    !thinkingLevel || thinkingLevel === "off" ? Math.min(requested, answer) : requested;
  const ceiling =
    typeof model.maxTokens === "number" && model.maxTokens >= 1 ? model.maxTokens : undefined;
  return ceiling === undefined ? budget : Math.min(ceiling, budget);
}

export function extractionCompletionTokens(
  model: { api?: string; reasoning?: boolean; maxTokens?: number },
  thinkingLevel: string,
  answerTokens: number = EXTRACT_ANSWER_TOKENS,
): number {
  const answer = Math.max(1, Math.floor(answerTokens));
  const ceiling =
    typeof model.maxTokens === "number" && model.maxTokens >= 1 ? model.maxTokens : undefined;
  const cap = (total: number) => (ceiling === undefined ? total : Math.min(ceiling, total));
  const separated = model.api !== undefined && SEPARATED_THINKING_APIS.has(model.api);
  const thinkingSharesCap =
    Boolean(model.reasoning) && !separated && Boolean(thinkingLevel) && thinkingLevel !== "off";
  if (!thinkingSharesCap) return cap(answer);
  return ceiling ?? REASONING_MODEL_MAX_TOKENS;
}

/** Leave at least three quarters of the window for input; token limits are positive integers. */
export function effectiveOutputReserve(contextWindow: number, maxTokens: number): number {
  return Math.min(maxTokens, Math.max(1, Math.floor(contextWindow / 4)));
}

/**
 * Completions default to a modest output cap so OpenRouter-style providers do not
 * hold credit for a model card's 128k ceiling. A configured maxTokens overrides this
 * default, still bounded to a quarter of the context window when it is supplied.
 * Reasoning models get the wider default because their thinking is billed against
 * the same ceiling: at 4k a hard question can leave no room for the reply at all.
 */
export function resolveCompletionMaxTokens(
  modelMaxTokens?: number,
  configuredMaxTokens?: number,
  optionsMaxTokens?: number,
  reasoning?: boolean,
  contextWindow?: number,
): number {
  const userCap =
    typeof configuredMaxTokens === "number" && configuredMaxTokens >= 1
      ? configuredMaxTokens
      : reasoning
        ? REASONING_MODEL_MAX_TOKENS
        : DEFAULT_MODEL_MAX_TOKENS;
  const optionCap =
    typeof optionsMaxTokens === "number" && optionsMaxTokens >= 1
      ? Math.min(optionsMaxTokens, userCap)
      : userCap;
  const maxTokens =
    typeof modelMaxTokens === "number" && modelMaxTokens >= 1
      ? Math.min(modelMaxTokens, optionCap)
      : optionCap;
  return contextWindow === undefined ? maxTokens : effectiveOutputReserve(contextWindow, maxTokens);
}

function nonNegativeCount(value: number | undefined): number {
  return typeof value === "number" && Number.isFinite(value) ? Math.max(0, value) : 0;
}
