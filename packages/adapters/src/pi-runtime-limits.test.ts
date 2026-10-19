import { DEFAULT_MODEL_MAX_TOKENS } from "@rakazo/contracts";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  billedPromptTokens,
  clipToolResultContent,
  clipToolResultText,
  EXTRACT_ANSWER_TOKENS,
  extractionCompletionTokens,
  extractionThinkingLevel,
  modelStreamMaxRetries,
  REASONING_MODEL_MAX_TOKENS,
  resolveCompletionMaxTokens,
  TOOL_RESULT_TEXT_LIMIT,
  taskCompletionTokens,
} from "./pi-runtime-limits.js";

describe("modelStreamMaxRetries", () => {
  afterEach(() => vi.unstubAllEnvs());

  it("defaults to one retry when unset", () => {
    vi.stubEnv("MODEL_STREAM_MAX_RETRIES", undefined);
    expect(modelStreamMaxRetries()).toBe(1);
  });

  it.each(["0", "1", "2", "3", "4", "5", " 3 "])("accepts %j", (value) => {
    vi.stubEnv("MODEL_STREAM_MAX_RETRIES", value);
    expect(modelStreamMaxRetries()).toBe(Number(value));
  });

  it.each(["", " ", "-1", "6", "1.5", "NaN", "Infinity", "three", "3retries"])(
    "falls back to one retry for %j",
    (value) => {
      vi.stubEnv("MODEL_STREAM_MAX_RETRIES", value);
      expect(modelStreamMaxRetries()).toBe(1);
    },
  );
});

describe("billedPromptTokens", () => {
  it("adds cache read and write onto uncached input so the meter matches provider cost", () => {
    expect(
      billedPromptTokens({
        input: 12,
        output: 40,
        cacheRead: 8_000,
        cacheWrite: 200,
      }),
    ).toEqual({
      inputTokens: 8_212,
      outputTokens: 40,
      cacheReadTokens: 8_000,
      cacheWriteTokens: 200,
    });
  });

  it("keeps uncached-only usage unchanged when cache fields are absent", () => {
    expect(billedPromptTokens({ input: 100, output: 20 })).toEqual({
      inputTokens: 100,
      outputTokens: 20,
      cacheReadTokens: 0,
      cacheWriteTokens: 0,
    });
  });

  it("treats a fully cached prompt as billed input rather than zero", () => {
    expect(
      billedPromptTokens({
        input: 0,
        output: 15,
        cacheRead: 12_500,
        cacheWrite: 0,
      }),
    ).toEqual({
      inputTokens: 12_500,
      outputTokens: 15,
      cacheReadTokens: 12_500,
      cacheWriteTokens: 0,
    });
  });

  it("reports the cache halves alongside the billed total", () => {
    expect(billedPromptTokens({ input: 100, cacheRead: 40, cacheWrite: 10, output: 5 })).toEqual({
      inputTokens: 150,
      cacheReadTokens: 40,
      cacheWriteTokens: 10,
      outputTokens: 5,
    });
  });

  it("floors missing or negative cache counts at zero", () => {
    expect(billedPromptTokens({ input: 100, cacheRead: -40, output: 5 })).toEqual({
      inputTokens: 100,
      cacheReadTokens: 0,
      cacheWriteTokens: 0,
      outputTokens: 5,
    });
  });
});

describe("resolveCompletionMaxTokens", () => {
  it("uses the shared default instead of a model-card ceiling", () => {
    expect(resolveCompletionMaxTokens(128_000)).toBe(DEFAULT_MODEL_MAX_TOKENS);
  });

  it("honors a configured escape hatch up to the model cap", () => {
    expect(resolveCompletionMaxTokens(128_000, 8_192)).toBe(8_192);
    expect(resolveCompletionMaxTokens(2_048, 8_192)).toBe(2_048);
  });

  it("clamps an agent-supplied options maxTokens to the user cap", () => {
    expect(resolveCompletionMaxTokens(128_000, undefined, 128_000)).toBe(DEFAULT_MODEL_MAX_TOKENS);
    expect(resolveCompletionMaxTokens(128_000, 16_384, 128_000)).toBe(16_384);
  });

  it("gives a reasoning model room for thinking and a reply", () => {
    expect(resolveCompletionMaxTokens(128_000, undefined, undefined, true)).toBe(
      REASONING_MODEL_MAX_TOKENS,
    );
    // A smaller model ceiling still wins.
    expect(resolveCompletionMaxTokens(8_192, undefined, undefined, true)).toBe(8_192);
  });

  it("keeps the configured cap and the non-reasoning default intact", () => {
    expect(resolveCompletionMaxTokens(128_000, 8_192, undefined, true)).toBe(8_192);
    expect(resolveCompletionMaxTokens(128_000, undefined, undefined, false)).toBe(
      DEFAULT_MODEL_MAX_TOKENS,
    );
  });
});

describe("extractionThinkingLevel", () => {
  it("uses the default effort when a reasoning connection leaves it unset", () => {
    expect(extractionThinkingLevel({ reasoning: false })).toBe("off");
    expect(extractionThinkingLevel({ reasoning: true })).toBe("medium");
    expect(extractionThinkingLevel({ reasoning: true, thinkingLevel: null })).toBe("medium");
    expect(extractionThinkingLevel({ reasoning: true, minReasoningEffort: null })).toBe("medium");
  });

  it("keeps an explicit off and an explicit effort", () => {
    expect(extractionThinkingLevel({ reasoning: true, thinkingLevel: "off" })).toBe("off");
    expect(extractionThinkingLevel({ reasoning: true, thinkingLevel: "high" })).toBe("high");
    expect(extractionThinkingLevel({ reasoning: false, thinkingLevel: "high" })).toBe("off");
  });

  it("uses the stored minimum instead of the chat effort", () => {
    expect(extractionThinkingLevel({ reasoning: true, minReasoningEffort: "low" })).toBe("low");
    expect(
      extractionThinkingLevel({
        reasoning: true,
        thinkingLevel: "off",
        minReasoningEffort: "low",
      }),
    ).toBe("low");
    expect(
      extractionThinkingLevel({
        reasoning: true,
        thinkingLevel: "high",
        minReasoningEffort: "low",
      }),
    ).toBe("low");
    expect(extractionThinkingLevel({ reasoning: false, minReasoningEffort: "low" })).toBe("off");
  });
});

describe("extractionCompletionTokens", () => {
  it("keeps the answer cap when thinking is billed separately or the model does not reason", () => {
    expect(
      extractionCompletionTokens(
        { api: "anthropic-messages", reasoning: true, maxTokens: 128_000 },
        "off",
      ),
    ).toBe(EXTRACT_ANSWER_TOKENS);
    expect(
      extractionCompletionTokens(
        { api: "google-generative-ai", reasoning: true, maxTokens: 128_000 },
        "low",
      ),
    ).toBe(EXTRACT_ANSWER_TOKENS);
    expect(
      extractionCompletionTokens(
        { api: "openai-completions", reasoning: false, maxTokens: 128_000 },
        "off",
      ),
    ).toBe(EXTRACT_ANSWER_TOKENS);
  });

  it("uses the connection max when thinking cannot be disabled", () => {
    expect(
      extractionCompletionTokens(
        { api: "openai-completions", reasoning: true, maxTokens: 65_536 },
        "off",
      ),
    ).toBe(EXTRACT_ANSWER_TOKENS);
    expect(
      extractionCompletionTokens(
        { api: "openai-completions", reasoning: true, maxTokens: 65_536 },
        "low",
      ),
    ).toBe(65_536);
    expect(
      extractionCompletionTokens(
        { api: "openai-responses", reasoning: true, maxTokens: 128_000 },
        "low",
      ),
    ).toBe(128_000);
    expect(
      extractionCompletionTokens(
        { api: "openai-completions", reasoning: true, maxTokens: 2_048 },
        "low",
      ),
    ).toBe(2_048);
    expect(extractionCompletionTokens({ api: "openai-completions", reasoning: true }, "low")).toBe(
      REASONING_MODEL_MAX_TOKENS,
    );
  });
});

describe("taskCompletionTokens", () => {
  it("uses the task budget instead of the chat max when thinking shares the cap", () => {
    expect(taskCompletionTokens({ maxTokens: 65_536 }, "low", 1_024, 256)).toBe(1_024);
    expect(taskCompletionTokens({ maxTokens: 512 }, "low", 1_024, 256)).toBe(512);
    expect(taskCompletionTokens({ maxTokens: 65_536 }, "off", 1_024, 256)).toBe(256);
    expect(taskCompletionTokens({}, "minimal", 1_024, 256)).toBe(1_024);
    expect(
      taskCompletionTokens({ maxTokens: 65_536 }, "low", REASONING_MODEL_MAX_TOKENS, 256),
    ).toBe(REASONING_MODEL_MAX_TOKENS);
  });
});

describe("clipToolResultText", () => {
  it("bounds oversized tool output for the in-turn transcript", () => {
    const text = "x".repeat(TOOL_RESULT_TEXT_LIMIT + 50);
    const clipped = clipToolResultText(text);
    expect(clipped.endsWith("…")).toBe(true);
    expect(clipped.length).toBe(TOOL_RESULT_TEXT_LIMIT + 1);
  });
});

describe("clipToolResultContent", () => {
  it("shares one character budget across all text parts and leaves images", () => {
    const first = "a".repeat(TOOL_RESULT_TEXT_LIMIT - 10);
    const second = "b".repeat(40);
    const image = { type: "image" as const, data: "iVBORw0KGgo=", mimeType: "image/png" as const };
    const clipped = clipToolResultContent([
      { type: "text" as const, text: first },
      image,
      { type: "text" as const, text: second },
      { type: "text" as const, text: "later omitted" },
    ]);
    expect(clipped).toHaveLength(3);
    expect(clipped[0]).toEqual({ type: "text", text: first });
    expect(clipped[1]).toEqual(image);
    const overflow = clipped[2] as { type: "text"; text: string };
    expect(overflow.type).toBe("text");
    expect(overflow.text.endsWith("…")).toBe(true);
    expect(overflow.text.startsWith("b")).toBe(true);
    expect(overflow.text.length).toBe(11);
    const textChars = clipped
      .filter((part) => part.type === "text")
      .reduce((sum, part) => sum + part.text.replace(/…$/, "").length, 0);
    expect(textChars).toBe(TOOL_RESULT_TEXT_LIMIT);
  });
});
