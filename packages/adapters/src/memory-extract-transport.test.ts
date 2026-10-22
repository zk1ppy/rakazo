import { once } from "node:events";
import type { IncomingMessage } from "node:http";
import { createServer } from "node:http";
import type { AgentRuntimeEvent } from "@rakazo/adapter-kit";
import { describe, expect, it } from "vitest";
import { parseMemoryExtraction } from "./memory-extract.js";
import { PiAgentRuntime } from "./pi-runtime.js";

const modelId = "hf:zai-org/GLM-5.3-Flash";
const entityJson = '{"entities":[{"type":"Person","name":"Nicolas"}],"relations":[]}';

type TransportBody = {
  max_tokens?: number;
  max_completion_tokens?: number;
  reasoning_effort?: string;
};

function sse(frames: unknown[]): string {
  return `${frames.map((frame) => `data: ${JSON.stringify(frame)}\n\n`).join("")}data: [DONE]\n\n`;
}

function completion(options: {
  reasoning?: string;
  content?: string;
  finish: "stop" | "length";
}): string {
  const frames: unknown[] = [];
  if (options.reasoning) {
    frames.push({
      id: "fixture",
      choices: [{ index: 0, delta: { reasoning_content: options.reasoning }, finish_reason: null }],
    });
  }
  if (options.content) {
    frames.push({
      id: "fixture",
      choices: [{ index: 0, delta: { content: options.content }, finish_reason: null }],
    });
  }
  frames.push({
    id: "fixture",
    choices: [{ index: 0, delta: {}, finish_reason: options.finish }],
  });
  return sse(frames);
}

async function readBody(request: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString("utf8");
}

function sentMaxOutput(body: TransportBody): number {
  const fields = [body.max_tokens, body.max_completion_tokens].filter(
    (value): value is number => value !== undefined,
  );
  expect(fields).toHaveLength(1);
  return fields[0] ?? 0;
}

async function capture(options: {
  thinkingLevel?: "off" | "medium" | null;
  reasoning?: string;
  content?: string;
  finish?: "stop" | "length";
}) {
  const bodies: TransportBody[] = [];
  const server = createServer(async (request, response) => {
    bodies.push(JSON.parse(await readBody(request)) as TransportBody);
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.end(
      completion({
        reasoning: options.reasoning,
        content: options.content,
        finish: options.finish ?? "stop",
      }),
    );
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("fixture endpoint unavailable");
  const events: AgentRuntimeEvent[] = [];
  try {
    for await (const event of new PiAgentRuntime().run(
      {
        botId: "bot",
        threadId: "thread",
        runId: `extract-wire-${options.finish ?? "stop"}-${options.thinkingLevel ?? "default"}`,
        prompt: "Extract.",
        instructions: "Reply with JSON.",
        history: [],
        tools: [],
        allowSilentEmpty: true,
        providerRetries: 0,
        visibleOutputTokens: 1_024,
        model: {
          provider: "openai-compatible",
          id: modelId,
          baseUrl: `http://127.0.0.1:${address.port}/v1`,
          allowPrivateEndpoint: true,
          reasoning: true,
          thinkingLevel: options.thinkingLevel,
          maxTokens: 65_536,
          contextWindow: 524_288,
        },
      },
      {
        operationId: "extract-wire",
        traceId: "extract-wire",
        spaceId: "space",
        userId: "user",
        signal: AbortSignal.timeout(15_000),
      },
    )) {
      events.push(event);
    }
  } finally {
    server.close();
    server.closeAllConnections();
    await once(server, "close");
  }
  const body = bodies[0];
  if (!body) throw new Error("provider received no request");
  return { body, bodies, events };
}

describe("extraction transport", () => {
  it("sends the connection max output when effort is left at the default", async () => {
    const { body, bodies, events } = await capture({
      thinkingLevel: null,
      reasoning: entityJson,
    });
    expect(bodies).toHaveLength(1);
    expect(sentMaxOutput(body)).toBe(65_536);
    expect(body.reasoning_effort).toBe("medium");
    const done = events.find((event) => event.type === "done");
    expect(done).toMatchObject({ type: "done", stopReason: "stop" });
    if (done?.type !== "done") throw new Error("missing done");
    expect(done.text).toContain('"name":"Nicolas"');
    expect(parseMemoryExtraction(done.text ?? "", "space", "ns").entities).toEqual([
      expect.objectContaining({ type: "Person", name: "Nicolas" }),
    ]);
  });

  it("sends the visible cap when thinking is explicitly off", async () => {
    const { body, bodies } = await capture({
      thinkingLevel: "off",
      content: entityJson,
    });
    expect(bodies).toHaveLength(1);
    expect(sentMaxOutput(body)).toBe(1_024);
    expect(body.reasoning_effort).toBe("none");
  });

  it("keeps JSON that arrived only in reasoning when the completion hits the cap", async () => {
    const { body, events } = await capture({
      thinkingLevel: null,
      reasoning: entityJson,
      finish: "length",
    });
    expect(sentMaxOutput(body)).toBe(65_536);
    const done = events.find((event) => event.type === "done");
    expect(done).toMatchObject({ type: "done", text: entityJson, stopReason: "length" });
  });
});
