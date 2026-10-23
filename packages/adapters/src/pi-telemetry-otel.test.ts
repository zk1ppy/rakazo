import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { TraceFlags, trace } from "@opentelemetry/api";
import {
  InMemorySpanExporter,
  NodeTracerProvider,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-node";
import { runWithLogContext } from "@rakazo/logging";
import { afterEach, expect, it, vi } from "vitest";
import { createPiTelemetryExporter, otelPiContext } from "./pi-telemetry-otel.js";

afterEach(() => vi.unstubAllEnvs());

it("parents a root span on the logging trace and span ids", async () => {
  const output = new InMemorySpanExporter();
  const provider = new NodeTracerProvider({ spanProcessors: [new SimpleSpanProcessor(output)] });
  const traceId = "ab".repeat(16);
  const spanId = "cd".repeat(8);
  const context = otelPiContext(provider.getTracer("fixture"), false);
  try {
    await runWithLogContext({ "trace.id": traceId, "span.id": spanId }, () =>
      context.startSpan({ name: "rakazo.agent.run" }, (span) =>
        span.startSpan(
          { name: "pi.ai.request", attributes: { "pi.ai.model": "fixture" } },
          async () => undefined,
        ),
      ),
    );
    await runWithLogContext({ "trace.id": "0".repeat(32), "span.id": "0".repeat(16) }, () =>
      context.startSpan({ name: "rakazo.agent.run" }, async () => {}),
    );
    await provider.forceFlush();
    const linked = output
      .getFinishedSpans()
      .filter((span) => span.spanContext().traceId === traceId);
    const run = linked.find((span) => span.name === "rakazo.agent.run")!;
    const model = linked.find((span) => span.name === "pi.ai.request")!;
    expect(run.parentSpanContext).toMatchObject({
      traceId,
      spanId,
      isRemote: true,
      traceFlags: TraceFlags.SAMPLED,
    });
    expect(model.parentSpanContext?.spanId).toBe(run.spanContext().spanId);
    expect(model.attributes["pi.ai.model"]).toBe("fixture");
    const rejected = output
      .getFinishedSpans()
      .find((span) => span.spanContext().traceId !== traceId && span.name === "rakazo.agent.run")!;
    expect(rejected.parentSpanContext).toBeUndefined();
    expect(rejected.spanContext().traceId).not.toBe("0".repeat(32));
  } finally {
    await provider.shutdown();
  }
});

it("does not register a global tracer, so health checks and database polls stay untraced", async () => {
  const before = trace.getTracerProvider();
  vi.stubEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9");
  const exporter = createPiTelemetryExporter();
  try {
    expect(trace.getTracerProvider()).toBe(before);
    trace.getTracer("http").startSpan("GET /health").end();
    trace.getTracer("pg").startSpan("graphile.poll").end();
  } finally {
    await exporter.shutdown().catch(() => undefined);
  }
});

it("exports OTLP/HTTP with standard endpoint, headers and service name on shutdown", async () => {
  const received: Array<{ url?: string; authorization?: string; body: string }> = [];
  const server = createServer(async (request, response) => {
    let body = "";
    for await (const chunk of request) body += chunk;
    received.push({ url: request.url, authorization: request.headers.authorization, body });
    response.setHeader("content-type", "application/json");
    response.end("{}");
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  vi.stubEnv(
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
  );
  vi.stubEnv("OTEL_EXPORTER_OTLP_HEADERS", "authorization=Bearer%20fake-fixture-token");
  vi.stubEnv("OTEL_SERVICE_NAME", "fixture-worker");
  const exporter = createPiTelemetryExporter();
  try {
    await exporter.context.startSpan(
      {
        name: "pi.ai.request",
        attributes: { "pi.ai.model": "fixture", "pi.ai.prompt": "private" },
      },
      async () => {},
    );
    await exporter.shutdown();
    expect(received).toHaveLength(1);
    expect(received[0]?.url).toBe("/v1/traces");
    expect(received[0]?.authorization).toBe("Bearer fake-fixture-token");
    const payload = JSON.parse(received[0]!.body);
    expect(payload.resourceSpans[0].resource.attributes).toContainEqual({
      key: "service.name",
      value: { stringValue: "fixture-worker" },
    });
    expect(received[0]?.body).toContain("pi.ai.request");
    expect(received[0]?.body).not.toMatch(/private|fake-fixture-token/);
  } finally {
    await exporter.shutdown().catch(() => undefined);
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});
