import { AsyncLocalStorage } from "node:async_hooks";
import type { SpanAttributes, TelemetryContext, TelemetrySpan } from "@earendil-works/pi-telemetry";
import type { AgentRunRequest } from "@rakazo/adapter-kit";
import { getLogContext } from "@rakazo/logging";

const current = new AsyncLocalStorage<TelemetryContext>();
let exporter: Promise<{ context: TelemetryContext; shutdown(): Promise<void> }> | undefined;

type TraceParentIds = { traceId?: string; spanId?: string };

function agentRun(request: Pick<AgentRunRequest, "botId" | "runId" | "routineId">) {
  return {
    name: "rakazo.agent.run",
    attributes: {
      "rakazo.bot.id": request.botId,
      "rakazo.run.id": request.runId,
      "rakazo.routine.id": request.routineId ?? undefined,
    },
  };
}

async function loadExporter(): Promise<TelemetryContext | undefined> {
  if (!process.env.OTEL_EXPORTER_OTLP_ENDPOINT?.trim()) return undefined;
  exporter ??= import("./pi-telemetry-otel.js").then(({ createPiTelemetryExporter }) =>
    createPiTelemetryExporter(),
  );
  try {
    return (await exporter).context;
  } catch {
    // Invalid optional observability configuration must not fail a run or log headers.
    return undefined;
  }
}

function inSpan<T>(
  parent: TelemetryContext,
  name: string,
  attributes: SpanAttributes,
  work: (span: TelemetrySpan) => Promise<T>,
): Promise<T> {
  return parent.startSpan({ name, attributes }, (span) => current.run(span, () => work(span)));
}

/** Disabled deployments never load or initialize the OTel SDK. */
export async function withPiTelemetry<T>(
  request: Pick<AgentRunRequest, "botId" | "runId" | "routineId">,
  work: (span?: TelemetrySpan) => Promise<T>,
  correlation?: TraceParentIds,
): Promise<T> {
  const run = agentRun(request);
  const active = current.getStore();
  if (active) return inSpan(active, run.name, run.attributes, work);
  const telemetry = await loadExporter();
  if (!telemetry) return work();
  const start = () => inSpan(telemetry, run.name, run.attributes, work);
  if (correlation?.traceId && correlation.spanId) {
    const { remoteParentContext, runWithRemoteParent } = await import("./pi-telemetry-otel.js");
    const parent = remoteParentContext(correlation.traceId, correlation.spanId);
    if (parent) return runWithRemoteParent(parent, start);
  }
  return start();
}

/**
 * One span per locked background job so model calls inside it export under that
 * job. Idle Graphile SQL polls and HTTP health checks never reach here.
 */
export async function withWorkerJobTelemetry<T>(work: () => Promise<T>): Promise<T> {
  const active = current.getStore();
  if (active) {
    return inSpan(active, "rakazo.worker.job", jobAttributes(), () => work());
  }
  const telemetry = await loadExporter();
  if (!telemetry) return work();
  return inSpan(telemetry, "rakazo.worker.job", jobAttributes(), () => work());
}

function jobAttributes(): SpanAttributes {
  const ctx = getLogContext();
  const jobType = ctx["job.type"];
  const jobId = ctx["job.id"];
  return {
    "rakazo.job.type": typeof jobType === "string" ? jobType : undefined,
    "rakazo.job.id": typeof jobId === "string" ? jobId : undefined,
  };
}

export function withPiTelemetrySpan<T>(
  name: string,
  attributes: SpanAttributes,
  work: (span?: TelemetrySpan) => Promise<T>,
): Promise<T> {
  const parent = current.getStore();
  if (!parent) return work();
  return parent.startSpan({ name, attributes }, (span) => current.run(span, () => work(span)));
}

export async function shutdownPiTelemetry(): Promise<void> {
  if (!exporter) return;
  try {
    await (await exporter).shutdown();
  } catch {
    /* Export failures are best effort. */
  }
  exporter = undefined;
}

/** Content serialization is optional and must never interrupt model/tool execution. */
export function recordPiTelemetryContent(
  span: TelemetrySpan | undefined,
  key: string,
  value: unknown,
): void {
  if (!span || process.env.OTEL_CAPTURE_CONTENT !== "true") return;
  try {
    span.setAttributes({ [key]: JSON.stringify(value) });
  } catch {
    /* Non-serializable content is omitted. */
  }
}
