import { afterEach, describe, expect, it, vi } from "vitest";
import type { CleanupSpace } from "./memory-cleanup.js";
import {
  classifyCleanupEntity,
  cleanupMemoryGraph,
  formatCleanupReport,
  parseCleanupGraphArgs,
  readCleanupObjectHubDegree,
  staleDeclinedPairs,
} from "./memory-cleanup.js";

const names: Dropped = { dropSnakeCaseIds: true, patterns: [] };

type Dropped = { dropSnakeCaseIds: boolean; patterns: RegExp[] };

function space(): CleanupSpace & {
  deleted: string[][];
  forgotten: Array<{ leftId: string; rightId: string }>;
} {
  const deleted: string[][] = [];
  const forgotten: Array<{ leftId: string; rightId: string }> = [];
  return {
    spaceId: "space-1",
    deleted,
    forgotten,
    listEntities: async () => [
      { id: "1", name: "shared/fabriq/prep-notes", type: "Object" },
      { id: "2", name: "jean-pierre", type: "Person" },
      { id: "3", name: "importer_garmin", type: "Object" },
      { id: "4", name: "notes.md", type: "Object" },
    ],
    deleteEntities: async (ids) => {
      deleted.push(ids);
      return ids.length;
    },
    listDeclined: async () => [
      {
        leftId: "a",
        rightId: "b",
        leftFingerprint: "old",
        rightFingerprint: "same",
        currentLeft: "new",
        currentRight: "same",
      },
      {
        leftId: "c",
        rightId: "d",
        leftFingerprint: "same",
        rightFingerprint: "same",
        currentLeft: "same",
        currentRight: "same",
      },
    ],
    forgetDeclined: async (pairs) => {
      forgotten.push(...pairs);
      return pairs.length;
    },
  };
}

describe("cleanupMemoryGraph", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("parses a dry-run by default and requires --apply to write", () => {
    expect(parseCleanupGraphArgs([])).toEqual({ dryRun: true });
    expect(parseCleanupGraphArgs(["--"])).toEqual({ dryRun: true });
    expect(parseCleanupGraphArgs(["--", "--space", "space-1", "--dry-run"])).toEqual({
      spaceId: "space-1",
      dryRun: true,
    });
    expect(parseCleanupGraphArgs(["--apply", "--space", "space-1"])).toEqual({
      spaceId: "space-1",
      dryRun: false,
    });
    expect(() => parseCleanupGraphArgs(["--apply", "--dry-run"])).toThrow(
      /cannot be used together/,
    );
    expect(() => parseCleanupGraphArgs(["--space"])).toThrow(/space id/);
  });

  it("lists path-shaped names and stale refusals without writing", async () => {
    const graph = space();
    const counts = await cleanupMemoryGraph([graph], { dryRun: true }, names);
    expect(counts).toMatchObject({
      spaces: 1,
      failed: 0,
      entities: 3,
      declinedStale: 1,
      deleted: 0,
      forgotten: 0,
      dryRun: true,
      sample: ["shared/fabriq/prep-notes", "importer_garmin", "notes.md"],
    });
    expect(graph.deleted).toEqual([]);
    expect(graph.forgotten).toEqual([]);
    expect(staleDeclinedPairs(await graph.listDeclined())).toHaveLength(1);
  });

  it("lists event and object hubs without deleting them", async () => {
    expect(readCleanupObjectHubDegree({})).toBe(12);
    expect(readCleanupObjectHubDegree({ MEMORY_CLEANUP_OBJECT_HUB_DEGREE: "0" })).toBe(0);
    expect(classifyCleanupEntity({ id: "e", name: "Test HIIT", type: "Event", degree: 51 })).toBe(
      "event-hub",
    );
    expect(classifyCleanupEntity({ id: "e", name: "Test HIIT", type: "Event", degree: 8 })).toBe(
      "keep",
    );
    expect(classifyCleanupEntity({ id: "n", name: "Nicolas", type: "Person", degree: 289 })).toBe(
      "keep",
    );
    expect(classifyCleanupEntity({ id: "u", name: "Unix", type: "Object", degree: 21 })).toBe(
      "object-hub",
    );
    expect(classifyCleanupEntity({ id: "g", name: "Gmail", type: "Object", degree: 12 })).toBe(
      "object-hub",
    );
    expect(classifyCleanupEntity({ id: "m", name: "Mimir", type: "Object", degree: 11 })).toBe(
      "keep",
    );
    expect(
      classifyCleanupEntity({
        id: "f",
        name: "grafana-homelab-alertes.md",
        type: "Object",
        degree: 15,
      }),
    ).toBe("delete");
    expect(
      classifyCleanupEntity({ id: "a", name: "Ammerschwihr", type: "Location", degree: 40 }),
    ).toBe("keep");
    const graph = space();
    graph.listEntities = async () => [
      { id: "file", name: "grafana-homelab-alertes.md", type: "Object", degree: 15 },
      { id: "hiit", name: "Test HIIT", type: "Event", degree: 51 },
      { id: "tyr", name: "Týr", type: "Object", degree: 49 },
      { id: "unix", name: "Unix", type: "Object", degree: 21 },
      { id: "nicolas", name: "Nicolas Tremblier", type: "Person", degree: 289 },
      { id: "fabriq", name: "Fabriq", type: "Organization", degree: 33 },
    ];
    const dry = await cleanupMemoryGraph([graph], { dryRun: true }, names, {
      eventDegree: 8,
      objectHubDegree: 12,
    });
    expect(dry.entities).toBe(1);
    expect(dry.eventHubs).toBe(1);
    expect(dry.objectHubs).toBe(2);
    expect(dry.sample).toEqual(["grafana-homelab-alertes.md"]);
    expect(dry.hubSample).toEqual([
      "event-hub Test HIIT 51",
      "object-hub Týr 49",
      "object-hub Unix 21",
    ]);
    expect(dry.deleted).toBe(0);
    expect(graph.deleted).toEqual([]);
    const applied = await cleanupMemoryGraph([graph], { dryRun: false }, names, {
      eventDegree: 8,
      objectHubDegree: 12,
    });
    expect(applied.deleted).toBe(1);
    expect(graph.deleted).toEqual([["file"]]);
    const report = formatCleanupReport(applied, []);
    expect(report).toContain("event-hubs 1");
    expect(report).toContain("object-hubs 2");
    expect(report).toContain("sample-hub event-hub Test HIIT 51");
    expect(report).toContain("sample-hub object-hub Týr 49");
    expect(report).toContain("deleted 1");
  });

  it("drops operational names with the configured patterns", async () => {
    vi.stubEnv("MEMORY_EXTRACT_OPERATIONAL_PATTERNS", "^agenda\\b");
    const graph = space();
    graph.listEntities = async () => [
      { id: "agenda", name: "Agenda du lundi", type: "Event", degree: 1 },
      { id: "brief", name: "Brief 9h 2026-10-23", type: "Event", degree: 1 },
    ];
    const dry = await cleanupMemoryGraph([graph], { dryRun: true });
    expect(dry.entities).toBe(1);
    expect(dry.sample).toEqual(["Agenda du lundi"]);
    expect(dry.eventHubs).toBe(0);
    expect(graph.deleted).toEqual([]);
  });

  it("detaches matching entities and forgets stale refusals when applied", async () => {
    const graph = space();
    const counts = await cleanupMemoryGraph([graph], { dryRun: false }, names);
    expect(counts.deleted).toBe(3);
    expect(counts.forgotten).toBe(1);
    expect(graph.deleted).toEqual([["1", "3", "4"]]);
    expect(graph.forgotten).toEqual([{ leftId: "a", rightId: "b" }]);
    const report = formatCleanupReport(counts, []);
    expect(report).toContain("dry-run false");
    expect(report).toContain("entities 3");
    expect(report).toContain("declined-stale 1");
    expect(report).toContain("sample shared/fabriq/prep-notes");
    expect(report).toContain("deleted 3");
    expect(report).toContain("forgotten 1");
  });

  it("counts a space that cannot be listed and still reports the others", async () => {
    const broken: CleanupSpace = {
      spaceId: "space-2",
      listEntities: async () => {
        throw new Error("bolt down");
      },
      deleteEntities: async () => 0,
      listDeclined: async () => [],
      forgetDeclined: async () => 0,
    };
    const counts = await cleanupMemoryGraph([broken, space()], { dryRun: true }, names);
    expect(counts.spaces).toBe(2);
    expect(counts.failed).toBe(1);
    expect(counts.entities).toBe(3);
    const report = formatCleanupReport(counts, [
      { spaceId: "space-9", reason: "memory secret is not visible" },
    ]);
    expect(report).toContain("space space-9 failed memory secret is not visible");
    expect(report).toContain("failed 2");
    expect(report).toContain("deleted dry-run");
  });
});
