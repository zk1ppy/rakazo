/** Fact and source ids kept on one entity relation. */
export const MAX_RELATION_PROVENANCE_IDS = 64;

/**
 * Relation types compare as lowercase snake_case.
 * "lives in", "LIVES_IN", and "lives_in" are one type.
 * Empty, or longer than 64 characters, is not a type.
 */
export function normalizeRelationType(value: string): string | null {
  const normalized = value
    .normalize("NFKC")
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, "_")
    .replace(/[^a-z0-9_]/g, "")
    .replace(/_+/g, "_")
    .replace(/^_|_$/g, "");
  if (!normalized || normalized.length > 64) return null;
  return normalized;
}

/**
 * Directed identity of one business relation.
 * "lives in" and "LIVES_IN" share a key. The reverse direction does not.
 * An empty type has no key: callers keep those edges out of a typed collapse.
 */
export function relationCollapseKey(fromId: string, toId: string, type: string): string | null {
  const normalized = normalizeRelationType(type);
  if (!normalized || !fromId.trim() || !toId.trim() || fromId === toId) return null;
  return `${fromId}\n${normalized}\n${toId}`;
}

/** Order-preserving unique union, capped so one edge cannot grow without bound. */
export function unionRelationIds(
  existing: readonly string[],
  incoming: readonly string[],
  cap = MAX_RELATION_PROVENANCE_IDS,
): string[] {
  const out: string[] = [];
  for (const id of [...existing, ...incoming]) {
    if (typeof id !== "string") continue;
    const trimmed = id.trim();
    if (!trimmed || out.includes(trimmed)) continue;
    out.push(trimmed);
    if (out.length >= cap) break;
  }
  return out;
}

export interface RelationEdgeProvenance {
  factIds: readonly string[];
  sourceIds: readonly string[];
  firstSeen: string | null;
  lastSeen: string | null;
  validFrom: string | null;
  validTo: string | null;
  time: string | null;
  createdAt: string | null;
}

export interface RelationProvenanceInput {
  factId?: string | null;
  sourceIds?: readonly string[];
  learnedAt?: string | null;
  time?: string | null;
}

export interface MergedRelationProvenance {
  factIds: string[];
  factId: string;
  sourceIds: string[];
  mentions: number;
  firstSeen: string | null;
  lastSeen: string | null;
  validFrom: string | null;
  validTo: string | null;
  time: string | null;
  createdAt: string | null;
}

/**
 * Provenance for one directed pair and one relation type.
 * Mentions is the number of stored fact ids, so a repeated fact does not
 * increase it. firstSeen is the earlier stamp. lastSeen is the later one.
 * validTo stays empty when any copy is still active.
 */
export function mergeRelationProvenance(
  edges: readonly RelationEdgeProvenance[],
  incoming?: RelationProvenanceInput,
): MergedRelationProvenance {
  let factIds: string[] = [];
  let sourceIds: string[] = [];
  let firstSeen: string | null = null;
  let lastSeen: string | null = null;
  let active = edges.length === 0;
  let validTo: string | null = null;
  let time: string | null = null;
  let createdAt: string | null = null;
  for (const edge of edges) {
    factIds = unionRelationIds(factIds, edge.factIds);
    sourceIds = unionRelationIds(sourceIds, edge.sourceIds);
    firstSeen = earlierStamp(firstSeen, edge.firstSeen);
    firstSeen = earlierStamp(firstSeen, edge.validFrom);
    lastSeen = laterStamp(lastSeen, edge.lastSeen);
    lastSeen = laterStamp(lastSeen, edge.validFrom);
    const open = edge.validTo == null || edge.validTo.trim() === "";
    if (open) active = true;
    else validTo = laterStamp(validTo, edge.validTo);
    if (!time) time = stamp(edge.time);
    createdAt = earlierStamp(createdAt, edge.createdAt);
  }
  if (incoming?.factId) factIds = unionRelationIds(factIds, [incoming.factId]);
  if (incoming?.sourceIds) sourceIds = unionRelationIds(sourceIds, incoming.sourceIds);
  const learnedAt = stamp(incoming?.learnedAt);
  if (learnedAt) {
    firstSeen = earlierStamp(firstSeen, learnedAt);
    lastSeen = laterStamp(lastSeen, learnedAt);
    active = true;
  }
  if (!time) time = stamp(incoming?.time);
  return {
    factIds,
    factId: factIds[0] ?? "",
    sourceIds,
    mentions: factIds.length,
    firstSeen,
    lastSeen,
    validFrom: firstSeen,
    validTo: active ? null : validTo,
    time,
    createdAt,
  };
}

function earlierStamp(
  left: string | null | undefined,
  right: string | null | undefined,
): string | null {
  const first = stamp(left);
  const second = stamp(right);
  if (!first) return second;
  if (!second) return first;
  return first < second ? first : second;
}

function laterStamp(
  left: string | null | undefined,
  right: string | null | undefined,
): string | null {
  const first = stamp(left);
  const second = stamp(right);
  if (!first) return second;
  if (!second) return first;
  return first > second ? first : second;
}

function stamp(value: string | null | undefined): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed || null;
}

/**
 * Business types kept on a new edge. Anything else is a synonym or `related_to`.
 * `*` on MEMORY_RELATION_TYPES keeps the snake_case type and skips the map.
 */
export const DEFAULT_RELATION_TYPES = [
  "knows",
  "works_at",
  "works_on",
  "lives_in",
  "located_in",
  "part_of",
  "member_of",
  "owns",
  "created",
  "attended",
  "uses",
  "founded",
  "manages",
  "reports_to",
  "parent_of",
  "likes",
  "has",
  "related_to",
] as const;

const DEFAULT_RELATION_SYNONYMS: Readonly<Record<string, string>> = {
  contact_of: "knows",
  acquainted_with: "knows",
  friend_of: "knows",
  employed_by: "works_at",
  works_for: "works_at",
  employee_of: "works_at",
  based_in: "lives_in",
  resides_in: "lives_in",
  lives_at: "lives_in",
  located_at: "located_in",
  belongs_to: "part_of",
  mentioned_in: "related_to",
  discussed_in: "related_to",
  associated_with: "related_to",
  linked_to: "related_to",
};

export interface RelationVocabulary {
  /** Keep the snake_case type. Synonyms and the allowed list are not applied. */
  passthrough: boolean;
  allowed: ReadonlySet<string>;
  synonyms: ReadonlyMap<string, string>;
}

const RELATION_TYPE_TOKEN = /^[a-z0-9_]{1,64}$/;

/** Unset uses the built-in list. `*` leaves types as snake_case. A list replaces it. */
export function readRelationVocabulary(env: NodeJS.ProcessEnv = process.env): RelationVocabulary {
  const rawTypes = env.MEMORY_RELATION_TYPES?.trim() ?? "";
  if (rawTypes === "*") {
    return { passthrough: true, allowed: new Set(), synonyms: new Map() };
  }
  const allowed = new Set<string>(DEFAULT_RELATION_TYPES);
  if (rawTypes) {
    allowed.clear();
    for (const part of rawTypes.split(",")) {
      const type = normalizeRelationType(part);
      if (type) allowed.add(type);
    }
  }
  allowed.add("related_to");
  const synonyms = new Map<string, string>();
  const rawSynonyms = env.MEMORY_RELATION_SYNONYMS;
  const source =
    rawSynonyms === undefined
      ? Object.entries(DEFAULT_RELATION_SYNONYMS)
      : rawSynonyms.split(",").map((entry) => {
          const split = entry.split(":");
          return [split[0] ?? "", split.slice(1).join(":")] as const;
        });
  for (const [fromRaw, toRaw] of source) {
    const from = normalizeRelationType(fromRaw);
    const to = normalizeRelationType(toRaw);
    if (!from || !to || from === to) continue;
    synonyms.set(from, allowed.has(to) ? to : "related_to");
  }
  return { passthrough: false, allowed, synonyms };
}

/**
 * Vocabulary applied when an edge is written.
 * An allowed type stays. A synonym becomes its canonical type. Anything else is `related_to`.
 * Empty input is not a type.
 */
export function canonicalRelationType(
  value: string,
  vocabulary: RelationVocabulary = readRelationVocabulary(),
): string | null {
  const normalized = normalizeRelationType(value);
  if (!normalized) return null;
  if (vocabulary.passthrough) return normalized;
  if (vocabulary.allowed.has(normalized)) return normalized;
  return vocabulary.synonyms.get(normalized) ?? "related_to";
}

/**
 * Cypher CASE for a value that is already snake_case.
 * Only tokens that match the type pattern are interpolated.
 */
export function relationTypeCase(expr: string, vocabulary: RelationVocabulary): string {
  if (vocabulary.passthrough) return expr;
  const lines = [`CASE ${expr}`];
  for (const [from, to] of vocabulary.synonyms) {
    if (!RELATION_TYPE_TOKEN.test(from) || !RELATION_TYPE_TOKEN.test(to)) continue;
    lines.push(`WHEN '${from}' THEN '${to}'`);
  }
  lines.push(
    `ELSE CASE WHEN ${expr} IN $relationAllowed THEN ${expr} ELSE 'related_to' END`,
    "END",
  );
  return lines.join(" ");
}
