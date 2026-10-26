// @vitest-environment jsdom
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { act } from "react";
import type { Root } from "react-dom/client";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { MobileGroup } from "./api";
import type { InboxSpace } from "./inbox-spaces";
import { addToSearchInbox, setSearchInbox, useInboxGroup } from "./search-inbox";

vi.mock("./api", () => ({ currentApiBase: () => "http://api.test" }));
vi.mock("./session", () => ({
  currentSessionGeneration: () => 1,
  subscribeSessionGeneration: () => () => undefined,
}));

const app = (file: string) =>
  readFileSync(join(dirname(fileURLToPath(import.meta.url)), `../app/${file}`), "utf8");
const thread = app("thread.tsx");

const members = [
  { botId: "bot-2", name: "Scout", color: "#3B82F6::shape_1" },
  { botId: "bot-1", name: "Chief", color: "#F97316::shape_0" },
];
const space = {
  id: "space-1",
  name: "Personal",
  isDefault: true,
  hasContent: true,
  bots: [],
  groups: [{ id: "group-1", name: "Duo", members }],
} as unknown as InboxSpace;

let root: Root;
let container: HTMLDivElement;
let seen: unknown;
function Probe({ groupId }: { groupId: string }) {
  seen = useInboxGroup(groupId)?.members;
  return null;
}

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  setSearchInbox([]);
  seen = undefined;
  container = document.createElement("div");
  root = createRoot(container);
});
afterEach(() => {
  act(() => root.unmount());
  vi.unstubAllGlobals();
});

it("has a group's members, in its order, as soon as the inbox has loaded it", () => {
  setSearchInbox([space]);
  act(() => root.render(<Probe groupId="group-1" />));
  expect(seen).toBe(members);
  act(() => root.render(<Probe groupId="group-2" />));
  expect(seen).toBeUndefined();
});

it("seats a group that opens before the inbox knows it once the inbox loads", () => {
  act(() => root.render(<Probe groupId="group-1" />));
  expect(seen).toBeUndefined();
  act(() => setSearchInbox([space]));
  expect(seen).toBe(members);
});

it("seats a just-created group the inbox doesn't list yet once its screen adds it", () => {
  setSearchInbox([space]);
  act(() => root.render(<Probe groupId="group-new" />));
  expect(seen).toBeUndefined();
  const created = {
    id: "group-new",
    spaceId: "space-1",
    name: "New",
    members,
  } as unknown as MobileGroup;
  act(() => addToSearchInbox({ kind: "group", group: created }));
  expect(seen).toBe(members);
});

it("keeps seating a just-created group through an inbox refresh that predates it", () => {
  setSearchInbox([space]);
  const created = {
    id: "group-new",
    spaceId: "space-1",
    name: "New",
    members,
  } as unknown as MobileGroup;
  addToSearchInbox({ kind: "group", group: created });
  act(() => root.render(<Probe groupId="group-new" />));
  act(() => setSearchInbox([space]));
  expect(seen).toBe(members);
});

it("publishes the members the chat loads to its title", () => {
  expect(thread).toMatch(
    /const groupMembers = groupId\s+\? \(snap\?\.members \?\? mentionGroups\.find\(\(group\) => group\.id === groupId\)\?\.members\)/,
  );
  const publish = thread.slice(thread.indexOf("publishChatTitle(route.key, {"));
  expect(publish.slice(0, publish.indexOf("});"))).toContain("members: groupMembers,");
});
