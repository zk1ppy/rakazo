import { describe, expect, it } from "vitest";
import {
  applyOpencodeShellEnvironment,
  applyOpencodeShellUrls,
  opencodeShellEnvironment,
} from "./opencode-shell-environment.js";

describe("OpenCode shell environment", () => {
  it("forwards only the OpenCode variables that are set", () => {
    expect(
      opencodeShellEnvironment({
        OPENCODE_SERVER_URL: " http://opencode.example:5003 ",
        OPENCODE_PUBLIC_URL: "",
        OPENCODE_PASSWORD: "super-secret-password",
        OTHER: "nope",
      }),
    ).toEqual({
      OPENCODE_SERVER_URL: "http://opencode.example:5003",
      OPENCODE_PASSWORD: "super-secret-password",
    });
  });

  it("redacts only worker passwords and applies URLs after that snapshot", () => {
    const agent: Record<string, string> = {};
    const source = {
      OPENCODE_SERVER_URL: " http://opencode.example:5003 ",
      OPENCODE_PUBLIC_URL: "https://opencode.example",
      OPENCODE_PASSWORD: "super-secret-password",
      OPENCODE_SERVER_PASSWORD: "alias-secret",
    };
    const redactions = applyOpencodeShellEnvironment(agent, {}, source);
    const secrets = [...Object.values(agent), ...redactions];
    expect(agent).toEqual({ OPENCODE_PASSWORD: "super-secret-password" });
    expect(redactions).toEqual(["super-secret-password", "alias-secret"]);
    expect(secrets).not.toContain("http://opencode.example:5003");
    expect(secrets).not.toContain("https://opencode.example");
    applyOpencodeShellUrls(agent, {}, source);
    expect(agent).toEqual({
      OPENCODE_PASSWORD: "super-secret-password",
      OPENCODE_SERVER_URL: "http://opencode.example:5003",
      OPENCODE_PUBLIC_URL: "https://opencode.example",
    });
  });

  it("does not replace an agent secret or a bot command variable", () => {
    const agent = { OPENCODE_PASSWORD: "saved-agent-secret" };
    const source = {
      OPENCODE_SERVER_URL: "http://opencode.example:5003",
      OPENCODE_PUBLIC_URL: "https://opencode.example",
      OPENCODE_PASSWORD: "worker-secret",
    };
    const redactions = applyOpencodeShellEnvironment(
      agent,
      { OPENCODE_SERVER_URL: "saved-bot-url" },
      source,
    );
    expect(redactions).toEqual(["worker-secret"]);
    applyOpencodeShellUrls(agent, { OPENCODE_SERVER_URL: "saved-bot-url" }, source);
    expect(agent).toEqual({
      OPENCODE_PASSWORD: "saved-agent-secret",
      OPENCODE_PUBLIC_URL: "https://opencode.example",
    });
    expect(Object.values(agent)).not.toContain("http://opencode.example:5003");
    expect(Object.values(agent)).not.toContain("worker-secret");
  });

  it("does not invent a shell password from the server password alias", () => {
    const agent: Record<string, string> = {};
    const redactions = applyOpencodeShellEnvironment(
      agent,
      {},
      { OPENCODE_SERVER_PASSWORD: "alias-secret" },
    );
    expect(agent).toEqual({});
    expect(redactions).toEqual(["alias-secret"]);
  });
});
