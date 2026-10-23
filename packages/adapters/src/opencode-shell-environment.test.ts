import { describe, expect, it } from "vitest";
import {
  applyOpencodeShellEnvironment,
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

  it("does not replace an agent secret or a bot command variable", () => {
    const agent = { OPENCODE_PASSWORD: "saved-agent-secret" };
    const redactions = applyOpencodeShellEnvironment(
      agent,
      { OPENCODE_SERVER_URL: "saved-bot-url" },
      {
        OPENCODE_SERVER_URL: "http://opencode.example:5003",
        OPENCODE_PUBLIC_URL: "https://opencode.example",
        OPENCODE_PASSWORD: "worker-secret",
      },
    );
    expect(agent).toEqual({
      OPENCODE_PASSWORD: "saved-agent-secret",
      OPENCODE_PUBLIC_URL: "https://opencode.example",
    });
    expect(redactions).toEqual([
      "http://opencode.example:5003",
      "https://opencode.example",
      "worker-secret",
    ]);
  });
});
