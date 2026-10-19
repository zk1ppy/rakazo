// @vitest-environment jsdom

import { i18n } from "@lingui/core";
import type { SpaceMemoryConfig } from "@rakazo/contracts";
import type { ComponentProps, ReactNode } from "react";
import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@lingui/react/macro", () => {
  const t = (parts: TemplateStringsArray, ...values: unknown[]) =>
    parts.reduce((text, part, index) => `${text}${index > 0 ? values[index - 1] : ""}${part}`, "");
  return { useLingui: () => ({ t }), Trans: ({ children }: { children: ReactNode }) => children };
});

vi.mock("../../lib/rpc", () => ({
  rpc: {
    models: {
      credentials: vi.fn(async () => []),
      list: vi.fn(async () => []),
    },
  },
}));

vi.mock("@rakazo/ui-web", () => {
  const Container = ({ children }: { children?: ReactNode }) => <div>{children}</div>;
  return {
    Button: (props: ComponentProps<"button">) => <button {...props} />,
    Field: Container,
    FieldError: Container,
    FieldLabel: ({ children, htmlFor }: { children?: ReactNode; htmlFor?: string }) => (
      <label htmlFor={htmlFor}>{children}</label>
    ),
    Input: (props: ComponentProps<"input">) => <input {...props} />,
    Label: ({ children, htmlFor }: { children?: ReactNode; htmlFor?: string }) => (
      <label htmlFor={htmlFor}>{children}</label>
    ),
    NativeSelect: (props: ComponentProps<"select">) => <select {...props} />,
    NativeSelectOption: (props: ComponentProps<"option">) => <option {...props} />,
    Switch: ({
      checked,
      onCheckedChange: _onCheckedChange,
      ...props
    }: ComponentProps<"input"> & { onCheckedChange?: (checked: boolean) => void }) => (
      <input type="checkbox" readOnly checked={checked} {...props} />
    ),
  };
});

import { Neo4jExtractionSettings } from "./Neo4jSettingsForm";

function config(settings: Record<string, string>): SpaceMemoryConfig {
  return {
    provider: "neo4j",
    settings,
    defaultMemoryScope: "shared",
    updatedAt: "2026-10-19T00:00:00.000Z",
  };
}

function labeled<T extends HTMLElement>(container: HTMLElement, label: string, tag: string): T {
  const found = [...container.querySelectorAll("label")].find(
    (entry) => entry.textContent === label,
  );
  const control = found?.htmlFor
    ? container.querySelector<T>(`${tag}#${CSS.escape(found.htmlFor)}`)
    : null;
  if (!control) throw new Error(`Missing control: ${label}`);
  return control;
}

describe("Neo4j consolidation settings", () => {
  let container: HTMLDivElement;

  beforeEach(() => {
    i18n.load("en", {});
    i18n.activate("en");
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    container = document.createElement("div");
    document.body.append(container);
  });

  afterEach(() => {
    container.remove();
  });

  it("saves consolidation reasoning and output tokens without a chat setting", async () => {
    const updates: unknown[] = [];
    const onUpdateExtraction = vi.fn(async (input: unknown) => {
      updates.push(input);
      return true;
    });
    const root = createRoot(container);
    await act(async () => {
      root.render(
        <Neo4jExtractionSettings
          config={config({})}
          busy={false}
          onUpdateExtraction={onUpdateExtraction}
        />,
      );
    });

    const thinking = labeled<HTMLSelectElement>(container, "Consolidation reasoning", "select");
    const tokens = labeled<HTMLInputElement>(container, "Consolidation output tokens", "input");
    const dream = container.querySelector<HTMLInputElement>("[data-testid=dream-mode-toggle]");
    expect(dream?.checked).toBe(false);
    expect(thinking.value).toBe("low");
    expect(tokens.value).toBe("");
    const timeout = labeled<HTMLInputElement>(container, "Consolidation timeout (ms)", "input");
    expect(tokens.placeholder).toBe("32768");
    expect(timeout.placeholder).toBe("180000");
    expect(timeout.value).toBe("");
    expect([...thinking.options].map((option) => option.value)).toEqual([
      "off",
      "minimal",
      "low",
      "medium",
      "high",
      "xhigh",
      "max",
    ]);

    const setter = Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, "value")?.set;
    await act(async () => {
      setter?.call(thinking, "minimal");
      thinking.dispatchEvent(new Event("change", { bubbles: true }));
    });
    expect(updates.at(-1)).toMatchObject({
      dream: false,
      consolidateThinkingLevel: "minimal",
      consolidateMaxOutputTokens: "",
    });

    await act(async () => {
      setter?.call(thinking, "low");
      thinking.dispatchEvent(new Event("change", { bubbles: true }));
    });
    expect(updates.at(-1)).toMatchObject({ consolidateThinkingLevel: "" });

    const budget = labeled<HTMLInputElement>(container, "Consolidation output tokens", "input");
    const inputSetter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")?.set;
    await act(async () => {
      budget.focus();
      inputSetter?.call(budget, "2048");
      budget.dispatchEvent(new Event("input", { bubbles: true }));
    });
    expect(budget.value).toBe("2048");
    await act(async () => {
      budget.blur();
    });
    expect(updates.at(-1)).toMatchObject({
      dream: false,
      consolidateThinkingLevel: "",
      consolidateMaxOutputTokens: "2048",
      consolidateTimeoutMs: "",
    });

    const timeoutInput = labeled<HTMLInputElement>(
      container,
      "Consolidation timeout (ms)",
      "input",
    );
    await act(async () => {
      timeoutInput.focus();
      inputSetter?.call(timeoutInput, "180000");
      timeoutInput.dispatchEvent(new Event("input", { bubbles: true }));
    });
    await act(async () => {
      timeoutInput.blur();
    });
    expect(updates.at(-1)).toMatchObject({
      consolidateTimeoutMs: "180000",
      consolidateMaxOutputTokens: "2048",
    });

    act(() => root.unmount());
  });

  it("shows a stored consolidation budget", async () => {
    const root = createRoot(container);
    await act(async () => {
      root.render(
        <Neo4jExtractionSettings
          config={config({
            consolidateThinkingLevel: "high",
            consolidateMaxOutputTokens: "2048",
          })}
          busy={false}
          onUpdateExtraction={async () => true}
        />,
      );
    });
    expect(labeled<HTMLSelectElement>(container, "Consolidation reasoning", "select").value).toBe(
      "high",
    );
    expect(labeled<HTMLInputElement>(container, "Consolidation output tokens", "input").value).toBe(
      "2048",
    );
    act(() => root.unmount());
  });
});
