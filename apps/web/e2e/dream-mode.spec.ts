import { expect, test } from "@playwright/test";
import { captureScreenshot, completeOnboarding, openUserSettings, signup } from "./helpers";

test("dream mode is a Space memory switch and stays off until turned on", async ({
  page,
}, testInfo) => {
  const config = {
    provider: "neo4j",
    settings: {
      uri: "bolt://127.0.0.1:7687",
      username: "neo4j",
      database: "neo4j",
      extract: "true",
    },
    defaultMemoryScope: "isolated" as const,
    updatedAt: "2026-10-19T00:00:00.000Z",
  };
  await page.route("**/rpc/memory/providerConfig", (route) =>
    route.fulfill({ json: { json: config } }),
  );
  await page.route("**/rpc/memory/updateNeo4jExtraction", async (route) => {
    const body = route.request().postDataJSON() as {
      json?: { dream?: boolean; extract?: boolean };
    };
    const input = body.json ?? {};
    await route.fulfill({
      json: {
        json: {
          ...config,
          settings: {
            ...config.settings,
            extract: input.extract === false ? "false" : "true",
            ...(input.dream === undefined ? {} : { dream: input.dream ? "true" : "false" }),
          },
          updatedAt: "2026-10-19T03:00:00.000Z",
        },
      },
    });
  });

  const stamp = Date.now();
  await signup(page, `dream-mode-${stamp}@rakazo.test`, "password12", "Dream mode");
  await completeOnboarding(page);
  const settings = await openUserSettings(page, "memory");
  const memory = settings.getByTestId("memory-settings");
  const dream = memory.getByRole("switch", { name: "Dream mode" });
  await expect(dream).toBeVisible();
  await expect(dream).not.toBeChecked();
  await expect(memory.getByRole("switch", { name: "Extract entities" })).toBeChecked();
  await expect(memory.getByText("Consolidation reasoning")).toBeVisible();
  await expect(memory.getByText("Consolidation output tokens")).toBeVisible();

  const update = page.waitForRequest("**/rpc/memory/updateNeo4jExtraction");
  await dream.click();
  const request = await update;
  const sent = request.postDataJSON() as { json?: { dream?: boolean; extract?: boolean } };
  expect(sent.json).toMatchObject({ dream: true, extract: true });
  await expect(dream).toBeChecked();
  await dream.scrollIntoViewIfNeeded();
  await captureScreenshot(page, testInfo, "dream-mode-on");
});
