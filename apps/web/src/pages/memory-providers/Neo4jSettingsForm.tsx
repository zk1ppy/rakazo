import { Trans, useLingui } from "@lingui/react/macro";
import type { SpaceMemoryConfig, ThinkingLevel } from "@rakazo/contracts";
import {
  Button,
  Field,
  FieldError,
  FieldLabel,
  Input,
  Label,
  NativeSelect,
  NativeSelectOption,
  Switch,
} from "@rakazo/ui-web";
import { useEffect, useId, useState } from "react";
import {
  connectedModelOptions,
  modelOptionKey,
  parseModelOptionKey,
  thinkingLevelLabel,
} from "../../lib/model-catalog";
import { rpc } from "../../lib/rpc";
import type { Neo4jSettingsFieldError } from "./neo4j-settings";
import { NEO4J_URI_PLACEHOLDER, neo4jConnectionDraft, neo4jExtractEnabled } from "./neo4j-settings";
import type { MemoryProviderSettingsFormProps, Neo4jExtractionUpdate } from "./registry";

const CONSOLIDATE_THINKING_LEVELS: ThinkingLevel[] = [
  "off",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
];

function useExtractionModelChoices() {
  const [choices, setChoices] = useState<ReturnType<typeof connectedModelOptions>>([]);
  useEffect(() => {
    let cancelled = false;
    void Promise.all([rpc.models.credentials(), rpc.models.list()])
      .then(([credentials, catalog]) => {
        if (!cancelled) setChoices(connectedModelOptions(credentials, catalog));
      })
      .catch(() => {
        if (!cancelled) setChoices([]);
      });
    return () => {
      cancelled = true;
    };
  }, []);
  return choices;
}

function extractionModelKey(settings: Record<string, string>): string {
  const provider = settings.extractModelProvider?.trim() ?? "";
  const id = settings.extractModelId?.trim() ?? "";
  if (!provider || !id) return "";
  return modelOptionKey(provider, id);
}

function extractionModelPatch(
  key: string,
): Pick<Neo4jExtractionUpdate, "extractModelProvider" | "extractModelId"> {
  const parsed = parseModelOptionKey(key);
  return {
    extractModelProvider: parsed?.provider ?? "",
    extractModelId: parsed?.modelId ?? "",
  };
}

function ExtractionModelSelect({
  id,
  value,
  disabled,
  choices,
  onChange,
}: {
  id: string;
  value: string;
  disabled: boolean;
  choices: ReturnType<typeof connectedModelOptions>;
  onChange: (value: string) => void;
}) {
  const { t } = useLingui();
  const known = choices.some((choice) => choice.key === value);
  return (
    <Field className="mt-4">
      <FieldLabel htmlFor={id}>
        <Trans>Extraction model</Trans>
      </FieldLabel>
      <NativeSelect
        id={id}
        className="w-full"
        value={value}
        disabled={disabled}
        onChange={(event) => onChange(event.target.value)}
      >
        <NativeSelectOption value="">{t`Space default`}</NativeSelectOption>
        {value && !known ? <NativeSelectOption value={value}>{value}</NativeSelectOption> : null}
        {choices.map((choice) => (
          <NativeSelectOption key={choice.key} value={choice.key}>
            {choice.label}
          </NativeSelectOption>
        ))}
      </NativeSelect>
    </Field>
  );
}

export function Neo4jSettingsForm({ busy, onConnect }: MemoryProviderSettingsFormProps) {
  const uriId = useId();
  const uriErrorId = useId();
  const databaseId = useId();
  const usernameId = useId();
  const passwordId = useId();
  const [uri, setUri] = useState("");
  const [uriError, setUriError] = useState<Neo4jSettingsFieldError | null>(null);
  const [database, setDatabase] = useState("");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [extract, setExtract] = useState(true);
  const [chunkSize, setChunkSize] = useState("");
  const [modelKey, setModelKey] = useState("");
  const extractId = useId();
  const chunkId = useId();
  const modelSelectId = useId();
  const modelChoices = useExtractionModelChoices();
  const selectedModel = parseModelOptionKey(modelKey);

  async function connect() {
    const result = neo4jConnectionDraft({
      uri,
      database,
      username,
      password,
      extract,
      maxChunkChars: extract ? chunkSize : "",
      ...(extract && selectedModel
        ? {
            extractModelProvider: selectedModel.provider,
            extractModelId: selectedModel.modelId,
          }
        : {}),
    });
    if (!result.ok) {
      setUriError(result.error);
      return;
    }
    if (!username.trim() || !password) return;
    setUriError(null);
    if (await onConnect(result.draft)) setPassword("");
  }

  const incomplete = !uri.trim() || !username.trim() || !password;

  return (
    <>
      <Field className="mt-1" data-invalid={uriError ? true : undefined}>
        <FieldLabel htmlFor={uriId}>
          <Trans>URI</Trans>
        </FieldLabel>
        <Input
          id={uriId}
          value={uri}
          required
          disabled={busy}
          aria-invalid={uriError ? true : undefined}
          aria-describedby={uriError ? uriErrorId : undefined}
          onChange={(event) => {
            setUri(event.target.value);
            setUriError(null);
          }}
          placeholder={NEO4J_URI_PLACEHOLDER}
          autoComplete="off"
        />
        {uriError === "required" ? (
          <FieldError id={uriErrorId}>
            <Trans>Neo4j URI is required.</Trans>
          </FieldError>
        ) : null}
      </Field>

      <Field className="mt-4">
        <FieldLabel htmlFor={databaseId}>
          <Trans>Database</Trans>
        </FieldLabel>
        <Input
          id={databaseId}
          value={database}
          disabled={busy}
          onChange={(event) => setDatabase(event.target.value)}
          placeholder="neo4j"
          autoComplete="off"
        />
      </Field>

      <Field className="mt-4">
        <FieldLabel htmlFor={usernameId}>
          <Trans>Username</Trans>
        </FieldLabel>
        <Input
          id={usernameId}
          value={username}
          disabled={busy}
          onChange={(event) => setUsername(event.target.value)}
          placeholder="neo4j"
          autoComplete="off"
        />
      </Field>

      <Field className="mt-4">
        <FieldLabel htmlFor={passwordId}>
          <Trans>Password</Trans>
        </FieldLabel>
        <Input
          id={passwordId}
          value={password}
          disabled={busy}
          onChange={(event) => setPassword(event.target.value)}
          type="password"
          autoComplete="new-password"
        />
      </Field>

      <div className="mt-4 flex items-start gap-3">
        <Switch
          id={extractId}
          className="mt-0.5"
          checked={extract}
          disabled={busy}
          onCheckedChange={setExtract}
        />
        <Label htmlFor={extractId} className="text-[14px] font-normal text-foreground/75">
          <Trans>Extract entities</Trans>
        </Label>
      </div>

      {extract ? (
        <Field className="mt-4">
          <FieldLabel htmlFor={chunkId}>
            <Trans>Chunk size</Trans>
          </FieldLabel>
          <Input
            id={chunkId}
            value={chunkSize}
            disabled={busy}
            inputMode="numeric"
            onChange={(event) => setChunkSize(event.target.value)}
            placeholder="4000"
            autoComplete="off"
          />
        </Field>
      ) : null}
      {extract ? (
        <ExtractionModelSelect
          id={modelSelectId}
          value={modelKey}
          disabled={busy}
          choices={modelChoices}
          onChange={setModelKey}
        />
      ) : null}

      <Button
        type="button"
        variant="secondary"
        className="mt-5 rounded-full"
        size="sm"
        disabled={busy || incomplete}
        onClick={() => void connect()}
      >
        {busy ? <Trans>Connecting…</Trans> : <Trans>Connect</Trans>}
      </Button>
    </>
  );
}

export function Neo4jExtractionSettings({
  config,
  busy,
  onUpdateExtraction,
}: {
  config: SpaceMemoryConfig;
  busy: boolean;
  onUpdateExtraction: (input: Neo4jExtractionUpdate) => Promise<boolean>;
}) {
  const extractId = useId();
  const chunkId = useId();
  const modelSelectId = useId();
  const modelChoices = useExtractionModelChoices();
  const [extract, setExtract] = useState(() => neo4jExtractEnabled(config.settings));
  const [chunkSize, setChunkSize] = useState(config.settings.maxChunkChars ?? "");
  const [modelKey, setModelKey] = useState(() => extractionModelKey(config.settings));
  const [consolidateThinking, setConsolidateThinking] = useState(
    config.settings.consolidateThinkingLevel || "low",
  );
  const [consolidateMaxTokens, setConsolidateMaxTokens] = useState(
    config.settings.consolidateMaxOutputTokens ?? "",
  );
  const thinkingId = useId();
  const tokensId = useId();

  useEffect(() => {
    setExtract(neo4jExtractEnabled(config.settings));
    setChunkSize(config.settings.maxChunkChars ?? "");
    setModelKey(extractionModelKey(config.settings));
    setConsolidateThinking(config.settings.consolidateThinkingLevel || "low");
    setConsolidateMaxTokens(config.settings.consolidateMaxOutputTokens ?? "");
  }, [config.settings]);

  async function save(
    nextExtract: boolean,
    nextChunk: string,
    nextModel: string,
    nextThinking = consolidateThinking,
    nextMaxTokens = consolidateMaxTokens,
  ) {
    return onUpdateExtraction({
      extract: nextExtract,
      maxChunkChars: nextChunk,
      ...extractionModelPatch(nextModel),
      consolidateThinkingLevel: nextThinking === "low" ? "" : nextThinking,
      consolidateMaxOutputTokens: nextMaxTokens.trim(),
    });
  }

  return (
    <div className="mt-4">
      <div className="flex items-start gap-3">
        <Switch
          id={extractId}
          className="mt-0.5"
          checked={extract}
          disabled={busy}
          onCheckedChange={(checked) => {
            const previous = extract;
            setExtract(checked);
            void save(checked, chunkSize, modelKey).then((ok) => {
              if (!ok) setExtract(previous);
            });
          }}
        />
        <Label htmlFor={extractId} className="text-[14px] font-normal text-foreground/75">
          <Trans>Extract entities</Trans>
        </Label>
      </div>
      {extract ? (
        <Field className="mt-4">
          <FieldLabel htmlFor={chunkId}>
            <Trans>Chunk size</Trans>
          </FieldLabel>
          <Input
            id={chunkId}
            value={chunkSize}
            disabled={busy}
            inputMode="numeric"
            onChange={(event) => setChunkSize(event.target.value)}
            onBlur={() => {
              if (chunkSize.trim() === (config.settings.maxChunkChars ?? "")) return;
              void save(extract, chunkSize, modelKey).then((ok) => {
                if (!ok) setChunkSize(config.settings.maxChunkChars ?? "");
              });
            }}
            placeholder="4000"
            autoComplete="off"
          />
        </Field>
      ) : null}
      {extract ? (
        <ExtractionModelSelect
          id={modelSelectId}
          value={modelKey}
          disabled={busy}
          choices={modelChoices}
          onChange={(next) => {
            const previous = modelKey;
            setModelKey(next);
            void save(extract, chunkSize, next).then((ok) => {
              if (!ok) setModelKey(previous);
            });
          }}
        />
      ) : null}
      <Field className="mt-4">
        <FieldLabel htmlFor={thinkingId}>
          <Trans>Consolidation reasoning</Trans>
        </FieldLabel>
        <NativeSelect
          id={thinkingId}
          className="w-full"
          value={consolidateThinking}
          disabled={busy}
          onChange={(event) => {
            const next = event.target.value;
            const previous = consolidateThinking;
            setConsolidateThinking(next);
            void save(extract, chunkSize, modelKey, next, consolidateMaxTokens).then((ok) => {
              if (!ok) setConsolidateThinking(previous);
            });
          }}
        >
          {CONSOLIDATE_THINKING_LEVELS.map((level) => (
            <NativeSelectOption key={level} value={level}>
              {thinkingLevelLabel(level)}
            </NativeSelectOption>
          ))}
        </NativeSelect>
      </Field>
      <Field className="mt-4">
        <FieldLabel htmlFor={tokensId}>
          <Trans>Consolidation output tokens</Trans>
        </FieldLabel>
        <Input
          id={tokensId}
          value={consolidateMaxTokens}
          disabled={busy}
          inputMode="numeric"
          onChange={(event) => setConsolidateMaxTokens(event.target.value)}
          onBlur={() => {
            if (
              consolidateMaxTokens.trim() === (config.settings.consolidateMaxOutputTokens ?? "")
            ) {
              return;
            }
            void save(extract, chunkSize, modelKey).then((ok) => {
              if (!ok) setConsolidateMaxTokens(config.settings.consolidateMaxOutputTokens ?? "");
            });
          }}
          placeholder="1024"
          autoComplete="off"
        />
      </Field>
    </div>
  );
}
