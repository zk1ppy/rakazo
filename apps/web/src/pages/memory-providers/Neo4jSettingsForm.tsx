import { Trans } from "@lingui/react/macro";
import type { SpaceMemoryConfig } from "@rakazo/contracts";
import { Button, Field, FieldError, FieldLabel, Input, Label, Switch } from "@rakazo/ui-web";
import { useEffect, useId, useState } from "react";
import type { Neo4jSettingsFieldError } from "./neo4j-settings";
import { NEO4J_URI_PLACEHOLDER, neo4jConnectionDraft, neo4jExtractEnabled } from "./neo4j-settings";
import type { MemoryProviderSettingsFormProps, Neo4jExtractionUpdate } from "./registry";

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
  const extractId = useId();
  const chunkId = useId();

  async function connect() {
    const result = neo4jConnectionDraft({
      uri,
      database,
      username,
      password,
      extract,
      maxChunkChars: extract ? chunkSize : "",
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
  const [extract, setExtract] = useState(() => neo4jExtractEnabled(config.settings));
  const [chunkSize, setChunkSize] = useState(config.settings.maxChunkChars ?? "");

  useEffect(() => {
    setExtract(neo4jExtractEnabled(config.settings));
    setChunkSize(config.settings.maxChunkChars ?? "");
  }, [config.settings]);

  async function save(nextExtract: boolean, nextChunk: string) {
    return onUpdateExtraction({ extract: nextExtract, maxChunkChars: nextChunk });
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
            void save(checked, chunkSize).then((ok) => {
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
              void save(extract, chunkSize).then((ok) => {
                if (!ok) setChunkSize(config.settings.maxChunkChars ?? "");
              });
            }}
            placeholder="4000"
            autoComplete="off"
          />
        </Field>
      ) : null}
    </div>
  );
}
