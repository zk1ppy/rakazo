import { Trans } from "@lingui/react/macro";
import { Button, Field, FieldError, FieldLabel, Input } from "@rakazo/ui-web";
import { useId, useState } from "react";
import type { Neo4jSettingsFieldError } from "./neo4j-settings";
import { NEO4J_URI_PLACEHOLDER, neo4jConnectionDraft } from "./neo4j-settings";
import type { MemoryProviderSettingsFormProps } from "./registry";

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

  async function connect() {
    const result = neo4jConnectionDraft({ uri, database, username, password });
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
