import { describe, expect, it } from "vitest";
import { loadWorkerConfig } from "./config.js";

describe("loadWorkerConfig", () => {
  it("uses DATABASE_URL from the caller environment", () => {
    const config = loadWorkerConfig({
      DATABASE_URL: " postgres://rakazo:secret@postgres:5432/rakazo ",
      DB_POOL_MAX: "4",
      NODE_ENV: "production",
      ENCRYPTION_KEY: "worker-encryption-key",
    });
    expect(config.databaseUrl).toBe("postgres://rakazo:secret@postgres:5432/rakazo");
    expect(config.realtimeDatabaseUrl).toBe(config.databaseUrl);
    expect(config.env.DB_POOL_MAX).toBe("4");
    expect(config.env.ENCRYPTION_KEY).toBe("worker-encryption-key");
    expect(config.env.NODE_ENV).toBe("production");
  });

  it("reads DATABASE_URL_FILE and a separate realtime URL", () => {
    const config = loadWorkerConfig(
      {
        DATABASE_URL_FILE: "/secrets/db",
        REALTIME_DATABASE_URL: "postgres://rakazo:secret@postgres:5432/realtime",
        ENCRYPTION_KEY_FILE: "/secrets/encryption",
      },
      {
        readFile: (path) => {
          if (path === "/secrets/db") return "postgres://rakazo:secret@postgres:5432/rakazo\n";
          if (path === "/secrets/encryption") return "file-encryption-key\n";
          throw new Error(`unexpected ${path}`);
        },
      },
    );
    expect(config.databaseUrl).toBe("postgres://rakazo:secret@postgres:5432/rakazo");
    expect(config.realtimeDatabaseUrl).toBe("postgres://rakazo:secret@postgres:5432/realtime");
    expect(config.env.DATABASE_URL).toBe("postgres://rakazo:secret@postgres:5432/rakazo");
    expect(config.env.ENCRYPTION_KEY).toBe("file-encryption-key");
  });

  it("builds a URL from POSTGRES_* or libpq parts", () => {
    const fromPostgres = loadWorkerConfig({
      POSTGRES_HOST: "postgres",
      POSTGRES_USER: "rakazo",
      POSTGRES_PASSWORD: "p@ss/word",
      POSTGRES_DB: "rakazo",
    });
    expect(fromPostgres.databaseUrl).toBe("postgres://rakazo:p%40ss%2Fword@postgres:5432/rakazo");

    const fromLibpq = loadWorkerConfig(
      {
        PGHOST: "db.internal",
        PGPORT: "5433",
        PGUSER: "rakazo",
        PGPASSWORD_FILE: "/secrets/pg",
        PGDATABASE: "rakazo",
      },
      {
        readFile: () => "libpq-secret",
      },
    );
    expect(fromLibpq.databaseUrl).toBe("postgres://rakazo:libpq-secret@db.internal:5433/rakazo");
    expect(fromLibpq.env.PGPASSWORD).toBe("libpq-secret");
  });

  it("names DATABASE_URL when no database configuration is available", () => {
    expect(() => loadWorkerConfig({ POSTGRES_USER: "rakazo" })).toThrow(/DATABASE_URL/);
    expect(() => loadWorkerConfig({ POSTGRES_PORT: "nope", POSTGRES_HOST: "postgres" })).toThrow(
      /DATABASE_URL/,
    );
  });
});
