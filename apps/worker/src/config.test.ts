import { describe, expect, it, vi } from "vitest";
import { loadWorkerConfig } from "./config.js";

const none = () => [];

describe("loadWorkerConfig", () => {
  it("uses DATABASE_URL and does not read sibling processes", () => {
    const processEnvirons = vi.fn(none);
    const config = loadWorkerConfig(
      {
        DATABASE_URL: " postgres://rakazo:secret@postgres:5432/rakazo ",
        DB_POOL_MAX: "4",
        NODE_ENV: "production",
      },
      { processEnvirons },
    );
    expect(config.databaseUrl).toBe("postgres://rakazo:secret@postgres:5432/rakazo");
    expect(config.realtimeDatabaseUrl).toBe(config.databaseUrl);
    expect(config.env.DB_POOL_MAX).toBe("4");
    expect(processEnvirons).not.toHaveBeenCalled();
  });

  it("reads DATABASE_URL_FILE and a separate realtime URL", () => {
    const config = loadWorkerConfig(
      {
        DATABASE_URL_FILE: "/secrets/db",
        REALTIME_DATABASE_URL: "postgres://rakazo:secret@postgres:5432/realtime",
      },
      {
        processEnvirons: none,
        readFile: (path) => {
          if (path === "/secrets/db") return "postgres://rakazo:secret@postgres:5432/rakazo\n";
          throw new Error(`unexpected ${path}`);
        },
      },
    );
    expect(config.databaseUrl).toBe("postgres://rakazo:secret@postgres:5432/rakazo");
    expect(config.realtimeDatabaseUrl).toBe("postgres://rakazo:secret@postgres:5432/realtime");
    expect(config.env.DATABASE_URL).toBe("postgres://rakazo:secret@postgres:5432/rakazo");
  });

  it("builds a URL from POSTGRES_* or libpq parts", () => {
    const fromPostgres = loadWorkerConfig(
      {
        POSTGRES_HOST: "postgres",
        POSTGRES_USER: "rakazo",
        POSTGRES_PASSWORD: "p@ss/word",
        POSTGRES_DB: "rakazo",
      },
      { processEnvirons: none },
    );
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
        processEnvirons: none,
        readFile: () => "libpq-secret",
      },
    );
    expect(fromLibpq.databaseUrl).toBe("postgres://rakazo:libpq-secret@db.internal:5433/rakazo");
    expect(fromLibpq.env.PGPASSWORD).toBe("libpq-secret");
  });

  it("takes the database URL and Neo4j secret key from the running worker", () => {
    const config = loadWorkerConfig(
      { NODE_ENV: "production" },
      {
        processEnvirons: () => [
          {
            DATABASE_URL: "postgres://rakazo:secret@postgres:5432/rakazo",
            ENCRYPTION_KEY: "worker-encryption-key",
            SECRET_STORE: "encrypted",
          },
        ],
      },
    );
    expect(config.databaseUrl).toBe("postgres://rakazo:secret@postgres:5432/rakazo");
    expect(config.env.ENCRYPTION_KEY).toBe("worker-encryption-key");
    expect(config.env.SECRET_STORE).toBe("encrypted");
    expect(config.env.NODE_ENV).toBe("production");
  });

  it("names DATABASE_URL when no database configuration is available", () => {
    expect(() => loadWorkerConfig({ POSTGRES_USER: "rakazo" }, { processEnvirons: none })).toThrow(
      /DATABASE_URL/,
    );
    expect(() =>
      loadWorkerConfig(
        { POSTGRES_PORT: "nope", POSTGRES_HOST: "postgres" },
        { processEnvirons: none },
      ),
    ).toThrow(/DATABASE_URL/);
  });
});
