import { spawnSync } from "node:child_process";
import { constants } from "node:fs";
import {
  chmod,
  copyFile,
  lstat,
  mkdir,
  mkdtemp,
  open,
  rename,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { assertComputerHomeWritable, assertOpenedDirectoryBeneathRoot } from "./home-ownership.js";

const ownershipModule = fileURLToPath(new URL("./home-ownership.ts", import.meta.url));

function otherUserCanRun(): boolean {
  if (process.platform !== "linux") return false;
  const probe =
    process.getuid?.() === 0
      ? spawnSync(process.execPath, ["-e", "process.exit(0)"], { uid: 65534, gid: 65534 })
      : spawnSync("sudo", ["-n", "-u", "nobody", process.execPath, "-e", "process.exit(0)"]);
  return probe.status === 0;
}

async function checkHomeAsOtherUser(
  home: string,
  uid: number,
  gid: number,
): Promise<{ status: number; output: string } | undefined> {
  // The checkout may live under a home directory other users cannot traverse.
  // The checker only needs this module, which imports node builtins, so copy
  // it somewhere nobody can read.
  const scratch = await mkdtemp(path.join(tmpdir(), "rakazo-home-check-"));
  try {
    await chmod(scratch, 0o755);
    const modulePath = path.join(scratch, "home-ownership.ts");
    await copyFile(ownershipModule, modulePath);
    await chmod(modulePath, 0o644);
    const script = `
      import { assertComputerHomeWritable } from ${JSON.stringify(modulePath)};
      try {
        await assertComputerHomeWritable(process.argv[1], Number(process.argv[2]), Number(process.argv[3]));
        console.log("OK");
      } catch (error) {
        console.log(error instanceof Error ? error.message : String(error));
        process.exitCode = 2;
      }
    `;
    const nodeArgs = [
      "--experimental-strip-types",
      "--input-type=module",
      "-e",
      script,
      home,
      String(uid),
      String(gid),
    ];
    const env = { ...process.env, HOME: "/tmp", XDG_CACHE_HOME: "/tmp" };
    const result =
      process.getuid?.() === 0
        ? spawnSync(process.execPath, nodeArgs, { encoding: "utf8", env, uid: 65534, gid: 65534 })
        : spawnSync("sudo", ["-n", "-u", "nobody", process.execPath, ...nodeArgs], {
            encoding: "utf8",
            env,
          });
    if (result.error || result.status === null) return undefined;
    return { status: result.status, output: `${result.stdout ?? ""}\n${result.stderr ?? ""}` };
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}

const roots: string[] = [];
const DIRECTORY_OPEN_FLAGS = constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW;

afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("computer home ownership", () => {
  it("rejects a missing home instead of creating it as root", async () => {
    const parent = await mkdtemp(path.join(tmpdir(), "rakazo-home-missing-"));
    roots.push(parent);

    await expect(
      assertComputerHomeWritable(path.join(parent, "missing"), 1000, 1000),
    ).rejects.toThrow(/does not exist/);
  });

  it("rejects a symlink as the home root", async () => {
    const parent = await mkdtemp(path.join(tmpdir(), "rakazo-home-root-link-"));
    roots.push(parent);
    const outside = path.join(parent, "outside");
    const home = path.join(parent, "home");
    await mkdir(outside);
    await symlink(outside, home);

    await expect(assertComputerHomeWritable(home, 1000, 1000)).rejects.toThrow(/symbolic link/);
  });

  it("rejects a writable regular file as the home root", async () => {
    const parent = await mkdtemp(path.join(tmpdir(), "rakazo-home-file-root-"));
    roots.push(parent);
    const home = path.join(parent, "home");
    await writeFile(home, "{}");
    await chmod(home, 0o600);

    const stat = await lstat(home);
    await expect(assertComputerHomeWritable(home, stat.uid, stat.gid)).rejects.toThrow(
      /must be a directory/,
    );
  });

  it("rejects an existing entry that the host-run computer cannot write", async () => {
    const parent = await mkdtemp(path.join(tmpdir(), "rakazo-home-writable-"));
    roots.push(parent);
    const home = path.join(parent, "home");
    const file = path.join(home, "profile.json");
    await mkdir(home);
    await chmod(home, 0o777);
    await writeFile(file, "{}");
    await chmod(file, 0o400);

    const stat = await lstat(home);
    await expect(assertComputerHomeWritable(home, stat.uid + 1, stat.gid + 1)).rejects.toThrow(
      /chown -R/,
    );
  });

  it("accepts writable files owned by the sandbox user", async () => {
    const parent = await mkdtemp(path.join(tmpdir(), "rakazo-home-writable-file-"));
    roots.push(parent);
    const home = path.join(parent, "home");
    const file = path.join(home, "profile.json");
    await mkdir(home);
    await writeFile(file, "{}");
    await chmod(file, 0o644);

    const stat = await lstat(file);
    await expect(assertComputerHomeWritable(home, stat.uid, stat.gid)).resolves.toBeUndefined();
  });

  it("accepts sandbox-owned read-only git object files", async () => {
    const parent = await mkdtemp(path.join(tmpdir(), "rakazo-home-git-object-"));
    roots.push(parent);
    const home = path.join(parent, "home");
    const object = path.join(home, "repo", ".git", "objects", "ab", "cdef");
    await mkdir(path.dirname(object), { recursive: true });
    await writeFile(object, "blob");
    await chmod(object, 0o444);

    const stat = await lstat(object);
    await expect(assertComputerHomeWritable(home, stat.uid, stat.gid)).resolves.toBeUndefined();
  });

  it("rejects a read-only file owned by someone else", async () => {
    const parent = await mkdtemp(path.join(tmpdir(), "rakazo-home-foreign-readonly-"));
    roots.push(parent);
    const home = path.join(parent, "home");
    const file = path.join(home, "object");
    await mkdir(home);
    await chmod(home, 0o777);
    await writeFile(file, "blob");
    await chmod(file, 0o444);

    const stat = await lstat(file);
    await expect(assertComputerHomeWritable(home, stat.uid + 1, stat.gid + 1)).rejects.toThrow(
      /chown -R/,
    );
  });

  it("rejects an owner-owned file the owner cannot read", async () => {
    const parent = await mkdtemp(path.join(tmpdir(), "rakazo-home-unreadable-"));
    roots.push(parent);
    const home = path.join(parent, "home");
    const file = path.join(home, "profile.json");
    await mkdir(home);
    await writeFile(file, "{}");
    await chmod(file, 0o000);

    const stat = await lstat(file);
    await expect(assertComputerHomeWritable(home, stat.uid, stat.gid)).rejects.toThrow(/chown -R/);
  });

  it("rejects an owner-owned world-writable file the owner cannot write", async () => {
    const parent = await mkdtemp(path.join(tmpdir(), "rakazo-home-world-writable-"));
    roots.push(parent);
    const home = path.join(parent, "home");
    const file = path.join(home, "profile.json");
    await mkdir(home);
    await writeFile(file, "{}");
    await chmod(file, 0o002);

    const stat = await lstat(file);
    await expect(assertComputerHomeWritable(home, stat.uid, stat.gid)).rejects.toThrow(/chown -R/);
  });

  it("rejects an owner-owned directory that is not writable", async () => {
    const parent = await mkdtemp(path.join(tmpdir(), "rakazo-home-dir-mode-"));
    roots.push(parent);
    const home = path.join(parent, "home");
    await mkdir(home);
    await chmod(home, 0o555);

    const stat = await lstat(home);
    await expect(assertComputerHomeWritable(home, stat.uid, stat.gid)).rejects.toThrow(/chown -R/);
  });

  it("does not follow symlinks while checking host-run compatibility", async () => {
    const parent = await mkdtemp(path.join(tmpdir(), "rakazo-home-writable-link-"));
    roots.push(parent);
    const home = path.join(parent, "home");
    const outside = path.join(parent, "outside");
    await mkdir(home);
    await writeFile(outside, "outside");
    await chmod(outside, 0o400);
    await symlink(outside, path.join(home, "link"));

    const stat = await lstat(home);
    await expect(assertComputerHomeWritable(home, stat.uid, stat.gid)).resolves.toBeUndefined();
  });

  it.skipIf(process.platform !== "linux")(
    "names an unreadable directory instead of a /proc/self/fd open error",
    async () => {
      const parent = await mkdtemp(path.join(tmpdir(), "rakazo-home-unreadable-dir-"));
      roots.push(parent);
      const home = path.join(parent, "home");
      const cache = path.join(home, ".cache");
      await mkdir(cache, { recursive: true });
      await chmod(cache, 0o000);

      const stat = await lstat(home);
      let message = "";
      try {
        await assertComputerHomeWritable(home, stat.uid, stat.gid);
      } catch (error) {
        message = error instanceof Error ? error.message : String(error);
      }
      expect(message).toMatch(/chown -R/);
      expect(message).toContain(".cache");
      expect(message).not.toContain("/proc/self/fd");
    },
  );

  it.skipIf(process.platform !== "linux" || !otherUserCanRun())(
    "accepts a private cache the computer user owns when the supervisor cannot enter it",
    async () => {
      const parent = await mkdtemp(path.join(tmpdir(), "rakazo-home-private-cache-"));
      roots.push(parent);
      const home = path.join(parent, "home");
      const cache = path.join(home, ".cache");
      await mkdir(path.join(cache, "uv"), { recursive: true });
      await writeFile(path.join(cache, "uv", "entry"), "cached");
      await chmod(parent, 0o755);
      await chmod(home, 0o777);
      await chmod(cache, 0o700);
      await chmod(path.join(cache, "uv"), 0o700);

      const stat = await lstat(cache);
      const accepted = await checkHomeAsOtherUser(home, stat.uid, stat.gid);
      expect(accepted?.status, accepted?.output).toBe(0);
      expect(accepted?.output).toContain("OK");

      const sticky = path.join(home, "sticky");
      await mkdir(sticky);
      await chmod(sticky, 0o1700);
      const rejected = await checkHomeAsOtherUser(home, stat.uid, stat.gid);
      expect(rejected?.status, rejected?.output).toBe(2);
      expect(rejected?.output).toMatch(/chown -R/);
      expect(rejected?.output).not.toContain("/proc/self/fd");

      await rm(sticky, { recursive: true, force: true });
      const foreign = await checkHomeAsOtherUser(home, stat.uid + 1, stat.gid + 1);
      expect(foreign?.status, foreign?.output).toBe(2);
      expect(foreign?.output).toMatch(/chown -R/);
      expect(foreign?.output).toContain(".cache");
    },
  );

  it.skipIf(process.platform !== "linux")(
    "rejects an opened directory that was moved outside the home",
    async () => {
      const parent = await mkdtemp(path.join(tmpdir(), "rakazo-home-moved-"));
      roots.push(parent);
      const home = path.join(parent, "home");
      const outside = path.join(parent, "outside");
      const child = path.join(home, "nested");
      await mkdir(child, { recursive: true });
      await mkdir(outside, { recursive: true });
      const handle = await open(child, DIRECTORY_OPEN_FLAGS);
      try {
        await rename(child, path.join(outside, "nested"));
        await expect(assertOpenedDirectoryBeneathRoot(handle, home)).rejects.toThrow(
          /escaped validated root/,
        );
      } finally {
        await handle.close();
      }
    },
  );
});
