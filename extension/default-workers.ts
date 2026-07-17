import * as fs from "node:fs";
import * as path from "node:path";
import { getAgentDir } from "@mariozechner/pi-coding-agent";

interface DefaultWorkerInstallState {
  installedAt: string;
  examplesDir: string;
  targetDir: string;
  copied: string[];
  skippedExisting: string[];
}

const defaultWorkerNames = ["investigator.md", "scout.md", "worker.md"];

function readState(statePath: string): DefaultWorkerInstallState | undefined {
  if (!fs.existsSync(statePath)) return undefined;

  try {
    return JSON.parse(fs.readFileSync(statePath, "utf8")) as DefaultWorkerInstallState;
  } catch {
    return undefined;
  }
}

export function installDefaultWorkersOnce(extensionDir: string): string[] {
  const notes: string[] = [];
  const namespaceDir = path.join(getAgentDir(), "pi-workers");
  const targetDir = path.join(namespaceDir, "agents");
  const statePath = path.join(namespaceDir, "default-workers.json");

  if (readState(statePath)) return notes;

  const examplesDir = path.resolve(extensionDir, "..", "examples", "agents");
  if (!fs.existsSync(examplesDir)) return [`Default worker examples directory not found: ${examplesDir}`];

  fs.mkdirSync(targetDir, { recursive: true });

  const copied: string[] = [];
  const skippedExisting: string[] = [];

  for (const fileName of defaultWorkerNames) {
    const sourcePath = path.join(examplesDir, fileName);
    const targetPath = path.join(targetDir, fileName);

    if (!fs.existsSync(sourcePath)) {
      notes.push(`Default worker example missing: ${sourcePath}`);
      continue;
    }

    if (fs.existsSync(targetPath)) {
      skippedExisting.push(fileName);
      continue;
    }

    fs.copyFileSync(sourcePath, targetPath);
    copied.push(fileName);
  }

  const state: DefaultWorkerInstallState = {
    installedAt: new Date().toISOString(),
    examplesDir,
    targetDir,
    copied,
    skippedExisting,
  };

  fs.writeFileSync(statePath, `${JSON.stringify(state, null, 2)}\n`);

  if (copied.length > 0) {
    notes.push(`Installed default pi-workers into ${targetDir}: ${copied.join(", ")}`);
  }

  if (skippedExisting.length > 0) {
    notes.push(`Skipped existing default worker files in ${targetDir}: ${skippedExisting.join(", ")}`);
  }

  return notes;
}
