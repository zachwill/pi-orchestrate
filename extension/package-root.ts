import { fileURLToPath } from "node:url";

export const PACKAGE_ROOT = fileURLToPath(new URL("..", import.meta.url));
