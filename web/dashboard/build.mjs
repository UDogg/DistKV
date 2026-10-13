// Bundles the dashboard into internal/ui/dist (embedded by Go), or into a
// standalone simulation build for GitHub Pages (--sim --out <dir>).
import * as esbuild from "esbuild";
import { cpSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
const outIdx = args.indexOf("--out");
const out = resolve(here, outIdx >= 0 ? args[outIdx + 1] : "../../internal/ui/dist");
const sim = args.includes("--sim");
const watch = args.includes("--watch");

mkdirSync(out, { recursive: true });
for (const f of ["styles.css", "favicon.svg"]) cpSync(resolve(here, "static", f), resolve(out, f));
let html = readFileSync(resolve(here, "static/index.html"), "utf8");
if (sim) html = html.replace('<meta name="distkv-mode" content="auto">', '<meta name="distkv-mode" content="sim">');
writeFileSync(resolve(out, "index.html"), html);

const opts = {
  entryPoints: [resolve(here, "src/main.ts")],
  bundle: true,
  format: "esm",
  target: "es2022",
  minify: !watch,
  sourcemap: false,
  legalComments: "none",
  outfile: resolve(out, "app.js"),
  logLevel: "info",
};
if (watch) {
  const ctx = await esbuild.context(opts);
  await ctx.watch();
} else {
  await esbuild.build(opts);
}
