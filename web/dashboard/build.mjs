// Bundles the dashboard into internal/ui/dist (embedded by Go), or into a
// standalone simulation build for GitHub Pages (--sim --out <dir>).
import * as esbuild from "esbuild";
import { cpSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
const outIdx = args.indexOf("--out");
const out = resolve(here, outIdx >= 0 ? args[outIdx + 1] : "../../internal/ui/dist");
const sim = args.includes("--sim");
const watch = args.includes("--watch");

mkdirSync(out, { recursive: true });
cpSync(resolve(here, "static/favicon.svg"), resolve(out, "favicon.svg"));
// Minified CSS (a third smaller); the watch build keeps it readable.
await esbuild.build({ entryPoints: [resolve(here, "static/styles.css")], outfile: resolve(out, "styles.css"), minify: !watch, logLevel: "warning" });
const html = readFileSync(resolve(here, "static/index.html"), "utf8");
// Go serves the embedded build with ETags; the Pages build is written after
// the bundle (below), with cache-busting asset URLs.
if (!sim) writeFileSync(resolve(out, "index.html"), html);

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
if (sim) writePages();

// GitHub Pages serves every file with a 10-minute max-age and no way to set
// headers. Versioned asset URLs (?v=<content hash>) make a deploy take effect
// at once, with no stale app.js paired with a new index.html, while unchanged
// assets stay cached across deploys.
function writePages() {
  const v = (f) => `${f}?v=${createHash("sha256").update(readFileSync(resolve(out, f))).digest("hex").slice(0, 10)}`;
  let page = html.replace('<meta name="distkv-mode" content="auto">', '<meta name="distkv-mode" content="sim">');
  for (const f of ["styles.css", "app.js", "favicon.svg"]) {
    page = page.replaceAll(`"${f}"`, `"${v(f)}"`);
  }
  writeFileSync(resolve(out, "index.html"), page);
  // Unknown project paths land on the demo instead of GitHub's generic 404.
  writeFileSync(
    resolve(out, "404.html"),
    `<!doctype html><meta charset="utf-8"><title>DistKV</title><script>location.replace(location.pathname.split("/").slice(0, 2).join("/") + "/")</script>\n`,
  );
  // The site is plain files: skip Jekyll processing.
  writeFileSync(resolve(out, ".nojekyll"), "");
}
