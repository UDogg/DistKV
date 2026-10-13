# DistKV live dashboard 📊

TypeScript with no framework and no runtime dependencies, bundled by esbuild into
`internal/ui/dist` (~33 KB) and embedded in every Go binary.

| Where it runs | URL | Mode |
|---|---|---|
| `make demo` | http://localhost:8080 | **live + chaos controls + SQLite history** |
| any node | http://NODE:7001/ui/ | live (read-only) |
| any node + historian | `/ui/?historian=http://HOST:8090` | live + history |
| another origin | `?api=http://NODE:7001` (node needs `--cors-origin`) | live |
| GitHub Pages / `?sim` / `file://` | — | **in-browser simulation** |

Data arrives over **Server-Sent Events** (`/v1/events`). The simulation backend uses the real
routing code, so shard placement, preferred leaders and key lookups match a real cluster.

```bash
npm ci
npm run build      # type-check (strict) + bundle into internal/ui/dist, then rebuild Go
npm run watch      # rebuild on change
npm run pages      # simulation build into ../../dist-pages
```
Commit `internal/ui/dist` after changes. CI fails if it's stale.
