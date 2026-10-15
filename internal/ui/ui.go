// Package ui embeds the DistKV live dashboard (built from web/dashboard,
// written in TypeScript) so that every node serves it at /ui/ with no
// extra deployment step.
package ui

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
)

//go:embed dist
var dist embed.FS

// FS returns the dashboard's static files.
func FS() fs.FS {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		panic(err)
	}
	return sub
}

type asset struct {
	raw, gz []byte
	etag    string // of raw; the gzip variant's is distinct (RFC 9110 §8.8.3)
	gzEtag  string
	ctype   string
}

// assets are read, hashed and gzipped once, on first use: about 160 KB of
// dashboard becomes about 50 KB on the wire, and a reload costs a 304.
var assets = sync.OnceValue(func() map[string]*asset {
	m := map[string]*asset{}
	// The files are embedded in the binary: failing to read one is a build
	// defect, not a runtime condition, so fail loudly.
	err := fs.WalkDir(FS(), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		raw, err := fs.ReadFile(FS(), p)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(raw)
		a := &asset{raw: raw, etag: `"` + hex.EncodeToString(sum[:8]) + `"`, ctype: contentTypes[path.Ext(p)]}
		if a.ctype == "" {
			a.ctype = mime.TypeByExtension(path.Ext(p))
		}
		if a.ctype == "" {
			a.ctype = http.DetectContentType(raw)
		}
		if len(raw) > 512 {
			var b bytes.Buffer
			zw, _ := gzip.NewWriterLevel(&b, gzip.BestCompression)
			zw.Write(raw)
			zw.Close()
			if b.Len() < len(raw) {
				a.gz = b.Bytes()
				a.gzEtag = strings.TrimSuffix(a.etag, `"`) + `-gz"`
			}
		}
		m[p] = a
		return nil
	})
	if err != nil {
		panic("ui: reading embedded dashboard: " + err.Error())
	}
	return m
})

// contentTypes pins the dashboard's types; mime.TypeByExtension depends on
// the host's mime.types (absent in the distroless image).
var contentTypes = map[string]string{
	".html": "text/html; charset=utf-8",
	".js":   "text/javascript; charset=utf-8",
	".css":  "text/css; charset=utf-8",
	".svg":  "image/svg+xml",
}

// Handler serves the dashboard.
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
		if name == "" {
			name = "index.html"
		}
		a := assets()[name]
		if a == nil {
			http.NotFound(w, r)
			return
		}
		body, etag, gz := a.raw, a.etag, a.gz != nil && acceptsGzip(r.Header.Get("Accept-Encoding"))
		if gz {
			body, etag = a.gz, a.gzEtag
		}
		h := w.Header()
		h.Set("Content-Type", a.ctype)
		h.Set("ETag", etag)
		h.Set("Cache-Control", "no-cache") // revalidate: a 304 is cheap
		h.Set("Vary", "Accept-Encoding")
		if match := r.Header.Get("If-None-Match"); match != "" && etagMatches(match, etag) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		if gz {
			h.Set("Content-Encoding", "gzip")
		}
		h.Set("Content-Length", strconv.Itoa(len(body)))
		if r.Method == http.MethodHead {
			return
		}
		w.Write(body)
	})
}

func etagMatches(header, etag string) bool {
	for _, t := range strings.Split(header, ",") {
		t = strings.TrimSpace(t)
		if t == "*" || strings.TrimPrefix(t, "W/") == etag {
			return true
		}
	}
	return false
}

// acceptsGzip reports whether an Accept-Encoding header allows gzip
// (present, and not with q=0).
func acceptsGzip(header string) bool {
	for _, part := range strings.Split(header, ",") {
		coding, params, _ := strings.Cut(strings.TrimSpace(part), ";")
		if !strings.EqualFold(strings.TrimSpace(coding), "gzip") {
			continue
		}
		q := strings.ReplaceAll(strings.TrimSpace(params), " ", "")
		return !(q == "q=0" || q == "q=0.0" || q == "q=0.00" || q == "q=0.000")
	}
	return false
}
