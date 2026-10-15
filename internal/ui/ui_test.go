package ui

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"testing"
)

func get(t *testing.T, path string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, req)
	return rec
}

func TestServesIndexAndAssets(t *testing.T) {
	index, err := fs.ReadFile(FS(), "index.html")
	if err != nil {
		t.Fatal(err)
	}
	rec := get(t, "/", nil)
	if rec.Code != 200 || !bytes.Equal(rec.Body.Bytes(), index) || rec.Header().Get("Content-Type") != "text/html; charset=utf-8" {
		t.Fatalf("GET /: %d %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	for path, ctype := range map[string]string{"/app.js": "text/javascript; charset=utf-8", "/styles.css": "text/css; charset=utf-8", "/favicon.svg": "image/svg+xml"} {
		if rec := get(t, path, nil); rec.Code != 200 || rec.Header().Get("Content-Type") != ctype {
			t.Errorf("GET %s: %d %q", path, rec.Code, rec.Header().Get("Content-Type"))
		}
	}
	if rec := get(t, "/nope.js", nil); rec.Code != 404 {
		t.Errorf("missing file: %d", rec.Code)
	}
	if rec := get(t, "/../../etc/passwd", nil); rec.Code != 404 {
		t.Errorf("traversal: %d", rec.Code)
	}
}

func TestGzipAndETag(t *testing.T) {
	raw, _ := fs.ReadFile(FS(), "app.js")
	rec := get(t, "/app.js", map[string]string{"Accept-Encoding": "br, gzip"})
	if rec.Header().Get("Content-Encoding") != "gzip" || rec.Body.Len() >= len(raw)/2 {
		t.Fatalf("expected a much smaller gzip body: %q, %d of %d bytes", rec.Header().Get("Content-Encoding"), rec.Body.Len(), len(raw))
	}
	zr, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := io.ReadAll(zr); !bytes.Equal(got, raw) {
		t.Fatal("gzip body does not decompress to the asset")
	}
	if rec := get(t, "/app.js", map[string]string{"Accept-Encoding": "gzip;q=0"}); rec.Header().Get("Content-Encoding") != "" {
		t.Fatal("gzip;q=0 must get the identity encoding")
	}

	etag := rec.Header().Get("ETag")
	if etag == "" {
		t.Fatal("no ETag")
	}
	if rec := get(t, "/app.js", map[string]string{"If-None-Match": etag, "Accept-Encoding": "gzip"}); rec.Code != http.StatusNotModified || rec.Body.Len() != 0 {
		t.Fatalf("revalidation: %d, %d bytes", rec.Code, rec.Body.Len())
	}
	// Each content-coding has its own strong validator.
	identity := get(t, "/app.js", nil).Header().Get("ETag")
	if identity == etag {
		t.Fatal("gzip and identity responses share an ETag")
	}
	if rec := get(t, "/app.js", map[string]string{"If-None-Match": identity, "Accept-Encoding": "gzip"}); rec.Code != 200 {
		t.Fatalf("identity ETag revalidated a gzip response: %d", rec.Code)
	}
	if rec := get(t, "/app.js", map[string]string{"If-None-Match": `"stale"`}); rec.Code != 200 {
		t.Fatalf("stale ETag: %d", rec.Code)
	}
}
