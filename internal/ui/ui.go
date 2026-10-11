// Package ui embeds the DistKV live dashboard (built from web/dashboard,
// written in TypeScript) so that every node serves it at /ui/ with no
// extra deployment step.
package ui

import (
	"embed"
	"io/fs"
	"net/http"
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

// Handler serves the dashboard.
func Handler() http.Handler {
	files := http.FileServer(http.FS(FS()))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		files.ServeHTTP(w, r)
	})
}
