package partman

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed web/dist
var uiFS embed.FS

// UIHandler returns an http.Handler that serves the partition manager UI
func UIHandler() http.Handler {
	fsys, err := fs.Sub(uiFS, "web/dist")
	if err != nil {
		panic(err)
	}
	return http.FileServer(http.FS(fsys))
}
