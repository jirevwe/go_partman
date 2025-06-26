package partman

import (
	"embed"
	"encoding/json"
	"io/fs"
	"mime"
	"net/http"
)

//go:embed web/dist
var uiFS embed.FS

type apiHandler struct {
	manager *Manager
}

func (h *apiHandler) handleGetTables(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	tables, err := h.manager.GetManagedTables(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(tables)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func (h *apiHandler) handleGetPartitions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	tableName := r.URL.Query().Get("table")
	if tableName == "" {
		http.Error(w, "table parameter is required", http.StatusBadRequest)
		return
	}

	partitions, err := h.manager.GetPartitions(r.Context(), h.manager.config.Tables[0].Schema, tableName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(partitions)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

// UIHandler returns an http.Handler that serves the partition manager UI and API
func UIHandler(manager *Manager) http.Handler {
	fsys, err := fs.Sub(uiFS, "web/dist")
	if err != nil {
		panic(err)
	}

	api := &apiHandler{manager: manager}
	mux := http.NewServeMux()

	// API routes
	mux.Handle("/api/tables", enforceJSONHandler(setupCORS(http.HandlerFunc(api.handleGetTables))))
	mux.Handle("/api/partitions", enforceJSONHandler(setupCORS(http.HandlerFunc(api.handleGetPartitions))))

	// UI routes - serve static files for all other routes
	fileServer := http.FileServer(http.FS(fsys))
	mux.Handle("/", fileServer)

	return mux
}

func setupCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func enforceJSONHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType := r.Header.Get("Content-Type")

		if contentType != "" {
			mt, _, err := mime.ParseMediaType(contentType)
			if err != nil {
				http.Error(w, "Malformed Content-Type header", http.StatusBadRequest)
				return
			}

			if mt != "application/json" {
				http.Error(w, "Content-Type header must be application/json", http.StatusUnsupportedMediaType)
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}
