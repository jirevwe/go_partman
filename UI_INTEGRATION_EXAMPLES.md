# UI Integration Examples

The `go_partman` package provides flexible UI integration options that allow you to mount the partition manager UI in your own Go applications.

## Integration Options

### 1. Complete UI Handler (Recommended for simple setups)

Use `UIHandler()` when you want to serve both the API and static files from a single endpoint:

```go
package main

import (
    "net/http"
    "github.com/jirevwe/go_partman"
)

func main() {
    // Initialize your manager
    manager, err := partman.NewManager(/* your options */)
    if err != nil {
        log.Fatal(err)
    }

    // Mount the complete UI at /partman
    http.Handle("/partman/", http.StripPrefix("/partman", partman.UIHandler(manager)))

    // Or mount at root
    http.Handle("/", partman.UIHandler(manager))

    http.ListenAndServe(":8080", nil)
}
```

### 2. Separate API and Static Handlers (Recommended for complex setups)

Use separate handlers when you want more control over routing:

```go
package main

import (
    "net/http"
    "github.com/jirevwe/go_partman"
    "github.com/gorilla/mux"
)

func main() {
    // Initialize your manager
    manager, err := partman.NewManager(/* your options */)
    if err != nil {
        log.Fatal(err)
    }

    router := mux.NewRouter()

    // Mount API endpoints
    router.PathPrefix("/api/partman/").Handler(
        http.StripPrefix("/api/partman", partman.APIHandler(manager)),
    )

    // Mount static files
    router.PathPrefix("/partman/").Handler(
        http.StripPrefix("/partman", partman.StaticHandler()),
    )

    http.ListenAndServe(":8080", router)
}
```

### 3. Gin Framework Integration

```go
package main

import (
    "github.com/gin-gonic/gin"
    "github.com/jirevwe/go_partman"
    "net/http"
)

func main() {
    // Initialize your manager
    manager, err := partman.NewManager(/* your options */)
    if err != nil {
        log.Fatal(err)
    }

    r := gin.Default()

    // Mount API endpoints
    apiGroup := r.Group("/api/partman")
    {
        apiGroup.GET("/tables", gin.WrapH(partman.APIHandler(manager)))
        apiGroup.GET("/partitions", gin.WrapH(partman.APIHandler(manager)))
    }

    // Mount static files
    r.StaticFS("/partman", http.FS(partman.StaticHandler().(*http.FileServer).Handler.(http.FileSystem)))

    r.Run(":8080")
}
```

### 4. Echo Framework Integration

```go
package main

import (
    "github.com/labstack/echo/v4"
    "github.com/jirevwe/go_partman"
    "net/http"
)

func main() {
    // Initialize your manager
    manager, err := partman.NewManager(/* your options */)
    if err != nil {
        log.Fatal(err)
    }

    e := echo.New()

    // Mount API endpoints
    apiGroup := e.Group("/api/partman")
    apiGroup.Any("/*", echo.WrapHandler(partman.APIHandler(manager)))

    // Mount static files
    e.Static("/partman", "web/dist") // You'll need to copy the dist folder

    e.Start(":8080")
}
```

### 5. Chi Router Integration

```go
package main

import (
    "github.com/go-chi/chi/v5"
    "github.com/go-chi/chi/v5/middleware"
    "github.com/jirevwe/go_partman"
)

func main() {
    // Initialize your manager
    manager, err := partman.NewManager(/* your options */)
    if err != nil {
        log.Fatal(err)
    }

    r := chi.NewRouter()
    r.Use(middleware.Logger)

    // Mount API endpoints
    r.Route("/api/partman", func(r chi.Router) {
        r.Mount("/", partman.APIHandler(manager))
    })

    // Mount static files
    r.Mount("/partman", partman.StaticHandler())

    http.ListenAndServe(":8080", r)
}
```

## API Endpoints

### Get Managed Tables

```
GET /api/tables
```

Response:

```json
{
  "tables": ["table1", "table2", "table3"]
}
```

### Get Partition Details

```
GET /api/partitions?table=table_name&schema=schema_name
```

Parameters:

- `table` (required): The name of the table
- `schema` (optional): The database schema (defaults to first configured table's schema)

Response:

```json
{
  "partitions": [
    {
      "name": "table_20240101",
      "size": "1.2 MB",
      "rows": 15000,
      "range": "FOR VALUES FROM ('2024-01-01') TO ('2024-01-02')",
      "created": "2024-01-01",
      "size_bytes": 1258291
    }
  ],
  "parent_table": {
    "name": "table",
    "total_size": "15.6 MB",
    "total_rows": 150000,
    "partition_count": 10,
    "total_size_bytes": 16357785
  }
}
```

## Frontend Configuration

### Environment Variables

The frontend can be configured using environment variables:

```bash
# Development
VITE_API_URL=http://localhost:8080/api

# Production
VITE_API_URL=https://your-domain.com/api
```

### Custom API Base URL

If you mount the API at a different path, update the frontend configuration:

```typescript
// In web/src/api.ts
const API_BASE_URL =
  import.meta.env.VITE_API_URL || "http://localhost:8080/api/partman";
```

## Security Considerations

### 1. Authentication

Add authentication middleware to protect the UI:

```go
func authMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // Your authentication logic here
        if !isAuthenticated(r) {
            http.Error(w, "Unauthorized", http.StatusUnauthorized)
            return
        }
        next.ServeHTTP(w, r)
    })
}

// Apply to your handlers
http.Handle("/partman/", authMiddleware(http.StripPrefix("/partman", partman.UIHandler(manager))))
```

### 2. CORS Configuration

The UI includes CORS headers, but you may want to customize them:

```go
func customCORS(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Access-Control-Allow-Origin", "https://yourdomain.com")
        w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
        w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

        if r.Method == "OPTIONS" {
            w.WriteHeader(http.StatusOK)
            return
        }

        next.ServeHTTP(w, r)
    })
}
```

### 3. Rate Limiting

Add rate limiting to protect your API:

```go
import "golang.org/x/time/rate"

func rateLimitMiddleware(next http.Handler) http.Handler {
    limiter := rate.NewLimiter(rate.Every(time.Second), 10) // 10 requests per second

    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if !limiter.Allow() {
            http.Error(w, "Too many requests", http.StatusTooManyRequests)
            return
        }
        next.ServeHTTP(w, r)
    })
}
```

## Production Deployment

### 1. Build the Frontend

```bash
cd web
npm run build
```

### 2. Embed in Go Binary

The frontend is automatically embedded using Go's `embed` package.

### 3. Environment Configuration

```bash
export PARTMAN_DB_URL="postgres://user:pass@host:port/db"
export PARTMAN_LOG_LEVEL="info"
```

### 4. Health Checks

Add health check endpoints:

```go
func healthCheck(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

http.HandleFunc("/health", healthCheck)
```

## Troubleshooting

### Common Issues

1. **CORS Errors**: Ensure your API endpoints are accessible from your frontend domain
2. **404 Errors**: Check that your route prefixes match your frontend configuration
3. **Database Connection**: Verify your database connection and permissions
4. **Static File Serving**: Ensure the embedded files are being served correctly

### Debug Mode

Enable debug logging:

```go
logger := partman.NewSlogLogger(slog.HandlerOptions{
    Level: slog.LevelDebug,
})
```

This comprehensive integration guide should help you successfully integrate the partition manager UI into your Go applications.
