// Package dotenv loads the repository's .env into the environment of a test
// run, so the overrides that docker compose reads (POSTGRES_HOST_PORT, the
// database URLs) also reach go test. Variables already set win.
package dotenv

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var once sync.Once

// Load reads <module root>/.env once per process. A missing file is fine.
func Load() {
	once.Do(func() {
		dir, err := os.Getwd()
		if err != nil {
			return
		}
		for {
			if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
				break
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				return
			}
			dir = parent
		}
		f, err := os.Open(filepath.Join(dir, ".env"))
		if err != nil {
			return
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			k, v, ok := strings.Cut(line, "=")
			if !ok {
				continue
			}
			k, v = strings.TrimSpace(k), strings.Trim(strings.TrimSpace(v), `"'`)
			if _, set := os.LookupEnv(k); !set {
				os.Setenv(k, v)
			}
		}
	})
}

// HostPort is the host port of the Compose Postgres.
func HostPort() string {
	Load()
	if p := os.Getenv("POSTGRES_HOST_PORT"); p != "" {
		return p
	}
	return "55432"
}
