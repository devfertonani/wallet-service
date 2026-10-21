// Package migrations embeds the versioned SQL migrations so the binary, the
// migrate command and the integration tests apply exactly the same files.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
