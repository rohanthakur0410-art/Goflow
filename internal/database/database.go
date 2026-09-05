// Package database owns the SQLite connection, schema migrations, and every
// SQL statement GoFlow runs. SQLite is the single source of truth for job
// state; the in-memory queue (internal/queue) only ever holds job IDs that
// are ready to run.
package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// Store wraps a *sql.DB with GoFlow's schema-specific queries. All exported
// methods are safe for concurrent use; SQLite serializes writers internally
// and we additionally rely on transactions for compound operations like
// "claim this job" that must be atomic.
type Store struct {
	DB *sql.DB
}

// Open opens (creating if necessary) the SQLite database at path and
// configures it for a concurrent, single-file, multi-goroutine workload.
func Open(path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("creating database directory: %w", err)
		}
	}

	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_foreign_keys=on&_busy_timeout=5000", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite database: %w", err)
	}

	// SQLite allows only one writer at a time; a single shared connection
	// avoids SQLITE_BUSY errors under concurrent worker load while still
	// letting Go's database/sql pool serialize access for us.
	db.SetMaxOpenConns(1)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging sqlite database: %w", err)
	}

	return &Store{DB: db}, nil
}

// Close closes the underlying database connection.
func (s *Store) Close() error {
	return s.DB.Close()
}

// Migrate applies every *.sql file found in migrationsDir, in filename
// order, exactly once, tracking progress in a schema_migrations table.
func (s *Store) Migrate(migrationsDir string) error {
	if _, err := s.DB.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		filename   TEXT PRIMARY KEY,
		applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		return fmt.Errorf("creating schema_migrations table: %w", err)
	}

	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("reading migrations directory %q: %w", migrationsDir, err)
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	for _, name := range files {
		var already int
		if err := s.DB.QueryRow(`SELECT COUNT(*) FROM schema_migrations WHERE filename = ?`, name).Scan(&already); err != nil {
			return fmt.Errorf("checking migration state for %s: %w", name, err)
		}
		if already > 0 {
			continue
		}

		sqlBytes, err := os.ReadFile(filepath.Join(migrationsDir, name))
		if err != nil {
			return fmt.Errorf("reading migration %s: %w", name, err)
		}

		tx, err := s.DB.Begin()
		if err != nil {
			return fmt.Errorf("starting migration transaction for %s: %w", name, err)
		}
		if _, err := tx.Exec(string(sqlBytes)); err != nil {
			tx.Rollback()
			return fmt.Errorf("applying migration %s: %w", name, err)
		}
		if _, err := tx.Exec(`INSERT INTO schema_migrations (filename) VALUES (?)`, name); err != nil {
			tx.Rollback()
			return fmt.Errorf("recording migration %s: %w", name, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("committing migration %s: %w", name, err)
		}
	}

	return nil
}
