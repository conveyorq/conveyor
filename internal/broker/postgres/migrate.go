// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"embed"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5/pgxpool"
)

// migrationFiles embeds the schema migrations applied at startup.
//
//go:embed migrations/*.sql
var migrationFiles embed.FS

// migrationLockID is the advisory lock serializing concurrent migrators
// (e.g. several conveyord nodes booting at once against one database).
const migrationLockID = 7423886242271425537

// migrate applies every embedded migration that is not yet recorded in
// conveyor_schema_migrations, in lexical filename order, inside a single
// transaction guarded by an advisory lock.
func migrate(ctx context.Context, pool *pgxpool.Pool) (err error) {
	transaction, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin migration: %w", err)
	}
	defer func() { err = rollback(ctx, transaction, err) }()

	if _, err = transaction.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", migrationLockID); err != nil {
		return fmt.Errorf("postgres: acquire migration lock: %w", err)
	}

	const createVersions = `CREATE TABLE IF NOT EXISTS conveyor_schema_migrations (
		version TEXT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`
	if _, err = transaction.Exec(ctx, createVersions); err != nil {
		return fmt.Errorf("postgres: create migration table: %w", err)
	}

	rows, err := transaction.Query(ctx, "SELECT version FROM conveyor_schema_migrations")
	if err != nil {
		return fmt.Errorf("postgres: list applied migrations: %w", err)
	}

	applied := make(map[string]bool)

	for rows.Next() {
		var version string
		if err = rows.Scan(&version); err != nil {
			rows.Close()

			return fmt.Errorf("postgres: scan migration version: %w", err)
		}

		applied[version] = true
	}

	rows.Close()

	if err = rows.Err(); err != nil {
		return fmt.Errorf("postgres: list applied migrations: %w", err)
	}

	entries, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("postgres: read embedded migrations: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}

	slices.Sort(names)

	for _, name := range names {
		if applied[name] {
			continue
		}

		statements, err := migrationFiles.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("postgres: read migration %s: %w", name, err)
		}

		if _, err = transaction.Exec(ctx, string(statements)); err != nil {
			return fmt.Errorf("postgres: apply migration %s: %w", name, err)
		}

		if _, err = transaction.Exec(ctx, "INSERT INTO conveyor_schema_migrations (version) VALUES ($1)", name); err != nil {
			return fmt.Errorf("postgres: record migration %s: %w", name, err)
		}
	}

	if err = transaction.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit migrations: %w", err)
	}

	return nil
}
