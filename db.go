package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/oklog/ulid/v2"
)

type DB struct {
	conn *sql.DB
}

func initDB(path string) (*DB, error) {
	conn, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	// Create schema
	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id TEXT PRIMARY KEY,
		type TEXT NOT NULL,
		payload TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		max_retries INTEGER DEFAULT 3,
		attempts INTEGER DEFAULT 0,
		timeout_seconds INTEGER DEFAULT 300,
		priority INTEGER DEFAULT 1,
		error TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		completed_at TIMESTAMP,
		processing_started_at TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_type_status_priority ON jobs(type, status, priority DESC);
	CREATE INDEX IF NOT EXISTS idx_status ON jobs(status);
	`
	if _, err := conn.Exec(schema); err != nil {
		return nil, err
	}

	return &DB{conn: conn}, nil
}

func (db *DB) Close() error {
	return db.conn.Close()
}

func (db *DB) EnqueueJob(jobType string, payload map[string]interface{}, maxRetries, timeoutSeconds, priority int) (*Job, error) {
	jobID := ulid.Make().String()
	payloadJSON, _ := json.Marshal(payload)

	_, err := db.conn.Exec(`
		INSERT INTO jobs (id, type, payload, max_retries, timeout_seconds, priority, status)
		VALUES (?, ?, ?, ?, ?, ?, 'pending')
	`, jobID, jobType, string(payloadJSON), maxRetries, timeoutSeconds, priority)

	if err != nil {
		return nil, err
	}

	return db.GetJob(jobID)
}

func (db *DB) FetchNextJob(jobType string) (*Job, error) {
	tx, err := db.conn.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Check for timed-out jobs first
	_, err = tx.Exec(`
		UPDATE jobs
		SET status = 'pending', processing_started_at = NULL
		WHERE status = 'processing'
		  AND processing_started_at < datetime('now', '-' || timeout_seconds || ' seconds')
	`)
	if err != nil {
		return nil, err
	}

	// Fetch next pending job
	var id, typ, payloadJSON, status, errorMsg string
	var maxRetries, attempts, timeoutSeconds, priority int
	var createdAt, updatedAt time.Time
	var completedAt, processingStartedAt *time.Time

	err = tx.QueryRow(`
		SELECT id, type, payload, status, max_retries, attempts, timeout_seconds, priority, COALESCE(error, ''),
		       created_at, updated_at, completed_at, processing_started_at
		FROM jobs
		WHERE type = ? AND status = 'pending'
		ORDER BY priority DESC, created_at ASC
		LIMIT 1
	`, jobType).Scan(&id, &typ, &payloadJSON, &status, &maxRetries, &attempts, &timeoutSeconds, &priority, &errorMsg,
		&createdAt, &updatedAt, &completedAt, &processingStartedAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// Mark as processing
	_, err = tx.Exec(`
		UPDATE jobs
		SET status = 'processing', attempts = attempts + 1, processing_started_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, id)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	var payload map[string]interface{}
	json.Unmarshal([]byte(payloadJSON), &payload)

	return &Job{
		ID:             id,
		Type:           typ,
		Payload:        payload,
		Status:         "processing",
		MaxRetries:     maxRetries,
		Attempts:       attempts + 1,
		TimeoutSeconds: timeoutSeconds,
		Priority:       priority,
		Error:          errorMsg,
		CreatedAt:      createdAt,
		UpdatedAt:      updatedAt,
		CompletedAt:    completedAt,
	}, nil
}

func (db *DB) CompleteJob(jobID string) error {
	_, err := db.conn.Exec(`
		UPDATE jobs
		SET status = 'completed', completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, jobID)
	return err
}

func (db *DB) FailJob(jobID string, errorMsg string) error {
	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var attempts, maxRetries int
	err = tx.QueryRow(`SELECT attempts, max_retries FROM jobs WHERE id = ?`, jobID).Scan(&attempts, &maxRetries)
	if err != nil {
		return err
	}

	// If retries exhausted, move to dead letter queue
	if attempts >= maxRetries {
		_, err = tx.Exec(`
			UPDATE jobs
			SET status = 'dead_letter', error = ?, updated_at = CURRENT_TIMESTAMP
			WHERE id = ?
		`, errorMsg, jobID)
	} else {
		// Retry: reset to pending
		_, err = tx.Exec(`
			UPDATE jobs
			SET status = 'pending', error = ?, processing_started_at = NULL, updated_at = CURRENT_TIMESTAMP
			WHERE id = ?
		`, errorMsg, jobID)
	}

	if err != nil {
		return err
	}

	return tx.Commit()
}

func (db *DB) GetJob(jobID string) (*Job, error) {
	var id, typ, payloadJSON, status, errorMsg string
	var maxRetries, attempts, timeoutSeconds, priority int
	var createdAt, updatedAt time.Time
	var completedAt *time.Time

	err := db.conn.QueryRow(`
		SELECT id, type, payload, status, max_retries, attempts, timeout_seconds, priority, COALESCE(error, ''),
		       created_at, updated_at, completed_at
		FROM jobs
		WHERE id = ?
	`, jobID).Scan(&id, &typ, &payloadJSON, &status, &maxRetries, &attempts, &timeoutSeconds, &priority, &errorMsg,
		&createdAt, &updatedAt, &completedAt)

	if err != nil {
		return nil, err
	}

	var payload map[string]interface{}
	json.Unmarshal([]byte(payloadJSON), &payload)

	return &Job{
		ID:             id,
		Type:           typ,
		Payload:        payload,
		Status:         status,
		MaxRetries:     maxRetries,
		Attempts:       attempts,
		TimeoutSeconds: timeoutSeconds,
		Priority:       priority,
		Error:          errorMsg,
		CreatedAt:      createdAt,
		UpdatedAt:      updatedAt,
		CompletedAt:    completedAt,
	}, nil
}

func (db *DB) GetStats() (map[string]int, error) {
	stats := make(map[string]int)
	rows, err := db.conn.Query(`
		SELECT status, COUNT(*) as count
		FROM jobs
		GROUP BY status
	`)
	if err != nil {
		return stats, err
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return stats, err
		}
		stats[status] = count
	}

	return stats, nil
}
