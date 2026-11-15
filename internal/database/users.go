package database

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
)

// User is a row of the users table: the identity jobs are attributed to and
// the owner of an API key. Only the SHA-256 hash of the key is stored; the
// plaintext key exists only with the operator.
type User struct {
	ID        int64
	Username  string
	CreatedAt string
}

// HashAPIKey returns the canonical stored form of an API key: lowercase hex
// of its SHA-256. Callers never store or log the plaintext key.
func HashAPIKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

var sha256HexPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// GetUserByAPIKey looks up the user owning the given plaintext API key, by
// hashing it first. Returns ErrNotFound when no user matches.
func (s *Store) GetUserByAPIKey(key string) (*User, error) {
	hash := HashAPIKey(key)
	u := &User{}
	err := s.DB.QueryRow(
		`SELECT id, username, created_at FROM users WHERE api_key = ?`, hash,
	).Scan(&u.ID, &u.Username, &u.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("looking up api key: %w", err)
	}
	return u, nil
}

// SetUserAPIKeyHash stores the hash of the given plaintext key for a user,
// replacing any previous key. Used at startup when the operator provides a
// key through the environment.
func (s *Store) SetUserAPIKeyHash(userID int64, key string) error {
	res, err := s.DB.Exec(`UPDATE users SET api_key = ? WHERE id = ?`, HashAPIKey(key), userID)
	if err != nil {
		return fmt.Errorf("setting api key hash: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("setting api key hash: no user with id %d", userID)
	}
	return nil
}

// HashPlaintextAPIKeys one-way migrates any users.api_key value that is not
// already a SHA-256 hex digest (notably the seeded local dev key) to its
// hashed form. Operators keep using the same plaintext key; only the stored
// representation changes. Idempotent and safe to run on every startup.
func (s *Store) HashPlaintextAPIKeys() (int, error) {
	rows, err := s.DB.Query(`SELECT id, api_key FROM users`)
	if err != nil {
		return 0, fmt.Errorf("listing users for key hashing: %w", err)
	}
	defer rows.Close()

	type entry struct {
		id  int64
		key string
	}
	var pending []entry
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.id, &e.key); err != nil {
			return 0, fmt.Errorf("scanning user: %w", err)
		}
		if !sha256HexPattern.MatchString(e.key) {
			pending = append(pending, e)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for _, e := range pending {
		if _, err := s.DB.Exec(`UPDATE users SET api_key = ? WHERE id = ?`, HashAPIKey(e.key), e.id); err != nil {
			return 0, fmt.Errorf("hashing api key for user %d: %w", e.id, err)
		}
	}
	return len(pending), nil
}
