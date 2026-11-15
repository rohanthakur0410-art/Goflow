package database

import (
	"testing"
)

func TestHashAPIKey_Deterministic(t *testing.T) {
	a := HashAPIKey("gf_test_key_123")
	b := HashAPIKey("gf_test_key_123")
	if a != b || len(a) != 64 {
		t.Fatalf("hash should be deterministic 64-hex, got %q", a)
	}
	if HashAPIKey("other") == a {
		t.Fatal("different keys must hash differently")
	}
}

func TestHashPlaintextAPIKeys_MigratesSeedKey(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()

	n, err := s.HashPlaintextAPIKeys()
	if err != nil {
		t.Fatalf("hashing: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 migrated key (the seeded dev key), got %d", n)
	}

	// Second run is a no-op.
	n, err = s.HashPlaintextAPIKeys()
	if err != nil || n != 0 {
		t.Fatalf("expected idempotent no-op, got n=%d err=%v", n, err)
	}

	// The original plaintext key still authenticates its owner.
	u, err := s.GetUserByAPIKey("local-dev-key-not-a-secret")
	if err != nil {
		t.Fatalf("seeded key should authenticate after hashing: %v", err)
	}
	if u.ID != 1 || u.Username != "system" {
		t.Fatalf("expected system user, got %+v", u)
	}

	// A wrong key does not.
	if _, err := s.GetUserByAPIKey("wrong"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound for wrong key, got %v", err)
	}
}

func TestSetUserAPIKeyHash(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()

	if err := s.SetUserAPIKeyHash(1, "gf_new_admin_key"); err != nil {
		t.Fatalf("setting key: %v", err)
	}
	if _, err := s.GetUserByAPIKey("gf_new_admin_key"); err != nil {
		t.Fatalf("new key should authenticate: %v", err)
	}
	if _, err := s.GetUserByAPIKey("local-dev-key-not-a-secret"); err != ErrNotFound {
		t.Fatalf("old key should be replaced, got %v", err)
	}
	if err := s.SetUserAPIKeyHash(999, "gf_x"); err == nil {
		t.Fatal("setting a key for a missing user should fail")
	}
}
