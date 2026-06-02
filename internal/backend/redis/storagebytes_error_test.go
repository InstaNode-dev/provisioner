package redis

// storagebytes_error_test.go — hermetic (redismock, no real Redis) coverage
// for the MEMORY USAGE error handling in LocalBackend.StorageBytes. Pins bug
// bash 2026-06-02 #24 / the HIGH security finding: a genuine server error must
// propagate (quota is never decided on a truncated total), while the benign
// deleted-key race (goredis.Nil) is skipped.

import (
	"context"
	"errors"
	"testing"

	"github.com/go-redis/redismock/v9"
)

func TestLocalBackend_StorageBytes_MemoryUsageServerError_Propagates(t *testing.T) {
	rdb, mock := redismock.NewClientMock()
	b := &LocalBackend{rdb: rdb, redisHost: "x"}
	const token = "tok24err"
	key := token + ":k1"

	mock.ExpectScan(0, token+":*", 100).SetVal([]string{key}, 0)
	mock.ExpectMemoryUsage(key).SetErr(errors.New("ERR max number of clients reached"))

	if _, err := b.StorageBytes(context.Background(), token, ""); err == nil {
		t.Fatal("StorageBytes must PROPAGATE a server error from MEMORY USAGE (not swallow it and report a truncated total)")
	}
}

func TestLocalBackend_StorageBytes_MemoryUsageNil_SkipsKey(t *testing.T) {
	rdb, mock := redismock.NewClientMock()
	b := &LocalBackend{rdb: rdb, redisHost: "x"}
	const token = "tok24nil"
	key := token + ":k1"

	mock.ExpectScan(0, token+":*", 100).SetVal([]string{key}, 0)
	mock.ExpectMemoryUsage(key).RedisNil()

	got, err := b.StorageBytes(context.Background(), token, "")
	if err != nil {
		t.Fatalf("goredis.Nil (deleted-key race) must be skipped, not propagated; got err: %v", err)
	}
	if got != 0 {
		t.Errorf("got %d; want 0 (the only key was skipped as a deleted-key race)", got)
	}
}
