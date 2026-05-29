//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package ateetcd_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func setupTest(t *testing.T) (store.Interface, context.Context) {
	t.Helper()
	s, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	return s, context.Background()
}

func newActor(id string) *ateapipb.Actor {
	return &ateapipb.Actor{
		ActorId:                id,
		ActorTemplateNamespace: "ns",
		ActorTemplateName:      "tmpl",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
	}
}

func newWorker(ns, pool, pod string) *ateapipb.Worker {
	return &ateapipb.Worker{
		WorkerNamespace: ns,
		WorkerPool:      pool,
		WorkerPod:       pod,
		Ip:              "10.0.0.1",
	}
}

func TestGetActor_NotFound(t *testing.T) {
	s, ctx := setupTest(t)

	_, err := s.GetActor(ctx, "non-existent")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestCreateActor_Success(t *testing.T) {
	s, ctx := setupTest(t)

	if err := s.CreateActor(ctx, newActor("a1")); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}

	got, err := s.GetActor(ctx, "a1")
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if got.GetVersion() != 1 {
		t.Errorf("expected version 1, got %d", got.GetVersion())
	}
	want := newActor("a1")
	want.Version = 1
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("actor mismatch (-want +got):\n%s", diff)
	}
}

func TestCreateActor_AlreadyExists(t *testing.T) {
	s, ctx := setupTest(t)

	if err := s.CreateActor(ctx, newActor("a1")); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	err := s.CreateActor(ctx, newActor("a1"))
	if !errors.Is(err, store.ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestUpdateActor_Success(t *testing.T) {
	s, ctx := setupTest(t)

	if err := s.CreateActor(ctx, newActor("a1")); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}

	upd := newActor("a1")
	upd.Status = ateapipb.Actor_STATUS_RUNNING
	if err := s.UpdateActor(ctx, upd, 1); err != nil {
		t.Fatalf("UpdateActor: %v", err)
	}
	if upd.GetVersion() != 2 {
		t.Errorf("expected version 2 on returned actor, got %d", upd.GetVersion())
	}

	got, err := s.GetActor(ctx, "a1")
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if got.GetStatus() != ateapipb.Actor_STATUS_RUNNING || got.GetVersion() != 2 {
		t.Errorf("unexpected stored actor: status=%v version=%d", got.GetStatus(), got.GetVersion())
	}
}

func TestUpdateActor_VersionMismatch(t *testing.T) {
	s, ctx := setupTest(t)

	if err := s.CreateActor(ctx, newActor("a1")); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	err := s.UpdateActor(ctx, newActor("a1"), 99)
	if !errors.Is(err, store.ErrPersistenceRetry) {
		t.Errorf("expected ErrPersistenceRetry, got %v", err)
	}
}

func TestUpdateActor_ImmutableField(t *testing.T) {
	s, ctx := setupTest(t)

	if err := s.CreateActor(ctx, newActor("a1")); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	upd := newActor("a1")
	upd.ActorTemplateName = "different"
	if err := s.UpdateActor(ctx, upd, 1); err == nil {
		t.Errorf("expected error mutating immutable field, got nil")
	}
}

func TestDeleteActor_NotFound(t *testing.T) {
	s, ctx := setupTest(t)

	err := s.DeleteActor(ctx, "missing")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestDeleteActor_FailedPrecondition(t *testing.T) {
	s, ctx := setupTest(t)

	a := newActor("a1")
	a.Status = ateapipb.Actor_STATUS_RUNNING
	if err := s.CreateActor(ctx, a); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	err := s.DeleteActor(ctx, "a1")
	if !errors.Is(err, store.ErrFailedPrecondition) {
		t.Errorf("expected ErrFailedPrecondition, got %v", err)
	}
}

func TestDeleteActor_Success(t *testing.T) {
	s, ctx := setupTest(t)

	if err := s.CreateActor(ctx, newActor("a1")); err != nil { // suspended by default
		t.Fatalf("CreateActor: %v", err)
	}
	if err := s.DeleteActor(ctx, "a1"); err != nil {
		t.Fatalf("DeleteActor: %v", err)
	}
	if _, err := s.GetActor(ctx, "a1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestListActors(t *testing.T) {
	s, ctx := setupTest(t)

	for _, id := range []string{"a1", "a2", "a3"} {
		if err := s.CreateActor(ctx, newActor(id)); err != nil {
			t.Fatalf("CreateActor %s: %v", id, err)
		}
	}
	got, err := s.ListActors(ctx)
	if err != nil {
		t.Fatalf("ListActors: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("expected 3 actors, got %d", len(got))
	}
}

func TestWorkerLifecycle(t *testing.T) {
	s, ctx := setupTest(t)

	w := newWorker("ns", "pool", "pod")
	if err := s.CreateWorker(ctx, w); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}
	if err := s.CreateWorker(ctx, newWorker("ns", "pool", "pod")); !errors.Is(err, store.ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}

	got, err := s.GetWorker(ctx, "ns", "pool", "pod")
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if got.GetVersion() != 1 {
		t.Errorf("expected version 1, got %d", got.GetVersion())
	}

	got.ActorId = "scheduled-actor"
	if err := s.UpdateWorker(ctx, got, 1); err != nil {
		t.Fatalf("UpdateWorker: %v", err)
	}

	if err := s.UpdateWorker(ctx, got, 1); !errors.Is(err, store.ErrPersistenceRetry) {
		t.Errorf("expected ErrPersistenceRetry on stale version, got %v", err)
	}

	workers, err := s.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	if len(workers) != 1 {
		t.Errorf("expected 1 worker, got %d", len(workers))
	}

	if err := s.DeleteWorker(ctx, "ns", "pool", "pod"); err != nil {
		t.Fatalf("DeleteWorker: %v", err)
	}
	// DeleteWorker is idempotent.
	if err := s.DeleteWorker(ctx, "ns", "pool", "pod"); err != nil {
		t.Errorf("expected idempotent DeleteWorker, got %v", err)
	}
	if _, err := s.GetWorker(ctx, "ns", "pool", "pod"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestLocks(t *testing.T) {
	s, ctx := setupTest(t)

	ok, err := s.AcquireLock(ctx, "lock:1", "owner-a", 30*time.Second)
	if err != nil {
		t.Fatalf("AcquireLock: %v", err)
	}
	if !ok {
		t.Fatalf("expected to acquire lock")
	}

	// A second acquisition by a different owner must fail.
	ok, err = s.AcquireLock(ctx, "lock:1", "owner-b", 30*time.Second)
	if err != nil {
		t.Fatalf("AcquireLock (contended): %v", err)
	}
	if ok {
		t.Errorf("expected contended lock acquisition to fail")
	}

	// A non-owner release must not free the lock.
	if err := s.ReleaseLock(ctx, "lock:1", "owner-b"); err != nil {
		t.Fatalf("ReleaseLock (non-owner): %v", err)
	}
	if ok, _ := s.AcquireLock(ctx, "lock:1", "owner-c", 30*time.Second); ok {
		t.Errorf("lock should still be held by owner-a")
	}

	// The owner can release, after which the lock is free.
	if err := s.ReleaseLock(ctx, "lock:1", "owner-a"); err != nil {
		t.Fatalf("ReleaseLock (owner): %v", err)
	}
	if ok, _ := s.AcquireLock(ctx, "lock:1", "owner-d", 30*time.Second); !ok {
		t.Errorf("expected to re-acquire released lock")
	}
}

func TestDebugClearAll(t *testing.T) {
	s, ctx := setupTest(t)

	if err := s.CreateActor(ctx, newActor("a1")); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	if err := s.CreateWorker(ctx, newWorker("ns", "pool", "pod")); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}
	if err := s.DebugClearAll(ctx); err != nil {
		t.Fatalf("DebugClearAll: %v", err)
	}
	actors, _ := s.ListActors(ctx)
	workers, _ := s.ListWorkers(ctx)
	if len(actors) != 0 || len(workers) != 0 {
		t.Errorf("expected empty store after clear, got %d actors, %d workers", len(actors), len(workers))
	}
}
