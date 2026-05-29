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

// Package ateetcd is an ate storage backend built on etcd.
//
// Actors are stored in keys of the form `actor:<actor-id>`. They are stored as
// DBActor JSON-serialized objects.
//
// Workers are stored in keys of the form
// `worker:<namespace>:<pool-name>:<pod-name>`, holding a DBWorker JSON object.
//
// Unlike a Redis cluster, etcd presents a single, linearizable keyspace with a
// global revision counter, so we do not have to worry about which keys live on
// which shard: a transaction may touch any set of keys atomically. We still
// keep a monotonically increasing `version` field inside each stored object so
// the store.Interface optimistic-concurrency contract (expectedVersion) is
// honored independently of etcd's internal mod_revision. The compare-and-swap
// itself is implemented against etcd's mod_revision, which gives us true
// atomicity for the read-modify-write cycle.
//
// Distributed locks are implemented with etcd leases: AcquireLock puts the lock
// key under a TTL lease, so the lock is released automatically if the holder
// dies without calling ReleaseLock.
package ateetcd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Persistence is a service that stores information about applications in etcd.
type Persistence struct {
	cli *clientv3.Client
}

var _ store.Interface = (*Persistence)(nil)

// NewPersistence creates a new Persistence backed by the given etcd client.
func NewPersistence(cli *clientv3.Client) *Persistence {
	return &Persistence{
		cli: cli,
	}
}

const (
	actorKeyPrefix  = "actor:"
	workerKeyPrefix = "worker:"
)

func actorDBKey(id string) string {
	return actorKeyPrefix + id
}

func workerDBKey(namespace, poolName, podName string) string {
	return workerKeyPrefix + namespace + ":" + poolName + ":" + podName
}

// DebugClearAll flushes all data from etcd.
func (s *Persistence) DebugClearAll(ctx context.Context) error {
	if _, err := s.cli.Delete(ctx, "", clientv3.WithPrefix()); err != nil {
		return fmt.Errorf("while clearing etcd: %w", err)
	}
	return nil
}

func (s *Persistence) GetActor(ctx context.Context, id string) (*ateapipb.Actor, error) {
	dbKey := actorDBKey(id)

	resp, err := s.cli.Get(ctx, dbKey)
	if err != nil {
		return nil, fmt.Errorf("while getting actor key %q: %w", dbKey, err)
	}
	if len(resp.Kvs) == 0 {
		return nil, store.ErrNotFound
	}

	actor := &ateapipb.Actor{}
	if err := protojson.Unmarshal(resp.Kvs[0].Value, actor); err != nil {
		return nil, fmt.Errorf("while unmarshaling actor: %w", err)
	}

	if actor.GetActorId() != id {
		return nil, fmt.Errorf("(impossible) mismatch between stored id and key id")
	}

	return actor, nil
}

func (s *Persistence) CreateActor(ctx context.Context, actor *ateapipb.Actor) error {
	dbKey := actorDBKey(actor.GetActorId())

	// Clone because we will update the version field, and we don't want to
	// stomp the caller's copy.
	dbActor := proto.Clone(actor).(*ateapipb.Actor)
	dbActor.Version = 1

	dbActorBytes, err := protojson.Marshal(dbActor)
	if err != nil {
		return fmt.Errorf("in protojson.Marshal: %w", err)
	}

	// Create only if the key does not already exist (CreateRevision == 0).
	resp, err := s.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(dbKey), "=", 0)).
		Then(clientv3.OpPut(dbKey, string(dbActorBytes))).
		Commit()
	if err != nil {
		return fmt.Errorf("while executing etcd create actor txn: %w", err)
	}
	if !resp.Succeeded {
		return store.ErrAlreadyExists
	}

	return nil
}

func (s *Persistence) CreateWorker(ctx context.Context, worker *ateapipb.Worker) error {
	dbKey := workerDBKey(worker.GetWorkerNamespace(), worker.GetWorkerPool(), worker.GetWorkerPod())

	// Clone because we will update the version field, and we don't want to
	// stomp the caller's copy.
	dbWorker := proto.Clone(worker).(*ateapipb.Worker)
	dbWorker.Version = 1

	dbWorkerBytes, err := protojson.Marshal(dbWorker)
	if err != nil {
		return fmt.Errorf("in protojson.Marshal: %w", err)
	}

	resp, err := s.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(dbKey), "=", 0)).
		Then(clientv3.OpPut(dbKey, string(dbWorkerBytes))).
		Commit()
	if err != nil {
		return fmt.Errorf("while executing etcd create worker txn: %w", err)
	}
	if !resp.Succeeded {
		return store.ErrAlreadyExists
	}

	return nil
}

func (s *Persistence) GetWorker(ctx context.Context, namespace, pool, pod string) (*ateapipb.Worker, error) {
	dbKey := workerDBKey(namespace, pool, pod)

	resp, err := s.cli.Get(ctx, dbKey)
	if err != nil {
		return nil, fmt.Errorf("while getting worker key %q: %w", dbKey, err)
	}
	if len(resp.Kvs) == 0 {
		return nil, store.ErrNotFound
	}

	worker := &ateapipb.Worker{}
	if err := protojson.Unmarshal(resp.Kvs[0].Value, worker); err != nil {
		return nil, fmt.Errorf("in protojson.Unmarshal: %w", err)
	}

	if worker.GetWorkerNamespace() != namespace || worker.GetWorkerPool() != pool || worker.GetWorkerPod() != pod {
		return nil, fmt.Errorf("(impossible) mismatch between stored namespace/pool/pod and key")
	}

	return worker, nil
}

func (s *Persistence) UpdateWorker(ctx context.Context, worker *ateapipb.Worker, expectedVersion int64) error {
	dbKey := workerDBKey(worker.GetWorkerNamespace(), worker.GetWorkerPool(), worker.GetWorkerPod())

	// Clone because we will update the version field, and we don't want to
	// stomp the caller's copy.
	dbWorker := proto.Clone(worker).(*ateapipb.Worker)

	resp, err := s.cli.Get(ctx, dbKey)
	if err != nil {
		return fmt.Errorf("while getting worker: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return fmt.Errorf("worker does not exist")
	}
	current := resp.Kvs[0]

	currentWorker := &ateapipb.Worker{}
	if err := protojson.Unmarshal(current.Value, currentWorker); err != nil {
		return fmt.Errorf("in protojson.Unmarshal: %w", err)
	}

	if currentWorker.GetVersion() != expectedVersion {
		return store.ErrPersistenceRetry
	}
	dbWorker.Version = currentWorker.GetVersion() + 1
	if currentWorker.GetWorkerNamespace() != dbWorker.GetWorkerNamespace() {
		return fmt.Errorf("worker_namespace is immutable")
	}
	if currentWorker.GetWorkerPool() != dbWorker.GetWorkerPool() {
		return fmt.Errorf("worker_pool is immutable")
	}
	if currentWorker.GetWorkerPod() != dbWorker.GetWorkerPod() {
		return fmt.Errorf("worker_pod is immutable")
	}
	if currentWorker.GetIp() != dbWorker.GetIp() {
		return fmt.Errorf("ip is immutable")
	}

	newVal, err := protojson.Marshal(dbWorker)
	if err != nil {
		return fmt.Errorf("in protojson.Marshal: %w", err)
	}

	// Commit only if the key has not changed since we read it.
	txnResp, err := s.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(dbKey), "=", current.ModRevision)).
		Then(clientv3.OpPut(dbKey, string(newVal))).
		Commit()
	if err != nil {
		return fmt.Errorf("while executing update worker transaction: %w", err)
	}
	if !txnResp.Succeeded {
		return store.ErrPersistenceRetry
	}

	return nil
}

func (s *Persistence) DeleteWorker(ctx context.Context, namespace, pool, pod string) error {
	dbKey := workerDBKey(namespace, pool, pod)
	if _, err := s.cli.Delete(ctx, dbKey); err != nil {
		return fmt.Errorf("while deleting worker key %q: %w", dbKey, err)
	}
	return nil
}

func (s *Persistence) DeleteActor(ctx context.Context, id string) error {
	dbKey := actorDBKey(id)

	resp, err := s.cli.Get(ctx, dbKey)
	if err != nil {
		return fmt.Errorf("while getting actor: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return store.ErrNotFound
	}
	current := resp.Kvs[0]

	currentActor := &ateapipb.Actor{}
	if err := protojson.Unmarshal(current.Value, currentActor); err != nil {
		return fmt.Errorf("in protojson.Unmarshal: %w", err)
	}

	if currentActor.GetStatus() != ateapipb.Actor_STATUS_SUSPENDED {
		return store.ErrFailedPrecondition
	}

	// Delete only if the actor has not changed since we read it.
	txnResp, err := s.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(dbKey), "=", current.ModRevision)).
		Then(clientv3.OpDelete(dbKey)).
		Commit()
	if err != nil {
		return fmt.Errorf("while executing delete actor transaction: %w", err)
	}
	if !txnResp.Succeeded {
		return store.ErrPersistenceRetry
	}

	return nil
}

func (s *Persistence) UpdateActor(ctx context.Context, actor *ateapipb.Actor, expectedVersion int64) error {
	dbKey := actorDBKey(actor.GetActorId())

	// Clone because we will update the version field, and we don't want to
	// stomp the caller's copy.
	dbActor := proto.Clone(actor).(*ateapipb.Actor)

	resp, err := s.cli.Get(ctx, dbKey)
	if err != nil {
		return fmt.Errorf("while getting actor: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return fmt.Errorf("actor does not exist")
	}
	current := resp.Kvs[0]

	currentActor := &ateapipb.Actor{}
	if err := protojson.Unmarshal(current.Value, currentActor); err != nil {
		return fmt.Errorf("in protojson.Unmarshal: %w", err)
	}

	if currentActor.GetVersion() != expectedVersion {
		return store.ErrPersistenceRetry
	}
	dbActor.Version = currentActor.GetVersion() + 1
	if currentActor.GetActorId() != dbActor.GetActorId() {
		return fmt.Errorf("actor_id is immutable")
	}
	if currentActor.GetActorTemplateNamespace() != dbActor.GetActorTemplateNamespace() {
		return fmt.Errorf("actor_template_namespace is immutable")
	}
	if currentActor.GetActorTemplateName() != dbActor.GetActorTemplateName() {
		return fmt.Errorf("actor_template_name is immutable")
	}

	newVal, err := protojson.Marshal(dbActor)
	if err != nil {
		return fmt.Errorf("in protojson.Marshal: %w", err)
	}

	// Commit only if the key has not changed since we read it.
	txnResp, err := s.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(dbKey), "=", current.ModRevision)).
		Then(clientv3.OpPut(dbKey, string(newVal))).
		Commit()
	if err != nil {
		return fmt.Errorf("while executing update actor transaction: %w", err)
	}
	if !txnResp.Succeeded {
		return store.ErrPersistenceRetry
	}

	actor.Version = dbActor.Version
	return nil
}

func (s *Persistence) ListWorkers(ctx context.Context) ([]*ateapipb.Worker, error) {
	resp, err := s.cli.Get(ctx, workerKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("while listing workers: %w", err)
	}

	var result []*ateapipb.Worker
	for _, kv := range resp.Kvs {
		workerKey := string(kv.Key)
		parts := strings.Split(workerKey, ":")
		if len(parts) != 4 {
			return nil, fmt.Errorf("bad key format %q", workerKey)
		}

		worker := &ateapipb.Worker{}
		if err := protojson.Unmarshal(kv.Value, worker); err != nil {
			return nil, fmt.Errorf("in protojson.Unmarshal: %w", err)
		}
		result = append(result, worker)
	}
	return result, nil
}

func (s *Persistence) ListActors(ctx context.Context) ([]*ateapipb.Actor, error) {
	resp, err := s.cli.Get(ctx, actorKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("while listing actors: %w", err)
	}

	var result []*ateapipb.Actor
	for _, kv := range resp.Kvs {
		actorKey := string(kv.Key)
		parts := strings.Split(actorKey, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("bad key format %q", actorKey)
		}

		actor := &ateapipb.Actor{}
		if err := protojson.Unmarshal(kv.Value, actor); err != nil {
			return nil, fmt.Errorf("in protojson.Unmarshal: %w", err)
		}
		result = append(result, actor)
	}
	return result, nil
}

func (s *Persistence) AcquireLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	// etcd leases are granted in whole seconds; never grant a zero-second lease,
	// which would expire immediately.
	ttlSeconds := int64(ttl.Seconds())
	if ttlSeconds < 1 {
		ttlSeconds = 1
	}

	lease, err := s.cli.Grant(ctx, ttlSeconds)
	if err != nil {
		return false, fmt.Errorf("while granting lease for lock %q: %w", key, err)
	}

	// Acquire only if the key is currently unset.
	resp, err := s.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, value, clientv3.WithLease(lease.ID))).
		Commit()
	if err != nil {
		// Best-effort revoke so the dangling lease does not linger.
		_, _ = s.cli.Revoke(ctx, lease.ID)
		return false, fmt.Errorf("while acquiring lock for %q: %w", key, err)
	}
	if !resp.Succeeded {
		// We did not take the lock; revoke the unused lease.
		_, _ = s.cli.Revoke(ctx, lease.ID)
		return false, nil
	}
	return true, nil
}

func (s *Persistence) ReleaseLock(ctx context.Context, key string, value string) error {
	// Delete only if the lock is still held by us (value matches). This avoids
	// releasing a lock that a different client has since acquired.
	_, err := s.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(key), "=", value)).
		Then(clientv3.OpDelete(key)).
		Commit()
	if err != nil {
		return fmt.Errorf("while releasing lock for %q with value %q: %w", key, value, err)
	}
	return nil
}
