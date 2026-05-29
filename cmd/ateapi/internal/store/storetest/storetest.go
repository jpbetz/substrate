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

// Package storetest provides helpers for exercising the persistence layer
// against a real, throwaway etcd. It reuses the etcd binary that the project
// already downloads for envtest (via `setup-envtest`), so tests stay hermetic
// without pulling in an embedded-etcd dependency tree.
package storetest

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/ateetcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// SetupTestStore starts a single-node etcd and returns a real store
// implementation backed by it, along with a cleanup function that tears the
// etcd process down.
func SetupTestStore(t *testing.T) (store.Interface, func()) {
	t.Helper()

	cli, cleanup := StartEtcd(t)
	return ateetcd.NewPersistence(cli), cleanup
}

// StartEtcd launches a throwaway single-node etcd, returning a connected client
// and a cleanup function. It is exported so that store-level tests can reach
// the *clientv3.Client directly.
func StartEtcd(t *testing.T) (*clientv3.Client, func()) {
	t.Helper()

	etcdBin := filepath.Join(envtestAssetsDir(t), "etcd")
	if _, err := os.Stat(etcdBin); err != nil {
		t.Fatalf("etcd binary not found at %q: %v", etcdBin, err)
	}

	clientPort := freePort(t)
	peerPort := freePort(t)
	clientURL := fmt.Sprintf("http://127.0.0.1:%d", clientPort)
	peerURL := fmt.Sprintf("http://127.0.0.1:%d", peerPort)
	dataDir := t.TempDir()

	cmd := exec.Command(etcdBin,
		"--name", "ate-test",
		"--data-dir", dataDir,
		"--listen-client-urls", clientURL,
		"--advertise-client-urls", clientURL,
		"--listen-peer-urls", peerURL,
		"--initial-advertise-peer-urls", peerURL,
		"--initial-cluster", "ate-test="+peerURL,
		"--initial-cluster-state", "new",
		"--log-level", "error",
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientURL},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		t.Fatalf("failed to create etcd client: %v", err)
	}

	// Wait for etcd to accept requests.
	if err := waitForEtcd(cli, clientURL); err != nil {
		_ = cli.Close()
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		t.Fatalf("etcd did not become ready: %v", err)
	}

	cleanup := func() {
		_ = cli.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	}
	return cli, cleanup
}

func waitForEtcd(cli *clientv3.Client, endpoint string) error {
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, lastErr = cli.Status(ctx, endpoint)
		cancel()
		if lastErr == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return lastErr
}

// freePort asks the kernel for a free TCP port, then releases it. There is a
// small race between releasing and etcd binding, but it is acceptable for tests.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// envtestAssetsDir returns the directory holding the envtest binaries (etcd,
// kube-apiserver, kubectl). It honors KUBEBUILDER_ASSETS when set, otherwise it
// asks setup-envtest via the project's run-tool.sh wrapper.
func envtestAssetsDir(t *testing.T) string {
	t.Helper()
	if dir := os.Getenv("KUBEBUILDER_ASSETS"); dir != "" {
		return dir
	}

	cmd := exec.Command("bash", filepath.Join(repoRoot(t), "hack", "run-tool.sh"),
		"setup-envtest", "use", "--print", "path")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("failed to resolve envtest assets dir: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// repoRoot walks up from this source file until it finds the module's go.mod.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine caller for repo root lookup")
	}
	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not locate repo root (go.mod not found)")
		}
		dir = parent
	}
}
