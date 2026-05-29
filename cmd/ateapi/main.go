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

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/controlapi"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/sessionidentity"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/ateetcd"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/pkg/client/clientset/versioned"
	"github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var (
	listenAddr           = flag.String("grpc-listen-addr", ":443", "Address and port the gRPC server should listen on.")
	metricsListenAddr    = flag.String("metrics-listen-addr", ":9090", "Address and port the prometheus metrics server should listen on.")
	grpcServerCredBundle = flag.String("grpc-server-cred-bundle", "", "File with the server TLS credential bundle.")

	etcdEndpoints     = flag.String("etcd-endpoints", "", "Comma-separated list of etcd client endpoints (e.g. https://etcd-0.etcd.ate-system.svc:2379).")
	etcdCACerts       = flag.String("etcd-ca-certs", "", "The file that contains the CA certificate for the etcd cluster.")
	etcdTLSServerName = flag.String("etcd-tls-server-name", "", "The ServerName to use for etcd TLS hostname verification.")
	etcdClientCert    = flag.String("etcd-client-cert", "", "The file containing the client TLS certificate/key credential bundle for etcd.")

	clientJWTIssuer      = flag.String("client-jwt-issuer", "", "The expected issuer URL for client JWTs.")
	clientJWTAudience    = flag.String("client-jwt-audience", "", "The expected audience for client JWTs.")
	sessionIDJWTPoolFile = flag.String("session-id-jwt-pool", "", "The file that contains the serialized JWT authority pool for signing session JWTs")

	sessionIDCAPoolFile = flag.String("session-id-ca-pool", "", "The file that contains the CA pool for signing session JWTs")
	workerpoolCACerts   = flag.String("workerpool-ca-certs", "", "The file that contains the CA for verifying workerpool client certificates.")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	serverboot.InitLogger()

	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName: "ateapi",
		Sampler:     sdktrace.ParentBased(sdktrace.AlwaysSample()),
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize tracing", err)
	}
	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)

	mp, err := serverboot.InitMetrics(ctx, "ateapi")
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize metrics", err)
	}
	defer serverboot.ShutdownProvider("MeterProvider", mp.Shutdown)

	loadFlagsFromEnv()
	logFlagValues(ctx)

	etcdClient, err := connectEtcd(ctx)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to set up etcd", err)
	}
	defer etcdClient.Close()

	clientset, ateClient, err := newKubeClients()
	if err != nil {
		serverboot.Fatal(ctx, "Failed to create Kubernetes clients", err)
	}

	serverCreds, err := buildServerCreds(ctx)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to build server credentials", err)
	}

	etcdPersistence := ateetcd.NewPersistence(etcdClient)

	ateFactory := externalversions.NewSharedInformerFactory(ateClient, 0)
	actorTemplateLister := ateFactory.Api().V1alpha1().ActorTemplates().Lister()

	workerPodInformerFactory, workerPodInformer := controlapi.WorkerPodInformer(clientset)
	ateletPodInformerFactory, ateletPodInformer := controlapi.AteletInformer(clientset)

	syncer := controlapi.NewWorkerPoolSyncer(etcdPersistence, workerPodInformer)
	syncer.Start(ctx)

	stopCh := make(chan struct{})
	defer close(stopCh)
	workerPodInformerFactory.Start(stopCh)
	ateletPodInformerFactory.Start(stopCh)
	ateFactory.Start(stopCh)

	workerPodInformerFactory.WaitForCacheSync(stopCh)
	ateletPodInformerFactory.WaitForCacheSync(stopCh)
	ateFactory.WaitForCacheSync(stopCh)

	dialer := controlapi.NewAteletDialer(workerPodInformer.GetIndexer(), ateletPodInformer.GetIndexer())
	sm := controlapi.NewService(etcdPersistence, actorTemplateLister, dialer)

	sessionIdentitySrv := sessionidentity.New(*clientJWTIssuer, *clientJWTAudience, *sessionIDJWTPoolFile, *sessionIDCAPoolFile, *workerpoolCACerts)

	lisCfg := &net.ListenConfig{}
	lis, err := lisCfg.Listen(ctx, "tcp", *listenAddr)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to start listener", err)
	}

	mux := grpc.NewServer(
		grpc.Creds(serverCreds),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(ateinterceptors.ServerUnaryInterceptor),
	)
	reflection.Register(mux)
	ateapipb.RegisterControlServer(mux, sm)
	ateapipb.RegisterSessionIdentityServer(mux, sessionIdentitySrv)

	go serverboot.StartMetricsServer(ctx, serverboot.MetricsServerOptions{
		Addr:         *metricsListenAddr,
		EnableReadyz: true,
	})

	if err := mux.Serve(lis); err != nil {
		serverboot.Fatal(ctx, "Failed to serve", err)
	}
}

// loadFlagsFromEnv resolves any flag whose value is the sentinel `@env`
// against a known environment variable. Lets one set of Kubernetes
// manifests source per-developer config from a ConfigMap without
// editing the manifests for each branch.
func loadFlagsFromEnv() {
	overrides := []struct {
		flag *string
		env  string
	}{
		{etcdEndpoints, "ATE_API_ETCD_ENDPOINTS"},
		{clientJWTIssuer, "ATE_API_K8SJWT_ISSUER"},
		{etcdTLSServerName, "ATE_API_ETCD_TLS_SERVER_NAME"},
		{etcdClientCert, "ATE_API_ETCD_CLIENT_CERT"},
	}
	for _, o := range overrides {
		if *o.flag == "@env" {
			*o.flag = os.Getenv(o.env)
		}
	}
}

func logFlagValues(ctx context.Context) {
	slog.InfoContext(ctx, "Final flag values",
		slog.String("grpc-listen-addr", *listenAddr),
		slog.String("grpc-server-cred-bundle", *grpcServerCredBundle),
		slog.String("etcd-endpoints", *etcdEndpoints),
		slog.String("etcd-ca-certs", *etcdCACerts),
		slog.String("etcd-tls-server-name", *etcdTLSServerName),
		slog.String("etcd-client-cert", *etcdClientCert),
		slog.String("client-jwt-issuer", *clientJWTIssuer),
		slog.String("client-jwt-audience", *clientJWTAudience),
		slog.String("session-id-jwt-pool", *sessionIDJWTPoolFile),
		slog.String("session-id-ca-pool", *sessionIDCAPoolFile),
		slog.String("workerpool-ca-certs", *workerpoolCACerts),
	)
}

// connectEtcd builds the etcd TLS config, opens the client, and verifies
// connectivity with retries.
func connectEtcd(ctx context.Context) (*clientv3.Client, error) {
	endpoints := splitEndpoints(*etcdEndpoints)
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no etcd endpoints configured (set --etcd-endpoints or ATE_API_ETCD_ENDPOINTS)")
	}

	tlsConfig, err := buildEtcdTLSConfig(ctx)
	if err != nil {
		return nil, err
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		TLS:         tlsConfig,
		DialTimeout: 5 * time.Second,
		Context:     ctx,
	})
	if err != nil {
		return nil, fmt.Errorf("create etcd client: %w", err)
	}

	if err := waitForEtcd(ctx, client, endpoints); err != nil {
		client.Close()
		return nil, err
	}
	return client, nil
}

// buildEtcdTLSConfig builds the client TLS config for etcd. It returns nil when
// no TLS material is configured, which leaves the connection plaintext.
func buildEtcdTLSConfig(ctx context.Context) (*tls.Config, error) {
	if *etcdCACerts == "" && *etcdClientCert == "" && *etcdTLSServerName == "" {
		return nil, nil
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if *etcdCACerts != "" {
		ca, err := os.ReadFile(*etcdCACerts)
		if err != nil {
			return nil, fmt.Errorf("read etcd CA cert: %w", err)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("parse etcd CA cert from %s", *etcdCACerts)
		}
		tlsConfig.RootCAs = caPool
		slog.InfoContext(ctx, "Using custom CA cert for etcd", slog.String("path", *etcdCACerts))
	}
	if *etcdTLSServerName != "" {
		tlsConfig.ServerName = *etcdTLSServerName
		slog.InfoContext(ctx, "Using custom ServerName for etcd TLS verification", slog.String("name", *etcdTLSServerName))
	}
	if *etcdClientCert != "" {
		cert, err := credbundle.Parse(*etcdClientCert)
		if err != nil {
			return nil, fmt.Errorf("parse etcd client credential bundle: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{*cert}
		slog.InfoContext(ctx, "Using client TLS certificate for etcd", slog.String("path", *etcdClientCert))
	}
	return tlsConfig, nil
}

func waitForEtcd(ctx context.Context, client *clientv3.Client, endpoints []string) error {
	var statusErr error
	for i := 0; i < 30; i++ {
		statusCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, statusErr = client.Status(statusCtx, endpoints[0])
		cancel()
		if statusErr == nil {
			return nil
		}
		slog.WarnContext(ctx, "Failed to connect to etcd, retrying...", slog.Int("attempt", i+1), slog.Any("err", statusErr))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("connect to etcd after 30 retries: %w", statusErr)
}

// splitEndpoints parses a comma-separated endpoint list, trimming whitespace
// and dropping empty entries.
func splitEndpoints(s string) []string {
	var out []string
	for _, e := range strings.Split(s, ",") {
		if e = strings.TrimSpace(e); e != "" {
			out = append(out, e)
		}
	}
	return out
}

// newKubeClients builds the standard Kubernetes clientset and the ate
// (substrate CRD) clientset from in-cluster config.
func newKubeClients() (*kubernetes.Clientset, versioned.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("get cluster config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("create clientset: %w", err)
	}
	ateClient, err := versioned.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("create ate clientset: %w", err)
	}
	return clientset, ateClient, nil
}

// buildServerCreds loads the workerpool CA pool (if configured) and
// composes gRPC TransportCredentials over the server bundle + optional
// client-cert verification.
func buildServerCreds(ctx context.Context) (credentials.TransportCredentials, error) {
	var clientCAs *x509.CertPool
	if *workerpoolCACerts != "" {
		// TODO: Periodically reload these to handle rotations. Consult with Tina to see how she did it for client-go.
		ca, err := os.ReadFile(*workerpoolCACerts)
		if err != nil {
			return nil, fmt.Errorf("read workerpool CA: %w", err)
		}
		clientCAs = x509.NewCertPool()
		if !clientCAs.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("parse workerpool CA from %s", *workerpoolCACerts)
		}
		slog.InfoContext(ctx, "Using custom CA for workerpool clients", slog.String("path", *workerpoolCACerts))
	}
	return credentials.NewTLS(&tls.Config{
		GetCertificate: credbundle.Loader(*grpcServerCredBundle),
		ClientAuth:     tls.VerifyClientCertIfGiven,
		ClientCAs:      clientCAs,
	}), nil
}
