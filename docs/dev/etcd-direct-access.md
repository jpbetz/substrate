# Accessing etcd directly

Sometimes you need to inspect the control-plane etcd directly. `etcdctl` ships
inside the etcd image, and the cluster speaks mutual TLS, so point it at the
pod's mounted CA and client credential bundle:

```sh
kubectl exec -n=ate-system -it etcd-0 -- env \
  ETCDCTL_API=3 \
  ETCDCTL_ENDPOINTS=https://etcd-0.etcd.ate-system.svc:2379 \
  ETCDCTL_CACERT=/etc/etcd-ca/ca.crt \
  ETCDCTL_CERT=/run/servicedns.podcert.ate.dev/credential-bundle.pem \
  ETCDCTL_KEY=/run/servicedns.podcert.ate.dev/credential-bundle.pem \
  etcdctl get --prefix ""
```

Useful variations:

* List all actors: `etcdctl get --prefix actor:`
* List all workers: `etcdctl get --prefix worker:`
* Check cluster health: `etcdctl endpoint health --cluster`
