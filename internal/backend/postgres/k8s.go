package postgres

// k8s.go — K8sBackend provisions a dedicated Postgres pod per token in its own
// Kubernetes namespace. Designed for the Team tier (dedicated infrastructure).
//
// # Architecture
//
// Each provisioned token gets its own namespace (instant-customer-{token}) with:
//   - NetworkPolicy: deny-all ingress/egress; allow DB port ingress (password is the auth gate)
//   - ResourceQuota + LimitRange: noisy-neighbor / DoS protection
//   - PodSecurityStandard "baseline": blocks privileged containers, hostPath mounts
//   - Secret: admin credentials (stored in k8s, read by StorageBytes)
//   - PVC: persistent volume (gp3 on EKS, local-path for dev; set K8S_STORAGE_CLASS)
//   - Deployment: postgres container with security hardening
//   - Service: NodePort for external access (externalHost:nodePort → pod:5432)
//
// Deprovision deletes the namespace — k8s GC cascades to all owned resources.
//
// # External access pattern
//
// The returned connection URL uses K8S_EXTERNAL_HOST + NodePort. This works for:
//   - Local dev (Rancher Desktop): K8S_EXTERNAL_HOST=127.0.0.1 (NodePorts on localhost)
//   - EKS (MVP): K8S_EXTERNAL_HOST={node-ip} — works but nodes can be replaced; see below
//   - EKS (production): deploy a TCP proxy (Envoy/PgBouncer) behind a single NLB.
//     Set K8S_EXTERNAL_HOST to the NLB DNS. Per-customer NLBs are too expensive at scale.
//     See docs/ops-k8s-dedicated.md for the full production deployment guide.
//
// # Security (per Emergent.sh + CNCF multi-tenancy guidance)
//
//  1. NetworkPolicy deny-all by default; ingress on DB port open (NodePort compatibility)
//  2. Password auth: 32 hex chars = 128 bits entropy (brute-force infeasible)
//  3. PodSecurityStandard "baseline" on namespace (no privileged containers, no hostPath)
//  4. Drop ALL capabilities, no privilege escalation
//  5. Seccomp RuntimeDefault
//  6. ResourceQuota limits CPU/memory/PVCs (one pod can't starve neighbors)
//  7. Egress restricted to DNS only (DB pod cannot exfiltrate data to the internet)
//  8. No service account token auto-mount in DB pods

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

const (
	k8sNsPrefix      = "instant-customer-"
	k8sRoleLabel     = "instant.dev/role"
	k8sRoleValue     = "customer-resource"
	k8sReadyTimeout  = 3 * time.Minute
	k8sReadyInterval = 3 * time.Second
)

// K8sBackend provisions a dedicated Postgres pod per token.
// All configuration comes from environment variables — see config.go for the full list.
type K8sBackend struct {
	cs            *kubernetes.Clientset
	storageClass  string // K8S_STORAGE_CLASS: "gp3" (EKS) or "local-path" (dev)
	image         string // K8S_POSTGRES_IMAGE: "pgvector/pgvector:pg16" (default)
	externalHost  string // K8S_EXTERNAL_HOST: node IP, LB DNS, or proxy hostname
	storageSizeGi int    // K8S_POSTGRES_STORAGE_GI: default 50
}

// newK8sBackend creates a K8sBackend.
// kubeconfigPath: empty = in-cluster config (production pod); file path = local dev.
func newK8sBackend(kubeconfigPath, storageClass, image, externalHost string, storageSizeGi int) (*K8sBackend, error) {
	var rc *rest.Config
	var err error
	if kubeconfigPath != "" {
		rc, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	} else {
		rc, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("k8s postgres: build config: %w", err)
	}

	slog.Info("k8s.postgres.init", "api_host", rc.Host)
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("k8s postgres: new clientset: %w", err)
	}
	if storageClass == "" {
		storageClass = "gp3"
	}
	if image == "" {
		// pgvector/pgvector:pg16 is the official postgres 16 image with pgvector pre-installed.
		// Use this instead of postgres:16 so pg_restore can restore databases that had
		// CREATE EXTENSION vector — which all shared-tier DBs have by default.
		image = "pgvector/pgvector:pg16"
	}
	if storageSizeGi <= 0 {
		storageSizeGi = 50
	}
	return &K8sBackend{cs: cs, storageClass: storageClass, image: image, externalHost: externalHost, storageSizeGi: storageSizeGi}, nil
}

// Provision creates a dedicated Postgres instance for the given token.
// Returns connection URL using externalHost:nodePort for both in-cluster and external clients.
// ProviderResourceID is set to the namespace name for use by Deprovision.
func (b *K8sBackend) Provision(ctx context.Context, token, tier string) (*Credentials, error) {
	ns := k8sNsPrefix + token
	dbName := "db_" + k8sShort(token)
	adminUser := "pgadmin"
	adminPass, err := k8sRandHex(16)
	if err != nil {
		return nil, fmt.Errorf("k8s postgres: rand admin pass: %w", err)
	}
	appUser := "usr_" + k8sShort(token)
	appPass, err := k8sRandHex(16)
	if err != nil {
		return nil, fmt.Errorf("k8s postgres: rand app pass: %w", err)
	}

	rollback := func(step string, cause error) error {
		slog.Error("k8s.postgres.provision.rollback", "step", step, "namespace", ns, "error", cause)
		_ = b.cs.CoreV1().Namespaces().Delete(context.Background(), ns, metav1.DeleteOptions{})
		return fmt.Errorf("k8s postgres: %s: %w", step, cause)
	}

	// Use a fresh background context for the provisioning sequence.
	// The gRPC request context (ctx) has a short deadline that would cancel
	// waitPodReady, which can legitimately take 1–3 minutes for pod startup.
	provCtx, provCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer provCancel()

	if err := b.applyNamespace(provCtx, ns); err != nil {
		return nil, fmt.Errorf("k8s postgres: namespace: %w", err)
	}
	if err := b.applyNetworkPolicy(provCtx, ns, 5432); err != nil {
		return nil, rollback("network policy", err)
	}
	if err := b.applyResourceQuota(provCtx, ns); err != nil {
		return nil, rollback("resource quota", err)
	}
	if err := b.applyAdminSecret(provCtx, ns, adminUser, adminPass); err != nil {
		return nil, rollback("admin secret", err)
	}
	if err := b.applyPVC(provCtx, ns); err != nil {
		return nil, rollback("pvc", err)
	}
	if err := b.applyDeployment(provCtx, ns, adminUser); err != nil {
		return nil, rollback("deployment", err)
	}
	svc, err := b.applyService(provCtx, ns)
	if err != nil {
		return nil, rollback("service", err)
	}

	clusterAddr := fmt.Sprintf("%s:5432", svc.Spec.ClusterIP)
	nodePort := int(svc.Spec.Ports[0].NodePort)

	if err := b.waitPodReady(provCtx, ns, "app=postgres"); err != nil {
		return nil, rollback("wait ready", err)
	}

	// Connect as admin via ClusterIP (in-cluster) to create the restricted app user.
	adminDSN := fmt.Sprintf("postgres://%s:%s@%s/postgres?sslmode=disable", adminUser, adminPass, clusterAddr)
	if err := b.initDatabase(provCtx, adminDSN, dbName, appUser, appPass); err != nil {
		return nil, rollback("init database", err)
	}

	connURL := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", appUser, appPass, b.externalHost, nodePort, dbName)
	slog.Info("k8s.postgres.provisioned", "namespace", ns, "node_port", nodePort)
	return &Credentials{URL: connURL, DatabaseName: dbName, Username: appUser, ProviderResourceID: ns}, nil
}

// StorageBytes returns bytes used by the database in the dedicated pod.
// Connects via the ClusterIP service using admin credentials stored in the k8s Secret.
func (b *K8sBackend) StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error) {
	ns := providerResourceID
	if ns == "" {
		ns = k8sNsPrefix + token
	}
	dbName := "db_" + k8sShort(token)

	secret, err := b.cs.CoreV1().Secrets(ns).Get(ctx, "postgres-admin", metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("k8s postgres.StorageBytes: get secret: %w", err)
	}
	svc, err := b.cs.CoreV1().Services(ns).Get(ctx, "postgres", metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("k8s postgres.StorageBytes: get service: %w", err)
	}

	adminUser := string(secret.Data["POSTGRES_USER"])
	adminPass := string(secret.Data["POSTGRES_PASSWORD"])
	dsn := fmt.Sprintf("postgres://%s:%s@%s:5432/postgres?sslmode=disable", adminUser, adminPass, svc.Spec.ClusterIP)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, fmt.Errorf("k8s postgres.StorageBytes: connect: %w", err)
	}
	defer conn.Close(ctx)

	var bytes int64
	if err := conn.QueryRow(ctx, "SELECT pg_database_size($1)", dbName).Scan(&bytes); err != nil {
		return 0, fmt.Errorf("k8s postgres.StorageBytes: query: %w", err)
	}
	return bytes, nil
}

// Deprovision deletes the customer namespace. k8s GC cascades to all owned resources
// (NetworkPolicy, Secret, PVC, Deployment, Service, Pods).
func (b *K8sBackend) Deprovision(ctx context.Context, token, providerResourceID string) error {
	ns := providerResourceID
	if ns == "" {
		ns = k8sNsPrefix + token
	}
	if err := b.cs.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("k8s postgres.Deprovision: delete namespace %s: %w", ns, err)
	}
	slog.Info("k8s.postgres.deprovisioned", "namespace", ns)
	return nil
}

// --- private resource creators ---

func (b *K8sBackend) applyNamespace(ctx context.Context, ns string) error {
	nsObj := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: ns,
			Labels: map[string]string{
				k8sRoleLabel: k8sRoleValue,
				// PodSecurityStandard "baseline": blocks privileged containers + hostPath mounts.
				// Postgres 16 official image runs entrypoint as root then drops to uid 999 via gosu,
				// so "restricted" (which requires runAsNonRoot) would block it.
				// Use bitnami/postgresql image to enable "restricted" if required.
				"pod-security.kubernetes.io/enforce": "baseline",
				"pod-security.kubernetes.io/warn":    "restricted",
			},
		},
	}
	_, err := b.cs.CoreV1().Namespaces().Create(ctx, nsObj, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !k8serrors.IsAlreadyExists(err) {
		return err
	}
	// Namespace exists; if it's Terminating (left by a previous failed attempt),
	// wait for it to fully terminate then recreate it.
	existing, getErr := b.cs.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if getErr != nil || existing.Status.Phase != corev1.NamespaceTerminating {
		return err // not terminating — surface the original AlreadyExists error
	}
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
		_, getErr = b.cs.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
		if k8serrors.IsNotFound(getErr) {
			_, err = b.cs.CoreV1().Namespaces().Create(ctx, nsObj, metav1.CreateOptions{})
			return err
		}
	}
	return fmt.Errorf("namespace %s still terminating after 2 minutes", ns)
}

// applyNetworkPolicy creates a deny-all policy with targeted allow rules.
// Ingress on dbPort is open to all sources — NodePort NATting makes source restriction
// unreliable with standard NetworkPolicy. Password auth (128-bit entropy) is the gate.
// Egress is restricted to DNS only: the DB pod never initiates outbound connections.
func (b *K8sBackend) applyNetworkPolicy(ctx context.Context, ns string, dbPort int) error {
	proto := corev1.ProtocolTCP
	udp := corev1.ProtocolUDP
	dbP := intstr.FromInt32(int32(dbPort))
	dns := intstr.FromInt32(53)
	_, err := b.cs.NetworkingV1().NetworkPolicies(ns).Create(ctx, &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "default-deny"},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{Ports: []networkingv1.NetworkPolicyPort{{Protocol: &proto, Port: &dbP}}},
			},
			Egress: []networkingv1.NetworkPolicyEgressRule{
				{
					Ports: []networkingv1.NetworkPolicyPort{
						{Protocol: &proto, Port: &dns},
						{Protocol: &udp, Port: &dns},
					},
					To: []networkingv1.NetworkPolicyPeer{
						{NamespaceSelector: &metav1.LabelSelector{}},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applyResourceQuota(ctx context.Context, ns string) error {
	_, err := b.cs.CoreV1().ResourceQuotas(ns).Create(ctx, &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "tenant-quota"},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList{
				corev1.ResourceRequestsCPU:    resource.MustParse("500m"),
				corev1.ResourceRequestsMemory: resource.MustParse("512Mi"),
				corev1.ResourceLimitsCPU:      resource.MustParse("2"),
				corev1.ResourceLimitsMemory:   resource.MustParse("2Gi"),
				"persistentvolumeclaims":      resource.MustParse("2"),
				corev1.ResourcePods:           resource.MustParse("3"),
			},
		},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applyAdminSecret(ctx context.Context, ns, adminUser, adminPass string) error {
	_, err := b.cs.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres-admin"},
		StringData: map[string]string{
			"POSTGRES_USER":     adminUser,
			"POSTGRES_PASSWORD": adminPass,
		},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applyPVC(ctx context.Context, ns string) error {
	sc := b.storageClass
	_, err := b.cs.CoreV1().PersistentVolumeClaims(ns).Create(ctx, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres-data"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &sc,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(fmt.Sprintf("%dGi", b.storageSizeGi)),
				},
			},
		},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applyDeployment(ctx context.Context, ns, adminUser string) error {
	replicas := int32(1)
	noPrivEsc := false
	runAsUser := int64(999) // postgres UID in the official image
	fsGroup := int64(999)   // postgres GID — PVC is mounted with this GID so postgres can write

	// Security design for postgres:16:
	// The official postgres entrypoint checks if running as UID 0 (root). If non-root,
	// it skips the chown step (which needs CAP_CHOWN) and starts postgres directly.
	// Running as UID 999 from the start avoids needing CAP_CHOWN while still using
	// the official image. The PVC is mounted with fsGroup=999 so postgres has write access.
	// This satisfies PodSecurityStandard "restricted" (runAsNonRoot, no privilege escalation,
	// drop ALL). Note: "baseline" is enforced at namespace level; container-level settings
	// here are stricter than required — defense in depth.
	_, err := b.cs.AppsV1().Deployments(ns).Create(ctx, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "postgres"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "postgres"}},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: boolPtr(false),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsUser:  &runAsUser,
						RunAsGroup: &fsGroup,
						FSGroup:    &fsGroup,
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Containers: []corev1.Container{{
						Name:  "postgres",
						Image: b.image,
						Ports: []corev1.ContainerPort{{ContainerPort: 5432, Protocol: corev1.ProtocolTCP}},
						EnvFrom: []corev1.EnvFromSource{{
							SecretRef: &corev1.SecretEnvSource{
								LocalObjectReference: corev1.LocalObjectReference{Name: "postgres-admin"},
							},
						}},
						Env: []corev1.EnvVar{
							{Name: "POSTGRES_DB", Value: "postgres"},
							// PGDATA sub-directory ensures postgres has write access via fsGroup.
							{Name: "PGDATA", Value: "/var/lib/postgresql/data/pgdata"},
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(5432)},
							},
							InitialDelaySeconds: 5,
							PeriodSeconds:       3,
							FailureThreshold:    20,
						},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &noPrivEsc,
							RunAsNonRoot:             boolPtr(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("100m"),
								corev1.ResourceMemory: resource.MustParse("256Mi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("2"),
								corev1.ResourceMemory: resource.MustParse("2Gi"),
							},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/var/lib/postgresql/data"},
							{Name: "run", MountPath: "/var/run/postgresql"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "data", VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "postgres-data"},
						}},
						{Name: "run", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applyService(ctx context.Context, ns string) (*corev1.Service, error) {
	return b.cs.CoreV1().Services(ns).Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres"},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeNodePort,
			Selector: map[string]string{"app": "postgres"},
			Ports:    []corev1.ServicePort{{Port: 5432, TargetPort: intstr.FromInt32(5432), Protocol: corev1.ProtocolTCP}},
		},
	}, metav1.CreateOptions{})
}

func (b *K8sBackend) waitPodReady(ctx context.Context, ns, labelSelector string) error {
	deadline := time.Now().Add(k8sReadyTimeout)
	for time.Now().Before(deadline) {
		pods, err := b.cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		if err != nil {
			return err
		}
		for i := range pods.Items {
			for _, cond := range pods.Items[i].Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(k8sReadyInterval):
		}
	}
	return fmt.Errorf("pod not ready after %s", k8sReadyTimeout)
}

func (b *K8sBackend) initDatabase(ctx context.Context, adminDSN, dbName, appUser, appPass string) error {
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)

	for _, q := range []string{
		fmt.Sprintf(`CREATE USER %q WITH PASSWORD '%s' NOSUPERUSER NOCREATEDB NOCREATEROLE`, appUser, appPass),
		fmt.Sprintf(`CREATE DATABASE %q OWNER %q`, dbName, appUser),
		fmt.Sprintf(`REVOKE CONNECT ON DATABASE %q FROM PUBLIC`, dbName),
		fmt.Sprintf(`GRANT ALL PRIVILEGES ON DATABASE %q TO %q`, dbName, appUser),
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("exec %q: %w", q[:min(len(q), 40)], err)
		}
	}

	// Pre-create the vector extension as superuser in the new database and transfer
	// ownership to appUser so that pg_restore (running as appUser) can execute both
	// CREATE EXTENSION and COMMENT ON EXTENSION without permission errors.
	// This is best-effort: non-fatal if the image does not include pgvector.
	//
	// adminDSN connects to the "postgres" DB; replace it with dbName for this step.
	dbDSN := strings.Replace(adminDSN, "/postgres?", "/"+dbName+"?", 1)
	if dbConn, dbErr := pgx.Connect(ctx, dbDSN); dbErr == nil {
		_, _ = dbConn.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS vector`)
		_, _ = dbConn.Exec(ctx, fmt.Sprintf(`ALTER EXTENSION vector OWNER TO %q`, appUser))
		dbConn.Close(ctx)
	}
	return nil
}

// --- helpers (package-private, not exported) ---

// k8sShort strips dashes from a UUID-like token and returns the first 12 hex chars.
// Used for database names and usernames where length matters (postgres identifier limit: 63).
func k8sShort(token string) string {
	s := strings.ReplaceAll(token, "-", "")
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

// k8sRandHex returns a cryptographically random hex string of length n*2.
func k8sRandHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func boolPtr(b bool) *bool { return &b }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
