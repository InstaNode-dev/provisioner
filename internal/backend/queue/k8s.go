package queue

// k8s.go — K8sBackend provisions a dedicated NATS pod per token in its own namespace.
// Security model mirrors redis/k8s.go.
//
// Configuration env vars:
//   K8S_EXTERNAL_HOST       — legacy NodePort hostname (kept for back-compat / fallback URL)
//   K8S_NATS_PUBLIC_HOST    — hostname embedded in customer URLs when nats-proxy is fronting
//   K8S_NATS_IMAGE          — container image, default "nats:2.10-alpine"
//   K8S_KUBECONFIG          — path to kubeconfig; empty = in-cluster
//   K8S_STORAGE_CLASS       — PVC storage class (used for hobby+ JetStream PVC)
//
// # External access model
//
// Customer connection URLs are of the form `nats://<token>@<publicHost>:4222`.
// The token rides on the CONNECT JSON's auth_token field. The nats-proxy
// (nats-proxy/) sends a generic INFO frame to the client, parses the CONNECT
// for auth_token, looks up `nats_route_by_token:<token>` in Redis, and forwards
// bytes to the dedicated pod. The dedicated pod has no auth configured — the
// routing IS the multi-tenancy boundary, just like redis-proxy.
//
// When K8S_NATS_PUBLIC_HOST is empty, the URL falls back to the legacy
// `nats://<K8S_EXTERNAL_HOST>:<NodePort>` shape so resources remain reachable
// in environments where the proxy isn't deployed (CI, dev, etc.).

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	goredis "github.com/redis/go-redis/v9"
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
	"instant.dev/provisioner/internal/ctxkeys"
)

const (
	natsK8sNsPrefix       = "instant-customer-"
	natsK8sRoleLabel      = "instant.dev/role"
	natsK8sRoleValue      = "customer-resource"
	natsK8sOwnerTeamLabel = "instant.dev/owner-team"
	natsK8sReadyTO        = 3 * time.Minute
	natsK8sReadyPoll      = 3 * time.Second
)

// tierSizing maps a billing tier to k8s resource sizing for the provisioned NATS pod.
// Anonymous (24h trial) gets the smallest viable pod — still a real, dedicated NATS,
// just configured for low cost so the free tier scales.
//
// JetStream needs persistent storage above anonymous; anonymous uses memory-only
// JetStream (-js without -sd) so no PVC is created.
type tierSizing struct {
	cpuReq, memReq string
	cpuLim, memLim string
	// pvcMi is the JetStream PVC size in MiB. Zero means no PVC (memory-only JS).
	pvcMi int
	// quotaRequests / quotaLimits cap the whole namespace as defense-in-depth.
	qCPURequests, qMemRequests string
	qCPULimits, qMemLimits     string
}

func sizingForTier(tier string) tierSizing {
	switch tier {
	case "anonymous":
		// Anonymous trial: smallest practical pod, memory-only JetStream.
		return tierSizing{
			cpuReq: "30m", memReq: "64Mi",
			cpuLim: "200m", memLim: "128Mi",
			pvcMi:        0, // memory-only JetStream
			qCPURequests: "60m", qMemRequests: "128Mi",
			qCPULimits: "400m", qMemLimits: "256Mi",
		}
	case "hobby":
		return tierSizing{
			cpuReq: "50m", memReq: "128Mi",
			cpuLim: "500m", memLim: "512Mi",
			pvcMi:        1024, // 1Gi
			qCPURequests: "100m", qMemRequests: "256Mi",
			qCPULimits: "1", qMemLimits: "1Gi",
		}
	case "pro":
		return tierSizing{
			cpuReq: "100m", memReq: "256Mi",
			cpuLim: "1", memLim: "1Gi",
			pvcMi:        10240, // 10Gi
			qCPURequests: "200m", qMemRequests: "512Mi",
			qCPULimits: "2", qMemLimits: "2Gi",
		}
	case "team", "growth":
		return tierSizing{
			cpuReq: "200m", memReq: "512Mi",
			cpuLim: "2", memLim: "2Gi",
			pvcMi:        51200, // 50Gi
			qCPURequests: "400m", qMemRequests: "1Gi",
			qCPULimits: "4", qMemLimits: "4Gi",
		}
	default:
		// Unknown tier → conservative hobby-equivalent sizing.
		return sizingForTier("hobby")
	}
}

// K8sBackend provisions a dedicated NATS pod per token.
type K8sBackend struct {
	cs           *kubernetes.Clientset
	storageClass string // K8S_STORAGE_CLASS (used for JetStream PVC at hobby+)
	image        string // K8S_NATS_IMAGE
	externalHost string // K8S_EXTERNAL_HOST (legacy NodePort host; kept for back-compat)
	publicHost   string // K8S_NATS_PUBLIC_HOST (e.g. nats.instanode.dev) — preferred URL host when set
	httpClient   *http.Client

	// Route registry — written on every successful Provision so the nats-proxy
	// (nats-proxy/) can demux client connections by CONNECT auth_token. Two
	// key families are written:
	//   <routePrefix><token>      → <service-fqdn>:4222  (consumed by the proxy)
	//   <tokenPrefix><token>      → <service-fqdn>:4222  (debugging / future expansion)
	// When rdb == nil, no route records are written.
	rdb         *goredis.Client
	routePrefix string // nats_route_by_token:  (consumed by nats-proxy)
	tokenPrefix string // nats_route:           (parallel debug record, like redis)
}

func newK8sBackend(kubeconfigPath, storageClass, image, externalHost string) (*K8sBackend, error) {
	var rc *rest.Config
	var err error
	if kubeconfigPath != "" {
		rc, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	} else {
		rc, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("k8s nats: build config: %w", err)
	}
	slog.Info("k8s.nats.init", "api_host", rc.Host)
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("k8s nats: new clientset: %w", err)
	}
	if image == "" {
		image = "nats:2.10-alpine"
	}
	if storageClass == "" {
		storageClass = "gp3"
	}
	return &K8sBackend{
		cs:           cs,
		storageClass: storageClass,
		image:        image,
		externalHost: externalHost,
		httpClient:   &http.Client{Timeout: 10 * time.Second},
	}, nil
}

// EnableRouteRegistry tells the K8sBackend to publish routing records to Redis
// after every successful Provision so the nats-proxy can forward client
// traffic to the dedicated pod by CONNECT auth_token. Two key families are
// written per resource:
//
//	<tokenPrefix><token>     — debugging / future expansion
//	<routePrefix><token>     — consumed by nats-proxy to demux by auth_token
//
// Safe to call once at startup; subsequent calls overwrite. Passing rdb=nil
// disables route registration (default).
func (b *K8sBackend) EnableRouteRegistry(rdb *goredis.Client, prefix string) {
	if prefix == "" {
		prefix = "nats_route:"
	}
	b.rdb = rdb
	b.tokenPrefix = prefix
	if b.routePrefix == "" {
		b.routePrefix = "nats_route_by_token:"
	}
}

// SetTokenRoutePrefix overrides the token→backend key family consumed by the
// nats-proxy. Default "nats_route_by_token:" matches the nats-proxy default.
// Must be called before any Provision to take effect.
func (b *K8sBackend) SetTokenRoutePrefix(prefix string) {
	if prefix == "" {
		return
	}
	b.routePrefix = prefix
}

// SetPublicHost sets the hostname embedded in customer connection URLs when
// the nats-proxy is fronting the cluster. Empty value keeps the legacy
// K8S_EXTERNAL_HOST + NodePort URL shape.
func (b *K8sBackend) SetPublicHost(host string) { b.publicHost = host }

// Provision creates a dedicated NATS instance with JetStream enabled.
func (b *K8sBackend) Provision(ctx context.Context, token, tier string) (*Credentials, error) {
	ns := natsK8sNsPrefix + token

	rollback := func(step string, cause error) error {
		slog.Error("k8s.nats.provision.rollback", "step", step, "namespace", ns, "error", cause)
		_ = b.cs.CoreV1().Namespaces().Delete(context.Background(), ns, metav1.DeleteOptions{})
		return fmt.Errorf("k8s nats: %s: %w", step, cause)
	}

	// Use a fresh background context — pod startup can take minutes, far exceeding
	// any gRPC request deadline on the incoming ctx.
	// Carry the teamID value forward so applyNamespace can label the namespace
	// with instant.dev/owner-team (pentest 2026-05-16 fix).
	provCtx, provCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer provCancel()
	if teamID, ok := ctx.Value(ctxkeys.TeamIDKey).(string); ok && teamID != "" {
		provCtx = context.WithValue(provCtx, ctxkeys.TeamIDKey, teamID)
	}

	sz := sizingForTier(tier)

	if err := b.applyNamespace(provCtx, ns); err != nil {
		return nil, fmt.Errorf("k8s nats: namespace: %w", err)
	}
	if err := b.applyNetworkPolicy(provCtx, ns); err != nil {
		return nil, rollback("network policy", err)
	}
	if err := b.applyResourceQuota(provCtx, ns, sz); err != nil {
		return nil, rollback("resource quota", err)
	}
	if sz.pvcMi > 0 {
		if err := b.applyPVC(provCtx, ns, sz); err != nil {
			return nil, rollback("pvc", err)
		}
	}
	if err := b.applyDeployment(provCtx, ns, sz); err != nil {
		return nil, rollback("deployment", err)
	}
	svc, err := b.applyService(provCtx, ns)
	if err != nil {
		return nil, rollback("service", err)
	}

	if err := b.waitPodReady(provCtx, ns); err != nil {
		return nil, rollback("wait ready", err)
	}

	// P2 (W3 T2): a NodePort Service's .spec.ports[].nodePort is allocated by
	// the apiserver. The Create response usually carries it, but not always —
	// re-Get the Service once if it is still 0 so the legacy fallback URL never
	// embeds a dead "nats://host:0". This only matters when publicHost is unset
	// (the nats-proxy path does not use the NodePort at all).
	nodePort := int(svc.Spec.Ports[0].NodePort)
	if nodePort == 0 && b.publicHost == "" {
		if fresh, getErr := b.cs.CoreV1().Services(ns).Get(provCtx, "nats", metav1.GetOptions{}); getErr != nil {
			slog.Warn("k8s.nats.nodeport_refetch_failed", "namespace", ns, "error", getErr)
		} else if len(fresh.Spec.Ports) > 0 {
			nodePort = int(fresh.Spec.Ports[0].NodePort)
		}
		if nodePort == 0 {
			return nil, rollback("nodeport allocation", fmt.Errorf("service %s/nats has no allocated NodePort", ns))
		}
	}

	// Customer-facing URL.
	//   With publicHost set (typical prod): nats://<token>@nats.instanode.dev:4222
	//     — the nats-proxy demuxes by CONNECT auth_token and forwards to the right pod.
	//   Without publicHost (legacy / dev without the proxy): falls back to the
	//     NodePort URL so the resource is still reachable from outside the cluster.
	var connURL string
	if b.publicHost != "" {
		connURL = fmt.Sprintf("nats://%s@%s:4222", token, b.publicHost)
	} else {
		connURL = fmt.Sprintf("nats://%s:%d", b.externalHost, nodePort)
	}

	// Derive the SubjectPrefix from the FULL token — see subjident.go. The
	// dedicated pod is the real isolation boundary here, but the pre-fix
	// token[:8] truncation was the same token-truncation bug class, so it is
	// fixed identically. The prefix is deterministic from the token, so the
	// canonical derivation also resolves it for any future lifecycle lookup.
	prefix := canonicalSubjectPrefix(token)

	// Route records consumed by nats-proxy. Failure here does NOT fail the
	// provision — the pod is functional over its NodePort, and customers using
	// the public URL will get a clear "Authorization Violation" at the proxy
	// if the lookup fails. Worth surfacing via slog.Warn.
	if b.rdb != nil {
		serviceFQDN := fmt.Sprintf("nats.%s.svc.cluster.local:4222", ns)
		regCtx, regCancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := b.rdb.Set(regCtx, b.tokenPrefix+token, serviceFQDN, 0).Err(); err != nil {
			slog.Warn("k8s.nats.route_register_failed", "token", token, "error", err)
		} else {
			slog.Info("k8s.nats.route_registered", "token", token, "backend", serviceFQDN)
		}
		// The proxy consumes THIS key — it's the one that actually matters for
		// external connectivity through nats.instanode.dev.
		if err := b.rdb.Set(regCtx, b.routePrefix+token, serviceFQDN, 0).Err(); err != nil {
			slog.Warn("k8s.nats.token_route_register_failed", "token", token, "error", err)
		} else {
			slog.Info("k8s.nats.token_route_registered", "token", token, "backend", serviceFQDN)
		}
		regCancel()
	}

	slog.Info("k8s.nats.provisioned", "namespace", ns, "node_port", nodePort, "tier", tier, "pvc_mi", sz.pvcMi, "public_host", b.publicHost)
	return &Credentials{
		URL:                connURL,
		SubjectPrefix:      prefix,
		ProviderResourceID: ns,
	}, nil
}

// Deprovision deletes the customer namespace (cascading GC of all resources).
// When route registration is enabled, the route record is removed so callers of
// the future routing proxy fail fast instead of hitting a stale ClusterIP.
func (b *K8sBackend) Deprovision(ctx context.Context, token, providerResourceID string) error {
	ns := providerResourceID
	if ns == "" {
		ns = natsK8sNsPrefix + token
	}
	if err := b.cs.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("k8s nats.Deprovision: delete namespace %s: %w", ns, err)
	}
	if b.rdb != nil {
		delCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		keys := []string{b.tokenPrefix + token, b.routePrefix + token}
		if err := b.rdb.Del(delCtx, keys...).Err(); err != nil {
			slog.Warn("k8s.nats.route_unregister_failed", "token", token, "error", err)
		}
	}
	slog.Info("k8s.nats.deprovisioned", "namespace", ns)
	return nil
}

// --- private resource creators ---

func (b *K8sBackend) applyNamespace(ctx context.Context, ns string) error {
	labels := map[string]string{
		natsK8sRoleLabel:                     natsK8sRoleValue,
		"pod-security.kubernetes.io/enforce": "baseline",
		"pod-security.kubernetes.io/warn":    "restricted",
	}
	// SECURITY FIX (pentest 2026-05-16): label the namespace with the owning
	// team ID so that the deploy-side NetworkPolicy can scope DB-port egress
	// to this team's namespaces only (matchLabels on both role + owner-team),
	// preventing cross-tenant database access.
	if teamID, ok := ctx.Value(ctxkeys.TeamIDKey).(string); ok && teamID != "" {
		labels[natsK8sOwnerTeamLabel] = teamID
	}
	nsObj := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   ns,
			Labels: labels,
		},
	}
	_, err := b.cs.CoreV1().Namespaces().Create(ctx, nsObj, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !k8serrors.IsAlreadyExists(err) {
		return err
	}
	existing, getErr := b.cs.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if getErr != nil || existing.Status.Phase != corev1.NamespaceTerminating {
		return err
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

func (b *K8sBackend) applyNetworkPolicy(ctx context.Context, ns string) error {
	proto := corev1.ProtocolTCP
	udp := corev1.ProtocolUDP
	natsP := intstr.FromInt32(4222)
	monP := intstr.FromInt32(8222)
	dns := intstr.FromInt32(53)
	_, err := b.cs.NetworkingV1().NetworkPolicies(ns).Create(ctx, &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "default-deny"},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{Ports: []networkingv1.NetworkPolicyPort{
					{Protocol: &proto, Port: &natsP},
					{Protocol: &proto, Port: &monP},
				}},
			},
			Egress: []networkingv1.NetworkPolicyEgressRule{
				{
					Ports: []networkingv1.NetworkPolicyPort{
						{Protocol: &proto, Port: &dns},
						{Protocol: &udp, Port: &dns},
					},
					To: []networkingv1.NetworkPolicyPeer{{NamespaceSelector: &metav1.LabelSelector{}}},
				},
			},
		},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applyResourceQuota(ctx context.Context, ns string, sz tierSizing) error {
	hard := corev1.ResourceList{
		corev1.ResourceRequestsCPU:    resource.MustParse(sz.qCPURequests),
		corev1.ResourceRequestsMemory: resource.MustParse(sz.qMemRequests),
		corev1.ResourceLimitsCPU:      resource.MustParse(sz.qCPULimits),
		corev1.ResourceLimitsMemory:   resource.MustParse(sz.qMemLimits),
		corev1.ResourcePods:           resource.MustParse("3"),
	}
	if sz.pvcMi > 0 {
		hard["persistentvolumeclaims"] = resource.MustParse("2")
	}
	_, err := b.cs.CoreV1().ResourceQuotas(ns).Create(ctx, &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "tenant-quota"},
		Spec:       corev1.ResourceQuotaSpec{Hard: hard},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applyPVC(ctx context.Context, ns string, sz tierSizing) error {
	sc := b.storageClass
	_, err := b.cs.CoreV1().PersistentVolumeClaims(ns).Create(ctx, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "nats-jetstream"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &sc,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(fmt.Sprintf("%dMi", sz.pvcMi)),
				},
			},
		},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applyDeployment(ctx context.Context, ns string, sz tierSizing) error {
	replicas := int32(1)
	noPrivEsc := false
	runAsUser := int64(1000)
	fsGroup := int64(1000)

	// JetStream args:
	//   anonymous (no PVC): "-js" only — JetStream stays in-memory.
	//   hobby+ (with PVC):  "-js -sd /data" — JetStream persists to PVC.
	args := []string{"-js", "-m", "8222"}
	volumeMounts := []corev1.VolumeMount{
		{Name: "tmp", MountPath: "/tmp"},
	}
	volumes := []corev1.Volume{
		{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
	if sz.pvcMi > 0 {
		args = []string{"-js", "-sd", "/data", "-m", "8222"}
		volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: "data", MountPath: "/data"})
		volumes = append(volumes, corev1.Volume{
			Name: "data",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "nats-jetstream"},
			},
		})
	}

	_, err := b.cs.AppsV1().Deployments(ns).Create(ctx, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "nats"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "nats"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "nats"}},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: natsK8sBoolPtr(false),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: natsK8sBoolPtr(true),
						RunAsUser:    &runAsUser,
						FSGroup:      &fsGroup,
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Containers: []corev1.Container{{
						Name:  "nats",
						Image: b.image,
						Args:  args,
						Ports: []corev1.ContainerPort{
							{ContainerPort: 4222, Protocol: corev1.ProtocolTCP},
							{ContainerPort: 8222, Protocol: corev1.ProtocolTCP},
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/healthz",
									Port: intstr.FromInt32(8222),
								},
							},
							InitialDelaySeconds: 5,
							PeriodSeconds:       5,
							FailureThreshold:    12,
						},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &noPrivEsc,
							ReadOnlyRootFilesystem:   natsK8sBoolPtr(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse(sz.cpuReq),
								corev1.ResourceMemory: resource.MustParse(sz.memReq),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse(sz.cpuLim),
								corev1.ResourceMemory: resource.MustParse(sz.memLim),
							},
						},
						VolumeMounts: volumeMounts,
					}},
					Volumes: volumes,
				},
			},
		},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applyService(ctx context.Context, ns string) (*corev1.Service, error) {
	return b.cs.CoreV1().Services(ns).Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "nats"},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeNodePort,
			Selector: map[string]string{"app": "nats"},
			Ports: []corev1.ServicePort{
				{Name: "client", Port: 4222, TargetPort: intstr.FromInt32(4222), Protocol: corev1.ProtocolTCP},
			},
		},
	}, metav1.CreateOptions{})
}

func (b *K8sBackend) waitPodReady(ctx context.Context, ns string) error {
	deadline := time.Now().Add(natsK8sReadyTO)
	for time.Now().Before(deadline) {
		pods, err := b.cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "app=nats"})
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
		case <-time.After(natsK8sReadyPoll):
		}
	}
	return fmt.Errorf("nats pod not ready after %s", natsK8sReadyTO)
}

// k8sEnv — small helper for env-var fallback used by NewBackend.
func k8sEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// NewK8sDedicatedBackend creates a K8sBackend for pro/team-tier NATS provisioning.
// Each token gets its own k8s namespace with a dedicated NATS pod.
func NewK8sDedicatedBackend(kubeconfigPath, image, externalHost string) (Backend, error) {
	return newK8sBackend(kubeconfigPath, "", image, externalHost)
}

func natsK8sBoolPtr(b bool) *bool { return &b }
