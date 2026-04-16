package redis

// k8s.go — K8sBackend provisions a dedicated Redis pod per token in its own namespace.
// Security model and architecture mirrors the postgres K8sBackend — see postgres/k8s.go.
//
// Configuration env vars:
//   K8S_EXTERNAL_HOST      — hostname in returned URLs (required for k8s backend)
//   K8S_REDIS_IMAGE        — container image, default "redis:7-alpine"
//   K8S_STORAGE_CLASS      — PVC storage class, default "gp3"
//   K8S_REDIS_STORAGE_GI   — PVC size in GiB, default 10
//   K8S_KUBECONFIG         — path to kubeconfig file; empty = in-cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
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
)

const (
	redisK8sNsPrefix    = "instant-customer-"
	redisK8sRoleLabel   = "instant.dev/role"
	redisK8sRoleValue   = "customer-resource"
	redisK8sReadyTO     = 3 * time.Minute
	redisK8sReadyPoll   = 3 * time.Second
)

// K8sBackend provisions a dedicated Redis pod per token.
type K8sBackend struct {
	cs            *kubernetes.Clientset
	storageClass  string // K8S_STORAGE_CLASS
	image         string // K8S_REDIS_IMAGE
	externalHost  string // K8S_EXTERNAL_HOST
	storageSizeGi int    // K8S_REDIS_STORAGE_GI
}

func newK8sBackend(kubeconfigPath, storageClass, image, externalHost string, storageSizeGi int) (*K8sBackend, error) {
	var rc *rest.Config
	var err error
	if kubeconfigPath != "" {
		rc, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	} else {
		rc, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("k8s redis: build config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("k8s redis: new clientset: %w", err)
	}
	if storageClass == "" {
		storageClass = "gp3"
	}
	if image == "" {
		image = "redis:7-alpine"
	}
	if storageSizeGi <= 0 {
		storageSizeGi = 10
	}
	return &K8sBackend{cs: cs, storageClass: storageClass, image: image, externalHost: externalHost, storageSizeGi: storageSizeGi}, nil
}

// Provision creates a dedicated Redis instance. The pod is started with --requirepass
// injected via a k8s Secret — no init step needed (unlike Postgres).
func (b *K8sBackend) Provision(ctx context.Context, token, tier string) (*Credentials, error) {
	ns := redisK8sNsPrefix + token
	password, err := redisK8sRandHex(16)
	if err != nil {
		return nil, fmt.Errorf("k8s redis: rand pass: %w", err)
	}

	rollback := func(step string, cause error) error {
		slog.Error("k8s.redis.provision.rollback", "step", step, "namespace", ns, "error", cause)
		_ = b.cs.CoreV1().Namespaces().Delete(context.Background(), ns, metav1.DeleteOptions{})
		return fmt.Errorf("k8s redis: %s: %w", step, cause)
	}

	// Use a fresh background context — pod startup can take minutes, far exceeding
	// any gRPC request deadline on the incoming ctx.
	provCtx, provCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer provCancel()

	if err := b.applyNamespace(provCtx, ns); err != nil {
		return nil, fmt.Errorf("k8s redis: namespace: %w", err)
	}
	if err := b.applyNetworkPolicy(provCtx, ns, 6379); err != nil {
		return nil, rollback("network policy", err)
	}
	if err := b.applyResourceQuota(provCtx, ns); err != nil {
		return nil, rollback("resource quota", err)
	}
	if err := b.applySecret(provCtx, ns, password); err != nil {
		return nil, rollback("secret", err)
	}
	if err := b.applyPVC(provCtx, ns); err != nil {
		return nil, rollback("pvc", err)
	}
	if err := b.applyDeployment(provCtx, ns); err != nil {
		return nil, rollback("deployment", err)
	}
	svc, err := b.applyService(provCtx, ns)
	if err != nil {
		return nil, rollback("service", err)
	}

	if err := b.waitPodReady(provCtx, ns, "app=redis"); err != nil {
		return nil, rollback("wait ready", err)
	}

	nodePort := int(svc.Spec.Ports[0].NodePort)
	connURL := fmt.Sprintf("redis://:%s@%s:%d/0", password, b.externalHost, nodePort)

	slog.Info("k8s.redis.provisioned", "namespace", ns, "node_port", nodePort)
	return &Credentials{URL: connURL, KeyPrefix: "", ProviderResourceID: ns}, nil
}

// StorageBytes returns used_memory from the Redis INFO command.
func (b *K8sBackend) StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error) {
	ns := providerResourceID
	if ns == "" {
		ns = redisK8sNsPrefix + token
	}

	secret, err := b.cs.CoreV1().Secrets(ns).Get(ctx, "redis-auth", metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("k8s redis.StorageBytes: get secret: %w", err)
	}
	svc, err := b.cs.CoreV1().Services(ns).Get(ctx, "redis", metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("k8s redis.StorageBytes: get service: %w", err)
	}

	password := string(secret.Data["REDIS_PASSWORD"])
	rdb := goredis.NewClient(&goredis.Options{
		Addr:     fmt.Sprintf("%s:6379", svc.Spec.ClusterIP),
		Password: password,
	})
	defer rdb.Close()

	info, err := rdb.Info(ctx, "memory").Result()
	if err != nil {
		return 0, fmt.Errorf("k8s redis.StorageBytes: INFO memory: %w", err)
	}
	return parseUsedMemory(info), nil
}

// Deprovision deletes the customer namespace (cascading GC of all resources).
func (b *K8sBackend) Deprovision(ctx context.Context, token, providerResourceID string) error {
	ns := providerResourceID
	if ns == "" {
		ns = redisK8sNsPrefix + token
	}
	if err := b.cs.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("k8s redis.Deprovision: delete namespace %s: %w", ns, err)
	}
	slog.Info("k8s.redis.deprovisioned", "namespace", ns)
	return nil
}

// --- private resource creators ---

func (b *K8sBackend) applyNamespace(ctx context.Context, ns string) error {
	nsObj := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: ns,
			Labels: map[string]string{
				redisK8sRoleLabel:                    redisK8sRoleValue,
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
					To: []networkingv1.NetworkPolicyPeer{{NamespaceSelector: &metav1.LabelSelector{}}},
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
				corev1.ResourceRequestsCPU:    resource.MustParse("100m"),
				corev1.ResourceRequestsMemory: resource.MustParse("256Mi"),
				corev1.ResourceLimitsCPU:      resource.MustParse("500m"),
				corev1.ResourceLimitsMemory:   resource.MustParse("1Gi"),
				"persistentvolumeclaims":      resource.MustParse("2"),
				corev1.ResourcePods:           resource.MustParse("3"),
			},
		},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applySecret(ctx context.Context, ns, password string) error {
	_, err := b.cs.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-auth"},
		StringData: map[string]string{"REDIS_PASSWORD": password},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applyPVC(ctx context.Context, ns string) error {
	sc := b.storageClass
	_, err := b.cs.CoreV1().PersistentVolumeClaims(ns).Create(ctx, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-data"},
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

func (b *K8sBackend) applyDeployment(ctx context.Context, ns string) error {
	replicas := int32(1)
	noPrivEsc := false
	runAsUser := int64(999) // redis user in redis:7-alpine
	fsGroup := int64(999)

	_, err := b.cs.AppsV1().Deployments(ns).Create(ctx, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "redis"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "redis"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "redis"}},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: boolPtrR(false),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: boolPtrR(true),
						RunAsUser:    &runAsUser,
						FSGroup:      &fsGroup,
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Containers: []corev1.Container{{
						Name:  "redis",
						Image: b.image,
						// Pass requirepass via env var — redis:7-alpine supports REDIS_PASSWORD
						// via the docker-entrypoint.sh if set; otherwise pass via command args.
						Command: []string{"redis-server", "--requirepass", "$(REDIS_PASSWORD)", "--appendonly", "yes", "--dir", "/data"},
						Env: []corev1.EnvVar{{
							Name: "REDIS_PASSWORD",
							ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: "redis-auth"},
									Key:                  "REDIS_PASSWORD",
								},
							},
						}},
						Ports: []corev1.ContainerPort{{ContainerPort: 6379, Protocol: corev1.ProtocolTCP}},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(6379)},
							},
							InitialDelaySeconds: 2,
							PeriodSeconds:       2,
							FailureThreshold:    30,
						},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &noPrivEsc,
							ReadOnlyRootFilesystem:   boolPtrR(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("50m"),
								corev1.ResourceMemory: resource.MustParse("128Mi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("500m"),
								corev1.ResourceMemory: resource.MustParse("1Gi"),
							},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/data"},
							{Name: "tmp", MountPath: "/tmp"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "data", VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "redis-data"},
						}},
						{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applyService(ctx context.Context, ns string) (*corev1.Service, error) {
	return b.cs.CoreV1().Services(ns).Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "redis"},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeNodePort,
			Selector: map[string]string{"app": "redis"},
			Ports:    []corev1.ServicePort{{Port: 6379, TargetPort: intstr.FromInt32(6379), Protocol: corev1.ProtocolTCP}},
		},
	}, metav1.CreateOptions{})
}

func (b *K8sBackend) waitPodReady(ctx context.Context, ns, labelSelector string) error {
	deadline := time.Now().Add(redisK8sReadyTO)
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
		case <-time.After(redisK8sReadyPoll):
		}
	}
	return fmt.Errorf("redis pod not ready after %s", redisK8sReadyTO)
}

// parseUsedMemory extracts used_memory from Redis INFO memory output.
func parseUsedMemory(info string) int64 {
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "used_memory:") {
			var n int64
			fmt.Sscanf(strings.TrimPrefix(line, "used_memory:"), "%d", &n)
			return n
		}
	}
	return 0
}

func redisK8sRandHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func boolPtrR(b bool) *bool { return &b }
