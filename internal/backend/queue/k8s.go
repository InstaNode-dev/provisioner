package queue

// k8s.go — K8sBackend provisions a dedicated NATS pod per token in its own namespace.
// Security model mirrors redis/k8s.go.
//
// Configuration env vars:
//   K8S_EXTERNAL_HOST    — hostname in returned URLs (required)
//   K8S_NATS_IMAGE       — container image, default "nats:2.10-alpine"
//   K8S_KUBECONFIG       — path to kubeconfig; empty = in-cluster

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

const (
	natsK8sNsPrefix  = "instant-customer-"
	natsK8sRoleLabel = "instant.dev/role"
	natsK8sRoleValue = "customer-resource"
	natsK8sReadyTO   = 3 * time.Minute
	natsK8sReadyPoll = 3 * time.Second
)

// K8sBackend provisions a dedicated NATS pod per token.
type K8sBackend struct {
	cs           *kubernetes.Clientset
	image        string // K8S_NATS_IMAGE
	externalHost string // K8S_EXTERNAL_HOST
	httpClient   *http.Client
}

func newK8sBackend(kubeconfigPath, image, externalHost string) (*K8sBackend, error) {
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
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("k8s nats: new clientset: %w", err)
	}
	if image == "" {
		image = "nats:2.10-alpine"
	}
	return &K8sBackend{
		cs:           cs,
		image:        image,
		externalHost: externalHost,
		httpClient:   &http.Client{Timeout: 10 * time.Second},
	}, nil
}

// Provision creates a dedicated NATS pod with JetStream enabled.
func (b *K8sBackend) Provision(ctx context.Context, token, tier string) (*Credentials, error) {
	ns := natsK8sNsPrefix + token

	rollback := func(step string, cause error) error {
		slog.Error("k8s.nats.provision.rollback", "step", step, "namespace", ns, "error", cause)
		_ = b.cs.CoreV1().Namespaces().Delete(context.Background(), ns, metav1.DeleteOptions{})
		return fmt.Errorf("k8s nats: %s: %w", step, cause)
	}

	provCtx, provCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer provCancel()

	if err := b.applyNamespace(provCtx, ns); err != nil {
		return nil, fmt.Errorf("k8s nats: namespace: %w", err)
	}
	if err := b.applyNetworkPolicy(provCtx, ns); err != nil {
		return nil, rollback("network policy", err)
	}
	if err := b.applyResourceQuota(provCtx, ns); err != nil {
		return nil, rollback("resource quota", err)
	}
	if err := b.applyDeployment(provCtx, ns); err != nil {
		return nil, rollback("deployment", err)
	}
	svc, err := b.applyService(provCtx, ns)
	if err != nil {
		return nil, rollback("service", err)
	}

	if err := b.waitPodReady(provCtx, ns); err != nil {
		return nil, rollback("wait ready", err)
	}

	nodePort := int(svc.Spec.Ports[0].NodePort)
	connURL := fmt.Sprintf("nats://%s:%d", b.externalHost, nodePort)

	prefix := token
	if len(prefix) > 8 {
		prefix = prefix[:8]
	}

	slog.Info("k8s.nats.provisioned", "namespace", ns, "node_port", nodePort)
	return &Credentials{
		URL:                connURL,
		SubjectPrefix:      prefix + ".",
		ProviderResourceID: ns,
	}, nil
}

// Deprovision deletes the customer namespace (cascading GC of all resources).
func (b *K8sBackend) Deprovision(ctx context.Context, token, providerResourceID string) error {
	ns := providerResourceID
	if ns == "" {
		ns = natsK8sNsPrefix + token
	}
	if err := b.cs.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("k8s nats.Deprovision: delete namespace %s: %w", ns, err)
	}
	slog.Info("k8s.nats.deprovisioned", "namespace", ns)
	return nil
}

func (b *K8sBackend) applyNamespace(ctx context.Context, ns string) error {
	nsObj := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: ns,
			Labels: map[string]string{
				natsK8sRoleLabel:                     natsK8sRoleValue,
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

func (b *K8sBackend) applyResourceQuota(ctx context.Context, ns string) error {
	_, err := b.cs.CoreV1().ResourceQuotas(ns).Create(ctx, &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "tenant-quota"},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList{
				corev1.ResourceRequestsCPU:    resource.MustParse("50m"),
				corev1.ResourceRequestsMemory: resource.MustParse("64Mi"),
				corev1.ResourceLimitsCPU:      resource.MustParse("250m"),
				corev1.ResourceLimitsMemory:   resource.MustParse("256Mi"),
				corev1.ResourcePods:           resource.MustParse("2"),
			},
		},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applyDeployment(ctx context.Context, ns string) error {
	replicas := int32(1)
	noPrivEsc := false
	runAsUser := int64(1000)
	fsGroup := int64(1000)

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
						Args:  []string{"-js", "-m", "8222"},
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
								corev1.ResourceCPU:    resource.MustParse("50m"),
								corev1.ResourceMemory: resource.MustParse("64Mi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("250m"),
								corev1.ResourceMemory: resource.MustParse("256Mi"),
							},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "tmp", MountPath: "/tmp"},
						},
					}},
					Volumes: []corev1.Volume{
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

// NewBackend creates a Backend using the given backend type string.
func NewBackend(natsHost string) Backend {
	return newLocalBackend(natsHost)
}

// NewK8sDedicatedBackend creates a K8sBackend for pro/team-tier NATS provisioning.
func NewK8sDedicatedBackend(kubeconfigPath, image, externalHost string) (Backend, error) {
	return newK8sBackend(kubeconfigPath, image, externalHost)
}

func natsK8sBoolPtr(b bool) *bool { return &b }
