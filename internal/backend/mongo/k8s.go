package mongo

// k8s.go — K8sBackend provisions a dedicated MongoDB pod per token in its own namespace.
// Security model mirrors postgres/k8s.go — see that file for architecture notes.
//
// Configuration env vars:
//   K8S_EXTERNAL_HOST      — hostname in returned URLs (required)
//   K8S_MONGO_IMAGE        — container image, default "mongo:7"
//   K8S_STORAGE_CLASS      — PVC storage class, default "gp3"
//   K8S_MONGO_STORAGE_GI   — PVC size in GiB, default 50
//   K8S_KUBECONFIG         — path to kubeconfig; empty = in-cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	mongoclient "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
	mongoK8sNsPrefix  = "instant-customer-"
	mongoK8sRoleLabel = "instant.dev/role"
	mongoK8sRoleValue = "customer-resource"
	mongoK8sReadyTO   = 3 * time.Minute
	mongoK8sReadyPoll = 3 * time.Second
)

// K8sBackend provisions a dedicated MongoDB pod per token.
type K8sBackend struct {
	cs            *kubernetes.Clientset
	storageClass  string // K8S_STORAGE_CLASS
	image         string // K8S_MONGO_IMAGE
	externalHost  string // K8S_EXTERNAL_HOST
	storageSizeGi int    // K8S_MONGO_STORAGE_GI
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
		return nil, fmt.Errorf("k8s mongo: build config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("k8s mongo: new clientset: %w", err)
	}
	if storageClass == "" {
		storageClass = "gp3"
	}
	if image == "" {
		image = "mongo:7"
	}
	if storageSizeGi <= 0 {
		storageSizeGi = 50
	}
	return &K8sBackend{cs: cs, storageClass: storageClass, image: image, externalHost: externalHost, storageSizeGi: storageSizeGi}, nil
}

// Provision creates a dedicated MongoDB instance with a restricted app user.
func (b *K8sBackend) Provision(ctx context.Context, token, tier string) (*Credentials, error) {
	ns := mongoK8sNsPrefix + token
	dbName := "db_" + mongoK8sShort(token)
	adminPass, err := mongoK8sRandHex(16)
	if err != nil {
		return nil, fmt.Errorf("k8s mongo: rand admin pass: %w", err)
	}
	appUser := "usr_" + mongoK8sShort(token)
	appPass, err := mongoK8sRandHex(16)
	if err != nil {
		return nil, fmt.Errorf("k8s mongo: rand app pass: %w", err)
	}

	rollback := func(step string, cause error) error {
		slog.Error("k8s.mongo.provision.rollback", "step", step, "namespace", ns, "error", cause)
		_ = b.cs.CoreV1().Namespaces().Delete(context.Background(), ns, metav1.DeleteOptions{})
		return fmt.Errorf("k8s mongo: %s: %w", step, cause)
	}

	// Use a fresh background context — pod startup can take minutes, far exceeding
	// any gRPC request deadline on the incoming ctx.
	provCtx, provCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer provCancel()

	if err := b.applyNamespace(provCtx, ns); err != nil {
		return nil, fmt.Errorf("k8s mongo: namespace: %w", err)
	}
	if err := b.applyNetworkPolicy(provCtx, ns, 27017); err != nil {
		return nil, rollback("network policy", err)
	}
	if err := b.applyResourceQuota(provCtx, ns); err != nil {
		return nil, rollback("resource quota", err)
	}
	if err := b.applyAdminSecret(provCtx, ns, adminPass); err != nil {
		return nil, rollback("admin secret", err)
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

	if err := b.waitPodReady(provCtx, ns, "app=mongodb"); err != nil {
		return nil, rollback("wait ready", err)
	}

	clusterIP := svc.Spec.ClusterIP
	nodePort := int(svc.Spec.Ports[0].NodePort)

	adminURI := fmt.Sprintf("mongodb://root:%s@%s:27017/admin", adminPass, clusterIP)
	if err := b.initMongo(provCtx, adminURI, dbName, appUser, appPass); err != nil {
		return nil, rollback("init mongo", err)
	}

	connURL := fmt.Sprintf("mongodb://%s:%s@%s:%d/%s?authSource=%s",
		appUser, appPass, b.externalHost, nodePort, dbName, dbName)

	slog.Info("k8s.mongo.provisioned", "namespace", ns, "node_port", nodePort)
	return &Credentials{URL: connURL, DatabaseName: dbName, ProviderResourceID: ns}, nil
}

// StorageBytes returns the storageSize from dbStats for the customer database.
func (b *K8sBackend) StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error) {
	ns := providerResourceID
	if ns == "" {
		ns = mongoK8sNsPrefix + token
	}
	dbName := "db_" + mongoK8sShort(token)

	secret, err := b.cs.CoreV1().Secrets(ns).Get(ctx, "mongo-admin", metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("k8s mongo.StorageBytes: get secret: %w", err)
	}
	svc, err := b.cs.CoreV1().Services(ns).Get(ctx, "mongodb", metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("k8s mongo.StorageBytes: get service: %w", err)
	}

	adminPass := string(secret.Data["MONGO_INITDB_ROOT_PASSWORD"])
	uri := fmt.Sprintf("mongodb://root:%s@%s:27017/admin", adminPass, svc.Spec.ClusterIP)

	clientOpts := options.Client().ApplyURI(uri).SetServerSelectionTimeout(5 * time.Second)
	client, err := mongoclient.Connect(ctx, clientOpts)
	if err != nil {
		return 0, fmt.Errorf("k8s mongo.StorageBytes: connect: %w", err)
	}
	defer client.Disconnect(ctx)

	var result bson.M
	if err := client.Database(dbName).RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}}).Decode(&result); err != nil {
		return 0, fmt.Errorf("k8s mongo.StorageBytes: dbStats: %w", err)
	}
	if v, ok := result["storageSize"]; ok {
		switch n := v.(type) {
		case int32:
			return int64(n), nil
		case int64:
			return n, nil
		case float64:
			return int64(n), nil
		}
	}
	return 0, nil
}

// Deprovision deletes the customer namespace (cascading GC of all resources).
func (b *K8sBackend) Deprovision(ctx context.Context, token, providerResourceID string) error {
	ns := providerResourceID
	if ns == "" {
		ns = mongoK8sNsPrefix + token
	}
	if err := b.cs.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("k8s mongo.Deprovision: delete namespace %s: %w", ns, err)
	}
	slog.Info("k8s.mongo.deprovisioned", "namespace", ns)
	return nil
}

// --- private resource creators ---

func (b *K8sBackend) applyNamespace(ctx context.Context, ns string) error {
	nsObj := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: ns,
			Labels: map[string]string{
				mongoK8sRoleLabel:                    mongoK8sRoleValue,
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

func (b *K8sBackend) applyAdminSecret(ctx context.Context, ns, adminPass string) error {
	_, err := b.cs.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mongo-admin"},
		StringData: map[string]string{
			"MONGO_INITDB_ROOT_USERNAME": "root",
			"MONGO_INITDB_ROOT_PASSWORD": adminPass,
		},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applyPVC(ctx context.Context, ns string) error {
	sc := b.storageClass
	_, err := b.cs.CoreV1().PersistentVolumeClaims(ns).Create(ctx, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "mongo-data"},
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
	runAsUser := int64(999) // mongodb UID in the official mongo:7 image
	fsGroup := int64(999)   // mongodb GID

	_, err := b.cs.AppsV1().Deployments(ns).Create(ctx, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "mongodb"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "mongodb"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "mongodb"}},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: mongoK8sBoolPtr(false),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsUser:  &runAsUser,
						RunAsGroup: &fsGroup,
						FSGroup:    &fsGroup,
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Containers: []corev1.Container{{
						Name:  "mongodb",
						Image: b.image,
						Ports: []corev1.ContainerPort{{ContainerPort: 27017, Protocol: corev1.ProtocolTCP}},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(27017)},
							},
							InitialDelaySeconds: 10,
							PeriodSeconds:       3,
							FailureThreshold:    30,
						},
						EnvFrom: []corev1.EnvFromSource{{
							SecretRef: &corev1.SecretEnvSource{
								LocalObjectReference: corev1.LocalObjectReference{Name: "mongo-admin"},
							},
						}},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &noPrivEsc,
							RunAsNonRoot:             mongoK8sBoolPtr(true),
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
							{Name: "data", MountPath: "/data/db"},
							{Name: "tmp", MountPath: "/tmp"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "data", VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "mongo-data"},
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
		ObjectMeta: metav1.ObjectMeta{Name: "mongodb"},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeNodePort,
			Selector: map[string]string{"app": "mongodb"},
			Ports:    []corev1.ServicePort{{Port: 27017, TargetPort: intstr.FromInt32(27017), Protocol: corev1.ProtocolTCP}},
		},
	}, metav1.CreateOptions{})
}

func (b *K8sBackend) waitPodReady(ctx context.Context, ns, labelSelector string) error {
	deadline := time.Now().Add(mongoK8sReadyTO)
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
		case <-time.After(mongoK8sReadyPoll):
		}
	}
	return fmt.Errorf("mongodb pod not ready after %s", mongoK8sReadyTO)
}

// initMongo connects as admin and creates a restricted app user.
// The user is created IN the customer database (not admin), so the connection URL
// can use authSource=dbName — the user's authenticating database matches where they live.
func (b *K8sBackend) initMongo(ctx context.Context, adminURI, dbName, appUser, appPass string) error {
	clientOpts := options.Client().ApplyURI(adminURI).SetServerSelectionTimeout(10 * time.Second)
	client, err := mongoclient.Connect(ctx, clientOpts)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer client.Disconnect(ctx)

	// Create the user in dbName (not admin). This way authSource=dbName works in the
	// connection URL. MongoDB creates the database implicitly on first write.
	customerDB := client.Database(dbName)
	cmd := bson.D{
		{Key: "createUser", Value: appUser},
		{Key: "pwd", Value: appPass},
		{Key: "roles", Value: bson.A{
			bson.D{
				{Key: "role", Value: "readWrite"},
				{Key: "db", Value: dbName},
			},
		}},
	}
	if err := customerDB.RunCommand(ctx, cmd).Err(); err != nil {
		return fmt.Errorf("createUser: %w", err)
	}
	return nil
}

func mongoK8sShort(token string) string {
	s := strings.ReplaceAll(token, "-", "")
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

func mongoK8sRandHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func mongoK8sBoolPtr(b bool) *bool { return &b }
