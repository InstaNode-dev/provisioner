package mongo

// coverage_extra_test.go — closes the remaining branches in mongo.go
// (LocalBackend) and the K8sBackend Provision/StorageBytes happy-path tails
// that the fake-clientset lifecycle tests could not reach without a running
// mongod.
//
// The LocalBackend tests use the no-auth Mongo at CUSTOMER_MONGO_URL (default
// mongodb://127.0.0.1:27017). The auth-fail branch additionally needs an
// authenticated Mongo whose URI is given by CUSTOMER_MONGO_AUTH_URL
// (e.g. mongodb://root:rootpw@127.0.0.1:27018). Both skip cleanly when absent.
//
// The K8sBackend Provision-tail tests inject initMongoFn (the documented test
// seam) + a Ready pod so the orchestration runs end-to-end against a fake
// clientset, exercising the publicHost / NodePort URL builders and the route
// registry writes without standing up a real pod.

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	goredis "github.com/redis/go-redis/v9"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/kubernetes/fake"

	"instant.dev/provisioner/internal/ctxkeys"
)

// TestDecodeStorageSize_AllBSONNumericTypes exercises every arm of the dbStats
// storageSize decoder, including the BSON integer encodings (int32/int64) that
// a live mongod never emits for storageSize (it returns float64) and the
// missing / non-numeric fall-through. This is the regression guard that keeps
// the shared decoder tolerant of every server-version encoding.
func TestDecodeStorageSize_AllBSONNumericTypes(t *testing.T) {
	cases := []struct {
		name string
		in   bson.M
		want int64
	}{
		{"int32", bson.M{"storageSize": int32(4096)}, 4096},
		{"int64", bson.M{"storageSize": int64(1 << 40)}, 1 << 40},
		{"float64", bson.M{"storageSize": float64(8192)}, 8192},
		{"absent", bson.M{"other": 1}, 0},
		{"nil-result", bson.M{}, 0},
		{"wrong-type", bson.M{"storageSize": "not-a-number"}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := decodeStorageSize(tc.in); got != tc.want {
				t.Errorf("decodeStorageSize(%v) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

// hostPortFromAuthURL extracts the bare host:port from a mongodb:// URI that
// MAY carry user:pass@ credentials, stripping any credentials, path, and query.
// hostFromURI does NOT strip credentials (it keeps everything before the first
// '/'), so a credentialed URL would round-trip its creds — defeating the
// auth-fail test. This helper drops them.
func hostPortFromAuthURL(uri string) string {
	u := strings.TrimPrefix(uri, "mongodb://")
	if at := strings.Index(u, "@"); at >= 0 {
		u = u[at+1:]
	}
	if i := strings.IndexAny(u, "/?"); i >= 0 {
		u = u[:i]
	}
	return u
}

// ─── LocalBackend: StorageBytes type-switch + insert-failure ────────────────

// TestLocalStorageBytes_DecodesNonZeroSize provisions a DB, writes enough data
// that dbStats reports a non-zero storageSize, and asserts StorageBytes decodes
// it. This drives the storageSize type-switch (int32/int64/float64) return arm
// that the empty-DB path never reaches.
func TestLocalStorageBytes_DecodesNonZeroSize(t *testing.T) {
	uri, ok := liveMongoURI(t)
	if !ok {
		t.Skip("CUSTOMER_MONGO_URL unreachable; skipping")
	}
	b := newLocalBackend(uri, hostFromURI(uri))
	token := uniqueToken("size-nonzero")
	defer func() { _ = b.Deprovision(context.Background(), token, "") }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := b.Provision(ctx, token, "hobby"); err != nil {
		t.Fatalf("Provision: %v", err)
	}

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Disconnect(ctx)
	coll := client.Database(mongoDBName(token)).Collection("bulk")
	docs := make([]interface{}, 0, 200)
	for i := 0; i < 200; i++ {
		docs = append(docs, bson.D{{Key: "i", Value: i}, {Key: "pad", Value: strings.Repeat("y", 256)}})
	}
	if _, err := coll.InsertMany(ctx, docs); err != nil {
		t.Fatalf("InsertMany: %v", err)
	}

	got, err := b.StorageBytes(ctx, token, "")
	if err != nil {
		t.Fatalf("StorageBytes: %v", err)
	}
	// storageSize for a DB with real data must be > 0 — proving the type-switch
	// decoded a concrete numeric value rather than falling through to 0.
	if got <= 0 {
		t.Errorf("StorageBytes after bulk insert = %d, want > 0", got)
	}
}

// TestLocalProvision_AuthFailReturnsError points the LocalBackend at an
// authenticated Mongo with NO credentials in the admin URI, so createUser is
// rejected with an authentication/authorization error — covering the
// result.Err() != nil branch of Provision under a real auth failure (distinct
// from the connect-parse failure already covered).
func TestLocalProvision_AuthFailReturnsError(t *testing.T) {
	authURL := os.Getenv("CUSTOMER_MONGO_AUTH_URL")
	if authURL == "" {
		t.Skip("CUSTOMER_MONGO_AUTH_URL unset; skipping auth-fail branch")
	}
	// Strip credentials so createUser is unauthorized.
	host := hostPortFromAuthURL(authURL)
	noCredURI := "mongodb://" + host
	b := newLocalBackend(noCredURI, host)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if _, err := b.Provision(ctx, uniqueToken("authfail"), "hobby"); err == nil {
		t.Fatal("Provision against authed mongo without creds: want error, got nil")
	}
}

// TestLocalDeprovision_AuthFailReturnsError exercises the Deprovision drop
// path against an authed mongo with no creds: the canonical-DB Drop is
// rejected, so the function returns the wrapped drop error.
func TestLocalDeprovision_AuthFailReturnsError(t *testing.T) {
	authURL := os.Getenv("CUSTOMER_MONGO_AUTH_URL")
	if authURL == "" {
		t.Skip("CUSTOMER_MONGO_AUTH_URL unset; skipping auth-fail branch")
	}
	host := hostPortFromAuthURL(authURL)
	b := newLocalBackend("mongodb://"+host, host)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := b.Deprovision(ctx, uniqueToken("authfail-drop"), ""); err == nil {
		t.Fatal("Deprovision against authed mongo without creds: want drop error, got nil")
	}
}

// TestK8sStorageBytes_AuthFailDrainsCandidates points the dbStats probe at the
// AUTHED mongo with a WRONG root password planted in the Secret. The connection
// is established but every candidate's dbStats RunCommand fails with
// AuthenticationFailed, so the loop drains each candidate through the
// lastErr=err;continue arm and the function returns the wrapped lastErr after
// the loop — covering the candidate-miss continue + post-loop error arms.
func TestK8sStorageBytes_AuthFailDrainsCandidates(t *testing.T) {
	authURL := os.Getenv("CUSTOMER_MONGO_AUTH_URL")
	if authURL == "" {
		t.Skip("CUSTOMER_MONGO_AUTH_URL unset; skipping auth-fail drain")
	}
	host := hostPortFromAuthURL(authURL)
	hostOnly := host
	portNum := 27017
	if i := strings.LastIndex(host, ":"); i >= 0 {
		hostOnly = host[:i]
		if n, perr := strconv.Atoi(host[i+1:]); perr == nil {
			portNum = n
		}
	}
	const token = "k8s-authfail"
	ns := mongoK8sNsPrefix + token
	cs := fake.NewSimpleClientset(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "mongo-admin", Namespace: ns},
			Data:       map[string][]byte{"MONGO_INITDB_ROOT_PASSWORD": []byte("wrong-password")},
		},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "mongodb", Namespace: ns},
			Spec:       corev1.ServiceSpec{ClusterIP: hostOnly},
		},
	)
	b := &K8sBackend{cs: cs, mongoPort: portNum}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := b.StorageBytes(ctx, token, ""); err == nil {
		t.Fatal("StorageBytes with wrong creds: want drained dbStats error, got nil")
	}
}

// ─── K8sBackend: applyNamespace terminating-wait ctx-cancel ─────────────────

// TestApplyNamespace_TerminatingCtxCancel exercises the ctx.Done() arm of the
// terminating-namespace wait loop: a namespace stuck in Terminating that never
// drains, with a short context, must return ctx.Err() from inside the loop
// rather than spinning to the 2-minute deadline.
func TestApplyNamespace_TerminatingCtxCancel(t *testing.T) {
	const ns = "instant-customer-mongo-stuckterm"
	cs := fake.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
		Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceTerminating},
	})
	b := &K8sBackend{cs: cs}
	// Context expires well before the loop's 3s poll completes its first
	// iteration's Get, so the select hits ctx.Done().
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	err := b.applyNamespace(ctx, ns)
	if err == nil {
		t.Fatal("applyNamespace: want ctx error for stuck-Terminating ns, got nil")
	}
}

// ─── LocalBackend: connect-error branches via the connectFn seam ────────────

// errConnect is a connectFn that always fails. The real mongo.Connect almost
// never errors (a bad URI surfaces lazily on first RunCommand), so the
// connect-error arms of Provision / StorageBytes / Deprovision are otherwise
// unreachable.
func errConnect(ctx context.Context, opts ...*options.ClientOptions) (*mongo.Client, error) {
	return nil, errSyntheticConnect
}

var errSyntheticConnect = mongoSyntheticErr("synthetic connect failure")

type mongoSyntheticErr string

func (e mongoSyntheticErr) Error() string { return string(e) }

func TestLocalProvision_ConnectFnError(t *testing.T) {
	b := newLocalBackend("mongodb://127.0.0.1:27017", "127.0.0.1:27017")
	b.connectFn = errConnect
	if _, err := b.Provision(context.Background(), "tok", "hobby"); err == nil {
		t.Fatal("Provision: want connect error from seam, got nil")
	}
}

func TestLocalStorageBytes_ConnectFnError(t *testing.T) {
	b := newLocalBackend("mongodb://127.0.0.1:27017", "127.0.0.1:27017")
	b.connectFn = errConnect
	got, err := b.StorageBytes(context.Background(), "tok", "")
	// StorageBytes is fail-open: a connect failure must yield (0, nil).
	if err != nil {
		t.Errorf("StorageBytes connect-fail: want nil err (fail-open), got %v", err)
	}
	if got != 0 {
		t.Errorf("StorageBytes connect-fail: got %d, want 0", got)
	}
}

func TestLocalDeprovision_ConnectFnError(t *testing.T) {
	b := newLocalBackend("mongodb://127.0.0.1:27017", "127.0.0.1:27017")
	b.connectFn = errConnect
	if err := b.Deprovision(context.Background(), "tok", ""); err == nil {
		t.Fatal("Deprovision: want connect error from seam, got nil")
	}
}

// TestLocalConnect_DefaultsToRealDriver asserts the seam falls through to the
// real mongo.Connect when connectFn is nil — the prod path.
func TestLocalConnect_DefaultsToRealDriver(t *testing.T) {
	b := newLocalBackend("mongodb://127.0.0.1:27017", "127.0.0.1:27017")
	c, err := b.connect(context.Background(), options.Client().ApplyURI(b.adminURI))
	if err != nil {
		t.Fatalf("default connect: %v", err)
	}
	_ = c.Disconnect(context.Background())
}

// preDisconnectedConnect returns a connectFn that hands back a client which has
// ALREADY been disconnected, so the method's deferred client.Disconnect(ctx)
// returns "client is disconnected" — exercising the disconnect-error log arms
// that a healthy client never triggers. The pre-disconnected client also makes
// the method body's RunCommand fail, which is fine: we only assert the method
// runs to its deferred-disconnect path.
func preDisconnectedConnect(t *testing.T) func(ctx context.Context, opts ...*options.ClientOptions) (*mongo.Client, error) {
	return func(ctx context.Context, opts ...*options.ClientOptions) (*mongo.Client, error) {
		c, err := mongo.Connect(context.Background(), opts...)
		if err != nil {
			return nil, err
		}
		// Disconnect now so the caller's deferred Disconnect errors.
		_ = c.Disconnect(context.Background())
		return c, nil
	}
}

func TestLocalProvision_DisconnectErrorLogged(t *testing.T) {
	uri, ok := liveMongoURI(t)
	if !ok {
		t.Skip("CUSTOMER_MONGO_URL unreachable; skipping")
	}
	b := newLocalBackend(uri, hostFromURI(uri))
	b.connectFn = preDisconnectedConnect(t)
	// RunCommand on the disconnected client fails → Provision returns an error,
	// but the deferred Disconnect runs first and logs the disconnect error.
	if _, err := b.Provision(context.Background(), uniqueToken("disc-prov"), "hobby"); err == nil {
		t.Fatal("Provision on pre-disconnected client: want error, got nil")
	}
}

func TestLocalStorageBytes_DisconnectErrorLogged(t *testing.T) {
	uri, ok := liveMongoURI(t)
	if !ok {
		t.Skip("CUSTOMER_MONGO_URL unreachable; skipping")
	}
	b := newLocalBackend(uri, hostFromURI(uri))
	b.connectFn = preDisconnectedConnect(t)
	// Fail-open: returns (0, nil) but the deferred Disconnect-error log fires.
	if got, err := b.StorageBytes(context.Background(), uniqueToken("disc-sz"), ""); err != nil || got != 0 {
		t.Errorf("StorageBytes pre-disconnected: got (%d,%v), want (0,nil)", got, err)
	}
}

func TestLocalDeprovision_DisconnectErrorLogged(t *testing.T) {
	uri, ok := liveMongoURI(t)
	if !ok {
		t.Skip("CUSTOMER_MONGO_URL unreachable; skipping")
	}
	b := newLocalBackend(uri, hostFromURI(uri))
	b.connectFn = preDisconnectedConnect(t)
	// dropUser/dropDatabase on the disconnected client error; the canonical-DB
	// Drop error propagates, and the deferred Disconnect-error log fires too.
	if err := b.Deprovision(context.Background(), uniqueToken("disc-drop"), ""); err == nil {
		t.Fatal("Deprovision on pre-disconnected client: want error, got nil")
	}
}

// ─── K8sBackend: Provision happy-path tail via injected initMongoFn ─────────

// readyPodReactor returns a reactor that makes Pods List always report one
// Ready mongodb pod so waitPodReady returns immediately.
func readyPodReactor(ns string) func(ktesting.Action) (bool, runtime.Object, error) {
	return func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.PodList{Items: []corev1.Pod{{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "mongodb-ready",
				Namespace: ns,
				Labels:    map[string]string{"app": "mongodb"},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}}}, nil
	}
}

// TestK8sProvision_HappyPath_PublicHostURL drives Provision end-to-end with a
// no-op initMongoFn + a Ready pod, and publicHost set — covering the
// publicHost connURL branch and the success return.
func TestK8sProvision_HappyPath_PublicHostURL(t *testing.T) {
	const token = "happy-pub"
	ns := mongoK8sNsPrefix + token
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("list", "pods", readyPodReactor(ns))
	b := &K8sBackend{cs: cs, image: "mongo:7", storageClass: "gp3", publicHost: "mongo.instanode.dev"}
	b.initMongoFn = func(ctx context.Context, adminURI, dbName, appUser, appPass string) error {
		return nil // skip the real mongod init
	}
	baseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Carry a teamID so the namespace gets labelled — exercises the
	// ctx.Value(TeamIDKey) propagation branch inside Provision.
	ctx := context.WithValue(baseCtx, ctxkeys.TeamIDKey, "team-aaaa-bbbb")
	creds, err := b.Provision(ctx, token, "hobby")
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if creds == nil {
		t.Fatal("Provision returned nil creds on success")
	}
	if !strings.Contains(creds.URL, "mongo.instanode.dev:27017") {
		t.Errorf("URL missing public host: %q", creds.URL)
	}
	if !strings.Contains(creds.URL, "authSource=") {
		t.Errorf("URL missing authSource: %q", creds.URL)
	}
	if creds.ProviderResourceID != ns {
		t.Errorf("ProviderResourceID = %q, want %q", creds.ProviderResourceID, ns)
	}
	if creds.DatabaseName != mongoDBName(token) {
		t.Errorf("DatabaseName = %q, want %q", creds.DatabaseName, mongoDBName(token))
	}
}

// TestK8sProvision_HappyPath_NodePortURL covers the NodePort fallback connURL
// branch (publicHost empty) plus the route-registry writes (rdb set against an
// unreachable redis is warn-only and must not fail the provision).
func TestK8sProvision_HappyPath_NodePortURL(t *testing.T) {
	const token = "happy-np"
	ns := mongoK8sNsPrefix + token
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("list", "pods", readyPodReactor(ns))
	// Force a NodePort onto the created Service so the NodePort URL is built.
	cs.PrependReactor("create", "services", func(a ktesting.Action) (bool, runtime.Object, error) {
		ca, ok := a.(ktesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		svc := ca.GetObject().(*corev1.Service)
		svc.Spec.ClusterIP = "10.0.0.5"
		if len(svc.Spec.Ports) > 0 {
			svc.Spec.Ports[0].NodePort = 31234
		}
		return false, nil, nil
	})
	// Route registry pointed at a dead redis — Set fails, logged as warn only.
	rdb := goredis.NewClient(&goredis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: 200 * time.Millisecond,
		MaxRetries:  -1,
	})
	defer rdb.Close()
	b := &K8sBackend{cs: cs, image: "mongo:7", storageClass: "gp3", externalHost: "node.example.com"}
	b.EnableRouteRegistry(rdb, "")
	b.initMongoFn = func(ctx context.Context, adminURI, dbName, appUser, appPass string) error { return nil }

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	creds, err := b.Provision(ctx, token, "hobby")
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if !strings.Contains(creds.URL, "node.example.com:31234") {
		t.Errorf("URL missing NodePort host:port: %q", creds.URL)
	}
}

// TestK8sProvision_HappyPath_RouteRegistrySucceeds drives the route-registry
// SUCCESS arm (both Set calls succeed) using a live redis when REDIS_URL_TEST
// is set. Skips otherwise.
func TestK8sProvision_HappyPath_RouteRegistrySucceeds(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL_TEST")
	if redisURL == "" {
		t.Skip("REDIS_URL_TEST unset; skipping route-registry success arm")
	}
	opt, err := goredis.ParseURL(redisURL)
	if err != nil {
		t.Skipf("bad REDIS_URL_TEST: %v", err)
	}
	rdb := goredis.NewClient(opt)
	defer rdb.Close()
	pingCtx, pc := context.WithTimeout(context.Background(), 2*time.Second)
	defer pc()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		t.Skipf("REDIS_URL_TEST unreachable: %v", err)
	}

	const token = "happy-route"
	ns := mongoK8sNsPrefix + token
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("list", "pods", readyPodReactor(ns))
	b := &K8sBackend{cs: cs, image: "mongo:7", storageClass: "gp3", publicHost: "mongo.instanode.dev"}
	b.EnableRouteRegistry(rdb, "")
	b.initMongoFn = func(ctx context.Context, adminURI, dbName, appUser, appPass string) error { return nil }

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := b.Provision(ctx, token, "hobby"); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	// The proxy-consumed user route key must exist.
	val, err := rdb.Get(ctx, b.userPrefix+mongoUserName(token)).Result()
	if err != nil || !strings.Contains(val, ns) {
		t.Errorf("user route key = %q, err=%v; want fqdn containing %q", val, err, ns)
	}
}

// ─── K8sBackend: initMongo / tryInitMongo success arms ──────────────────────

// TestK8sTryInitMongo_SuccessAgainstLiveMongo drives the tryInitMongo SUCCESS
// path: against the no-auth dev Mongo, createUser on the customer DB succeeds,
// so tryInitMongo returns nil — covering the createUser-OK return arm.
func TestK8sTryInitMongo_SuccessAgainstLiveMongo(t *testing.T) {
	uri, ok := liveMongoURI(t)
	if !ok {
		t.Skip("CUSTOMER_MONGO_URL unreachable; skipping")
	}
	b := &K8sBackend{}
	token := uniqueToken("k8s-init")
	dbName := mongoDBName(token)
	user := mongoUserName(token)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.tryInitMongo(ctx, uri, dbName, user, "pw-"+token); err != nil {
		t.Fatalf("tryInitMongo against live mongo: %v", err)
	}
	// initMongo wraps tryInitMongo with retries; the first attempt succeeds so
	// it returns nil immediately (covers the err==nil return arm of initMongo).
	token2 := uniqueToken("k8s-init2")
	if err := b.initMongo(ctx, uri, mongoDBName(token2), mongoUserName(token2), "pw2"); err != nil {
		t.Fatalf("initMongo against live mongo: %v", err)
	}
	// Cleanup the users we created on the admin-less instance.
	cleanup := newLocalBackend(uri, hostFromURI(uri))
	_ = cleanup.Deprovision(context.Background(), token, "")
	_ = cleanup.Deprovision(context.Background(), token2, "")
	// Drop the created users explicitly (they live in the customer DB here).
	if c, err := mongo.Connect(context.Background(), options.Client().ApplyURI(uri)); err == nil {
		_ = c.Database(dbName).RunCommand(context.Background(), bson.D{{Key: "dropUser", Value: user}})
		_ = c.Database(dbName).Drop(context.Background())
		_ = c.Disconnect(context.Background())
	}
}

// TestK8sInitMongo_RetriesThenGivesUp drives initMongo's retry loop to
// exhaustion against a reachable-but-auth-failing target so each attempt
// returns a RETRYABLE error ("AuthenticationFailed"); with a context that
// outlives a couple of retry sleeps but the backend never recovers, the loop
// either gives up or exits on ctx.Done — covering the retry-sleep + give-up
// arms. We point at the AUTHED mongo with bad creds so every createUser fails
// with AuthenticationFailed (retryable), and use a short context.
func TestK8sInitMongo_RetriesThenGivesUp(t *testing.T) {
	authURL := os.Getenv("CUSTOMER_MONGO_AUTH_URL")
	if authURL == "" {
		t.Skip("CUSTOMER_MONGO_AUTH_URL unset; skipping retry-exhaustion arm")
	}
	host := hostPortFromAuthURL(authURL)
	// No credentials → createUser returns AuthenticationFailed (retryable).
	badURI := "mongodb://" + host + "/admin?serverSelectionTimeoutMS=300"
	b := &K8sBackend{}
	// Context that allows a couple of 2s retry sleeps then expires, so the loop
	// exits via ctx.Done() (the retry-sleep arm) rather than burning all 15.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := b.initMongo(ctx, badURI, "db_x", "usr_x", "pw"); err == nil {
		t.Fatal("initMongo: want error after retries against auth-failing mongo, got nil")
	}
}

// ─── K8sBackend: StorageBytes successful dbStats decode ─────────────────────

// TestK8sStorageBytes_DecodesAgainstAuthedMongo plants a Secret + Service whose
// ClusterIP+port point at a real AUTHENTICATED Mongo so the dbStats RunCommand
// actually SUCCEEDS and the storageSize type-switch decode arm runs — the one
// arm the unreachable / missing-secret fail-soft tests never reach.
//
// StorageBytes builds mongodb://root:<pass>@<ClusterIP>:<mongoPort>/admin. We
// set ClusterIP to the auth Mongo's host and the test-only mongoPort seam to
// its port, with the planted Secret carrying the matching root password.
// Provide the URI via CUSTOMER_MONGO_AUTH_URL (e.g.
// mongodb://root:rootpw@127.0.0.1:27018); skips cleanly when absent.
func TestK8sStorageBytes_DecodesAgainstAuthedMongo(t *testing.T) {
	authURL := os.Getenv("CUSTOMER_MONGO_AUTH_URL")
	if authURL == "" {
		t.Skip("CUSTOMER_MONGO_AUTH_URL unset; skipping k8s dbStats decode")
	}
	rest := strings.TrimPrefix(authURL, "mongodb://")
	at := strings.Index(rest, "@")
	if at < 0 {
		t.Skip("CUSTOMER_MONGO_AUTH_URL has no credentials; skipping")
	}
	cp := strings.SplitN(rest[:at], ":", 2)
	if len(cp) != 2 {
		t.Skip("CUSTOMER_MONGO_AUTH_URL credential not user:pass; skipping")
	}
	rootPass := cp[1]
	host := hostPortFromAuthURL(authURL)
	hostOnly := host
	portNum := 27017
	if i := strings.LastIndex(host, ":"); i >= 0 {
		hostOnly = host[:i]
		if n, perr := strconv.Atoi(host[i+1:]); perr == nil {
			portNum = n
		}
	}

	const token = "k8s-decode"
	ns := mongoK8sNsPrefix + token
	dbName := mongoDBName(token)

	// Seed the canonical DB with real data so dbStats reports a concrete
	// numeric storageSize and the type-switch decode arm runs (rather than the
	// non-existent-DB fall-through). Clean up afterwards.
	seedCtx, seedCancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer seedCancel()
	seedClient, serr := mongo.Connect(seedCtx, options.Client().ApplyURI(authURL))
	if serr != nil {
		t.Skipf("seed connect failed: %v", serr)
	}
	defer seedClient.Disconnect(context.Background())
	seedColl := seedClient.Database(dbName).Collection("seed")
	seedDocs := make([]interface{}, 0, 50)
	for i := 0; i < 50; i++ {
		seedDocs = append(seedDocs, bson.D{{Key: "i", Value: i}, {Key: "pad", Value: strings.Repeat("z", 128)}})
	}
	if _, serr := seedColl.InsertMany(seedCtx, seedDocs); serr != nil {
		t.Skipf("seed insert failed: %v", serr)
	}
	defer func() { _ = seedClient.Database(dbName).Drop(context.Background()) }()

	cs := fake.NewSimpleClientset(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "mongo-admin", Namespace: ns},
			Data:       map[string][]byte{"MONGO_INITDB_ROOT_PASSWORD": []byte(rootPass)},
		},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "mongodb", Namespace: ns},
			Spec:       corev1.ServiceSpec{ClusterIP: hostOnly},
		},
	)
	b := &K8sBackend{cs: cs, mongoPort: portNum}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got, err := b.StorageBytes(ctx, token, "")
	if err != nil {
		t.Fatalf("StorageBytes: %v", err)
	}
	if got <= 0 {
		t.Errorf("StorageBytes = %d, want > 0 after seeding data", got)
	}
}
