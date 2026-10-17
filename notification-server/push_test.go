package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
)

const testRepo = "63e0dbf7-1bdd-4278-8b6c-950c04422dd8"

func TestThrottleStateMachine(t *testing.T) {
	interval := time.Minute
	t0 := time.Unix(1_000_000, 0)
	var th throttle

	send, wakeAt := th.event(t0, interval)
	if !send || !wakeAt.IsZero() {
		t.Fatalf("first change: send=%v wakeAt=%v, want an immediate push", send, wakeAt)
	}
	send, wakeAt = th.event(t0.Add(time.Second), interval)
	if send || !wakeAt.Equal(t0.Add(interval)) {
		t.Fatalf("change in quiet period: send=%v wakeAt=%v, want a timer at its end", send, wakeAt)
	}
	send, wakeAt = th.event(t0.Add(2*time.Second), interval)
	if send || !wakeAt.IsZero() {
		t.Fatalf("second change in quiet period: send=%v wakeAt=%v, want nothing (timer armed)", send, wakeAt)
	}
	if !th.wake(t0.Add(interval), interval) {
		t.Fatal("end of quiet period with changes: want one trailing push")
	}
	// The trailing push starts a new quiet period.
	send, wakeAt = th.event(t0.Add(interval+time.Second), interval)
	if send || !wakeAt.Equal(t0.Add(2*interval)) {
		t.Fatalf("change after trailing push: send=%v wakeAt=%v", send, wakeAt)
	}
	if !th.wake(t0.Add(2*interval), interval) {
		t.Fatal("want a trailing push for the change after the first trailing push")
	}
	if th.idle(t0.Add(2 * interval)) {
		t.Fatal("state is not idle during the quiet period after a trailing push")
	}
	if !th.idle(t0.Add(3 * interval)) {
		t.Fatal("state is idle once the quiet period is over")
	}
	// Quiet period over without changes: next change goes out at once.
	send, _ = th.event(t0.Add(3*interval), interval)
	if !send {
		t.Fatal("change after an idle quiet period: want an immediate push")
	}
	// A timer firing with nothing pending sends nothing.
	var th2 throttle
	th2.event(t0, interval)
	if th2.wake(t0.Add(interval), interval) {
		t.Fatal("wake without changes: want no push")
	}
}

// The stage-3 phone test (60 s interval): changes at 09:59:12, 09:59:26,
// 10:00:14 and 10:01:23 were pushed at 09:59:13, 10:00:13, 10:01:13 and
// 10:02:13. The last change came 69 s after the one before, yet waited 50 s
// because each trailing push started a new hold.
func TestThrottleChangeAfterQuietIntervalIsImmediate(t *testing.T) {
	interval := time.Minute
	t0 := time.Date(2026, 10, 17, 9, 59, 12, 0, time.UTC)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	var th throttle

	if send, _ := th.event(at(0), interval); !send {
		t.Fatal("first change: want an immediate push")
	}
	if send, wakeAt := th.event(at(14), interval); send || !wakeAt.Equal(at(60)) {
		t.Fatalf("change 14 s later: send=%v wakeAt=%v, want held until %v", send, wakeAt, at(60))
	}
	if !th.wake(at(60), interval) {
		t.Fatal("want a trailing push")
	}
	// 48 s after the previous change: still held, to an interval after the
	// trailing push.
	if send, wakeAt := th.event(at(62), interval); send || !wakeAt.Equal(at(120)) {
		t.Fatalf("change 48 s after the previous one: send=%v wakeAt=%v, want held until %v", send, wakeAt, at(120))
	}
	if !th.wake(at(120), interval) {
		t.Fatal("want a trailing push")
	}
	// 69 s after the previous change, 11 s after the trailing push: the
	// library was quiet for an interval, so it goes out at once.
	if send, wakeAt := th.event(at(131), interval); !send || !wakeAt.IsZero() {
		t.Fatalf("change after a quiet interval: send=%v wakeAt=%v, want an immediate push", send, wakeAt)
	}
	// And a burst right after it is held for one trailing push.
	if send, wakeAt := th.event(at(132), interval); send || !wakeAt.Equal(at(191)) {
		t.Fatalf("change 1 s later: send=%v wakeAt=%v, want held until %v", send, wakeAt, at(191))
	}
}

type receiver struct {
	mu     sync.Mutex
	bodies []string
	times  []time.Time
	status int
}

func (rc *receiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	rc.mu.Lock()
	rc.bodies = append(rc.bodies, string(b))
	rc.times = append(rc.times, time.Now())
	status := rc.status
	rc.mu.Unlock()
	if status != 0 {
		w.WriteHeader(status)
	}
}

func (rc *receiver) count() int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return len(rc.bodies)
}

func newTestPusher(t *testing.T) *pusher {
	t.Helper()
	store, err := openPushStore(filepath.Join(t.TempDir(), "push.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.db.Close() })
	p := newPusher(store)
	pushes = p
	privateKey = "test-key-0123456789-0123456789"
	return p
}

func waitFor(t *testing.T, what string, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPusherThrottlesDeliveries(t *testing.T) {
	p := newTestPusher(t)
	rc := &receiver{}
	srv := httptest.NewServer(rc)
	defer srv.Close()

	interval := time.Second
	sub := pushSub{Endpoint: srv.URL + "/up", RepoID: testRepo, User: "a@x", Interval: interval, ExpiresAt: time.Now().Add(time.Hour)}
	if err := p.store.upsert(sub); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	p.notify(testRepo)
	waitFor(t, "immediate push", func() bool { return rc.count() == 1 }, 2*time.Second)
	if body := rc.bodies[0]; body != `{"repo_id":"`+testRepo+`"}` {
		t.Fatalf("payload %s", body)
	}

	// A burst during the quiet period: one trailing push at its end.
	for range 5 {
		p.notify(testRepo)
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	if n := rc.count(); n != 1 {
		t.Fatalf("%d pushes inside the quiet period, want 1", n)
	}
	waitFor(t, "trailing push", func() bool { return rc.count() == 2 }, 2*time.Second)
	if gap := rc.times[1].Sub(start); gap < time.Second {
		t.Fatalf("trailing push %v after the first change, want >= the 1 s interval", gap)
	}
	time.Sleep(1500 * time.Millisecond)
	if n := rc.count(); n != 2 {
		t.Fatalf("%d pushes, want exactly one trailing push", n)
	}

	// Quiet period over: the next change is pushed at once.
	p.notify(testRepo)
	waitFor(t, "push after the quiet period", func() bool { return rc.count() == 3 }, time.Second)

	// Other libraries don't push.
	p.notify("00000000-0000-0000-0000-000000000000")
	time.Sleep(200 * time.Millisecond)
	if n := rc.count(); n != 3 {
		t.Fatalf("%d pushes, change of another library pushed", n)
	}
}

func TestPusherChangeAfterQuietIntervalIsImmediate(t *testing.T) {
	p := newTestPusher(t)
	rc := &receiver{}
	srv := httptest.NewServer(rc)
	defer srv.Close()

	interval := time.Second
	sub := pushSub{Endpoint: srv.URL + "/up", RepoID: testRepo, User: "a@x", Interval: interval, ExpiresAt: time.Now().Add(time.Hour)}
	if err := p.store.upsert(sub); err != nil {
		t.Fatal(err)
	}

	p.notify(testRepo)
	waitFor(t, "immediate push", func() bool { return rc.count() == 1 }, time.Second)
	time.Sleep(100 * time.Millisecond)
	p.notify(testRepo)
	waitFor(t, "trailing push", func() bool { return rc.count() == 2 }, 2*time.Second)
	// More than an interval after the last change, but well inside the
	// interval after the trailing push.
	time.Sleep(300 * time.Millisecond)
	changed := time.Now()
	p.notify(testRepo)
	waitFor(t, "push of a change after a quiet interval", func() bool { return rc.count() == 3 }, 2*time.Second)
	if lag := rc.times[2].Sub(changed); lag > 200*time.Millisecond {
		t.Fatalf("change after a quiet interval pushed %v later, want at once", lag)
	}
}

func TestLeaseExpiry(t *testing.T) {
	p := newTestPusher(t)
	rc := &receiver{}
	srv := httptest.NewServer(rc)
	defer srv.Close()

	now := time.Now()
	live := pushSub{Endpoint: srv.URL + "/live", RepoID: testRepo, User: "a@x", Interval: time.Minute, ExpiresAt: now.Add(time.Hour)}
	expired := pushSub{Endpoint: srv.URL + "/expired", RepoID: testRepo, User: "a@x", Interval: time.Minute, ExpiresAt: now.Add(-time.Second)}
	for _, s := range []pushSub{live, expired} {
		if err := p.store.upsert(s); err != nil {
			t.Fatal(err)
		}
	}

	p.notify(testRepo)
	waitFor(t, "push", func() bool { return rc.count() == 1 }, 2*time.Second)
	time.Sleep(200 * time.Millisecond)
	if n := rc.count(); n != 1 {
		t.Fatalf("%d pushes, want only the unexpired lease", n)
	}

	// The live lease ends later; purging after that leaves nothing.
	p.purge(now)
	var rows int
	p.store.db.QueryRow(`SELECT count(*) FROM push_subscription`).Scan(&rows)
	if rows != 1 {
		t.Fatalf("%d subscriptions after purge, want 1", rows)
	}
	p.purge(now.Add(2 * time.Hour))
	p.store.db.QueryRow(`SELECT count(*) FROM push_subscription`).Scan(&rows)
	if rows != 0 {
		t.Fatalf("%d subscriptions after the lease ended, want 0", rows)
	}
	if len(p.throttles) != 0 {
		t.Fatalf("%d throttle states left after purge", len(p.throttles))
	}

	// Subscriptions survive a restart (reopening the file).
	path := filepath.Join(t.TempDir(), "push.db")
	s1, err := openPushStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.upsert(live); err != nil {
		t.Fatal(err)
	}
	s1.db.Close()
	s2, err := openPushStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.db.Close()
	if subs, err := s2.forRepo(testRepo, now); err != nil || len(subs) != 1 || subs[0].User != "a@x" {
		t.Fatalf("after reopening: %v %v", subs, err)
	}
}

func TestGoneEndpointIsDropped(t *testing.T) {
	p := newTestPusher(t)
	rc := &receiver{status: http.StatusGone}
	srv := httptest.NewServer(rc)
	defer srv.Close()

	sub := pushSub{Endpoint: srv.URL + "/gone", RepoID: testRepo, User: "a@x", Interval: time.Minute, ExpiresAt: time.Now().Add(time.Hour)}
	p.store.upsert(sub)
	p.notify(testRepo)
	waitFor(t, "subscription dropped", func() bool {
		subs, _ := p.store.forRepo(testRepo, time.Now())
		return len(subs) == 0
	}, 2*time.Second)
}

func signToken(t *testing.T, repoID, user string, exp time.Time) string {
	t.Helper()
	s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"repo_id": repoID, "username": user, "exp": exp.Unix(),
	}).SignedString([]byte(privateKey))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func postJSON(t *testing.T, h http.Handler, path string, body interface{}) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b)))
	return rec
}

func TestRegisterAndUnregister(t *testing.T) {
	p := newTestPusher(t)
	router := newHTTPRouter()
	endpoint := "https://ntfy.example/upabc?up=1"
	exp := time.Now().Add(72 * time.Hour).Truncate(time.Second)
	other := "11111111-2222-3333-4444-555555555555"

	rec := postJSON(t, router, "/push/register", map[string]interface{}{
		"endpoint": endpoint,
		"repos": []map[string]interface{}{
			{"repo_id": testRepo, "token": signToken(t, testRepo, "a@x", exp), "interval": 30},
			{"repo_id": other, "token": signToken(t, testRepo, "a@x", exp)},                       // token for another library
			{"repo_id": other, "token": signToken(t, other, "a@x", time.Now().Add(-time.Minute))}, // expired
			{"repo_id": other, "token": "garbage"},
		},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("register: %d %s", rec.Code, rec.Body)
	}
	var out pushRegisterResponse
	json.Unmarshal(rec.Body.Bytes(), &out)
	if len(out.Repos) != 1 || out.Repos[0].RepoID != testRepo || out.Repos[0].ExpiresAt != exp.Unix() {
		t.Fatalf("accepted %+v, want %s until %d", out.Repos, testRepo, exp.Unix())
	}
	if len(out.Rejected) != 3 {
		t.Fatalf("rejected %+v, want 3", out.Rejected)
	}
	subs, _ := p.store.forRepo(testRepo, time.Now())
	if len(subs) != 1 || subs[0].Interval != 30*time.Second || subs[0].User != "a@x" || !subs[0].ExpiresAt.Equal(exp) {
		t.Fatalf("stored %+v", subs)
	}

	// Renewal with a later token moves the lease.
	exp2 := exp.Add(time.Hour)
	postJSON(t, router, "/push/register", map[string]interface{}{
		"endpoint": endpoint,
		"repos":    []map[string]interface{}{{"repo_id": testRepo, "token": signToken(t, testRepo, "a@x", exp2)}},
	})
	subs, _ = p.store.forRepo(testRepo, time.Now())
	if len(subs) != 1 || !subs[0].ExpiresAt.Equal(exp2) || subs[0].Interval != defaultPushInterval {
		t.Fatalf("after renewal %+v", subs)
	}

	// Bad requests.
	if rec := postJSON(t, router, "/push/register", map[string]interface{}{"endpoint": "file:///etc/passwd"}); rec.Code != http.StatusBadRequest {
		t.Fatalf("non-http endpoint: %d", rec.Code)
	}
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, httptest.NewRequest(http.MethodGet, "/push/register", nil))
	if getRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET: %d", getRec.Code)
	}

	rec = postJSON(t, router, "/push/unregister", map[string]interface{}{"endpoint": endpoint, "repo_ids": []string{testRepo}})
	if rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte(`"removed":1`)) {
		t.Fatalf("unregister: %d %s", rec.Code, rec.Body)
	}
	if subs, _ := p.store.forRepo(testRepo, time.Now()); len(subs) != 0 {
		t.Fatalf("after unregister %+v", subs)
	}
}

func TestClampInterval(t *testing.T) {
	for in, want := range map[int64]time.Duration{
		0: defaultPushInterval, -5: defaultPushInterval, 1: time.Second, 60: time.Minute,
		1 << 40: maxPushInterval, 86400: 24 * time.Hour,
	} {
		if got := clampInterval(in); got != want {
			t.Errorf("clampInterval(%d) = %v, want %v", in, got, want)
		}
	}
}
