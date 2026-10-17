package main

// UnifiedPush subscribers (scoresby fork, not upstream).
//
// Besides websocket clients, devices can register a UnifiedPush endpoint per
// library. They prove read access with the library's notification JWT (the
// one seahub's repo-notification-jwt-token hands to desktop clients), and the
// subscription is a lease ending at the JWT's expiry; devices renew it with a
// fresh JWT whenever they sync. Subscriptions are kept in a SQLite file.
//
// On a repo-update event every unexpired subscription of the library gets a
// POST of {"repo_id": "..."} (no names), throttled per subscription: the first
// change is pushed at once, then nothing for the subscription's interval, then
// one trailing push if anything changed meanwhile. A 404 or 410 from the push
// server means the endpoint is gone: all its subscriptions are dropped.

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
)

const (
	defaultPushInterval = 60 * time.Second
	minPushInterval     = 1 * time.Second
	maxPushInterval     = 24 * time.Hour
	pushTimeout         = 10 * time.Second
	pushPurgePeriod     = 1 * time.Hour
	maxPushBody         = 1 << 20
	maxEndpointLen      = 2048
	maxReposPerRequest  = 1000
)

type pushSub struct {
	Endpoint  string
	RepoID    string
	User      string
	Interval  time.Duration
	ExpiresAt time.Time
}

type subKey struct {
	endpoint string
	repoID   string
}

func (s pushSub) key() subKey { return subKey{s.Endpoint, s.RepoID} }

// pushStore keeps the subscriptions in SQLite.
type pushStore struct {
	db *sql.DB
}

func openPushStore(path string) (*pushStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", "file:"+path+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS push_subscription (
		endpoint   TEXT NOT NULL,
		repo_id    TEXT NOT NULL,
		username   TEXT NOT NULL,
		interval_s INTEGER NOT NULL,
		expires_at INTEGER NOT NULL,
		PRIMARY KEY (endpoint, repo_id)
	);
	CREATE INDEX IF NOT EXISTS push_subscription_repo ON push_subscription (repo_id)`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &pushStore{db: db}, nil
}

func (s *pushStore) upsert(sub pushSub) error {
	_, err := s.db.Exec(`INSERT INTO push_subscription (endpoint, repo_id, username, interval_s, expires_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (endpoint, repo_id) DO UPDATE SET
			username = excluded.username, interval_s = excluded.interval_s, expires_at = excluded.expires_at`,
		sub.Endpoint, sub.RepoID, sub.User, int64(sub.Interval/time.Second), sub.ExpiresAt.Unix())
	return err
}

// remove deletes the endpoint's subscriptions for repoIDs, or all of them if
// repoIDs is empty.
func (s *pushStore) remove(endpoint string, repoIDs []string) (int64, error) {
	if len(repoIDs) == 0 {
		res, err := s.db.Exec(`DELETE FROM push_subscription WHERE endpoint = ?`, endpoint)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	}
	var n int64
	for _, repoID := range repoIDs {
		res, err := s.db.Exec(`DELETE FROM push_subscription WHERE endpoint = ? AND repo_id = ?`, endpoint, repoID)
		if err != nil {
			return n, err
		}
		k, _ := res.RowsAffected()
		n += k
	}
	return n, nil
}

func scanSubs(rows *sql.Rows) ([]pushSub, error) {
	defer rows.Close()
	var subs []pushSub
	for rows.Next() {
		var sub pushSub
		var interval, expires int64
		if err := rows.Scan(&sub.Endpoint, &sub.RepoID, &sub.User, &interval, &expires); err != nil {
			return nil, err
		}
		sub.Interval = time.Duration(interval) * time.Second
		sub.ExpiresAt = time.Unix(expires, 0)
		subs = append(subs, sub)
	}
	return subs, rows.Err()
}

// forRepo returns the library's subscriptions whose lease hasn't ended.
func (s *pushStore) forRepo(repoID string, now time.Time) ([]pushSub, error) {
	rows, err := s.db.Query(`SELECT endpoint, repo_id, username, interval_s, expires_at
		FROM push_subscription WHERE repo_id = ? AND expires_at > ?`, repoID, now.Unix())
	if err != nil {
		return nil, err
	}
	return scanSubs(rows)
}

// get returns the subscription if it exists and its lease hasn't ended.
func (s *pushStore) get(key subKey, now time.Time) (pushSub, bool, error) {
	rows, err := s.db.Query(`SELECT endpoint, repo_id, username, interval_s, expires_at
		FROM push_subscription WHERE endpoint = ? AND repo_id = ? AND expires_at > ?`,
		key.endpoint, key.repoID, now.Unix())
	if err != nil {
		return pushSub{}, false, err
	}
	subs, err := scanSubs(rows)
	if err != nil || len(subs) == 0 {
		return pushSub{}, false, err
	}
	return subs[0], true, nil
}

func (s *pushStore) purgeExpired(now time.Time) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM push_subscription WHERE expires_at <= ?`, now.Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// throttle is the per-subscription state: push the first change at once,
// then stay quiet for the interval and push once more at its end if anything
// changed during it. The caller sends when told to and arms a timer calling
// wake at the returned time.
type throttle struct {
	quietUntil time.Time
	pending    bool
	armed      bool
}

// event records a change. sendNow: push it now. wakeAt (if not zero): arm a
// timer for then.
func (t *throttle) event(now time.Time, interval time.Duration) (sendNow bool, wakeAt time.Time) {
	if !t.armed && !now.Before(t.quietUntil) {
		t.quietUntil = now.Add(interval)
		return true, time.Time{}
	}
	t.pending = true
	if t.armed {
		return false, time.Time{}
	}
	t.armed = true
	return false, t.quietUntil
}

// wake is the timer firing at the end of a quiet period. send: push the
// trailing change, which starts a new quiet period.
func (t *throttle) wake(now time.Time, interval time.Duration) (send bool) {
	t.armed = false
	if !t.pending {
		return false
	}
	t.pending = false
	t.quietUntil = now.Add(interval)
	return true
}

// idle: the state holds nothing worth keeping.
func (t *throttle) idle(now time.Time) bool {
	return !t.armed && !now.Before(t.quietUntil)
}

type pusher struct {
	store  *pushStore
	client *http.Client

	mu        sync.Mutex
	throttles map[subKey]*throttle
}

var pushes *pusher

func newPusher(store *pushStore) *pusher {
	return &pusher{
		store:     store,
		client:    &http.Client{Timeout: pushTimeout},
		throttles: make(map[subKey]*throttle),
	}
}

func initPush(configDir string) {
	path := os.Getenv("NOTIFICATION_SERVER_PUSH_DB")
	if path == "" {
		path = filepath.Join(configDir, "push-subscriptions.db")
	}
	store, err := openPushStore(path)
	if err != nil {
		log.Fatalf("Failed to open push subscription store %s: %v", path, err)
	}
	pushes = newPusher(store)
	go pushes.purgeLoop()
}

func (p *pusher) purgeLoop() {
	ticker := time.NewTicker(pushPurgePeriod)
	for range ticker.C {
		p.purge(time.Now())
	}
}

func (p *pusher) purge(now time.Time) {
	if n, err := p.store.purgeExpired(now); err != nil {
		log.Warnf("Failed to purge expired push subscriptions: %v", err)
	} else if n > 0 {
		log.Infof("Purged %d expired push subscriptions", n)
	}
	p.mu.Lock()
	for key, t := range p.throttles {
		if t.idle(now) {
			delete(p.throttles, key)
		}
	}
	p.mu.Unlock()
}

// notify handles a change of the library.
func (p *pusher) notify(repoID string) {
	now := time.Now()
	subs, err := p.store.forRepo(repoID, now)
	if err != nil {
		log.Warnf("Failed to load push subscriptions of %s: %v", repoID, err)
		return
	}
	for _, sub := range subs {
		key := sub.key()
		p.mu.Lock()
		t := p.throttles[key]
		if t == nil {
			t = new(throttle)
			p.throttles[key] = t
		}
		sendNow, wakeAt := t.event(now, sub.Interval)
		p.mu.Unlock()
		if sendNow {
			go p.deliver(sub)
		}
		if !wakeAt.IsZero() {
			time.AfterFunc(wakeAt.Sub(now), func() { p.wake(key, t) })
		}
	}
}

// wake ends the quiet period of throttle t, unless the subscription was
// forgotten (and maybe re-registered with a fresh state) since t's timer
// was armed.
func (p *pusher) wake(key subKey, t *throttle) {
	now := time.Now()
	sub, ok, err := p.store.get(key, now)
	if err != nil {
		log.Warnf("Failed to load push subscription: %v", err)
	}
	p.mu.Lock()
	if p.throttles[key] != t {
		p.mu.Unlock()
		return
	}
	if !ok {
		// Unregistered, expired or dropped meanwhile.
		delete(p.throttles, key)
		p.mu.Unlock()
		return
	}
	send := t.wake(now, sub.Interval)
	if !send {
		delete(p.throttles, key)
	}
	p.mu.Unlock()
	if send {
		go p.deliver(sub)
	}
}

type pushPayload struct {
	RepoID string `json:"repo_id"`
}

func (p *pusher) deliver(sub pushSub) {
	body, _ := json.Marshal(pushPayload{RepoID: sub.RepoID})
	req, err := http.NewRequest(http.MethodPost, sub.Endpoint, bytes.NewReader(body))
	if err != nil {
		log.Warnf("Bad push endpoint for %s: %v", sub.RepoID, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("TTL", "86400")
	rsp, err := p.client.Do(req)
	if err != nil {
		log.Infof("Failed to push %s to %s: %v", sub.RepoID, endpointHost(sub.Endpoint), err)
		return
	}
	io.Copy(io.Discard, io.LimitReader(rsp.Body, 64<<10))
	rsp.Body.Close()
	switch {
	case rsp.StatusCode == http.StatusNotFound || rsp.StatusCode == http.StatusGone:
		if _, err := p.unregister(sub.Endpoint, nil); err != nil {
			log.Warnf("Failed to drop push subscriptions: %v", err)
		}
		log.Infof("Push endpoint at %s answered %d: dropped its subscriptions", endpointHost(sub.Endpoint), rsp.StatusCode)
	case rsp.StatusCode < 200 || rsp.StatusCode > 299:
		log.Infof("Push of %s to %s answered %d", sub.RepoID, endpointHost(sub.Endpoint), rsp.StatusCode)
	default:
		log.Debugf("Pushed %s to %s", sub.RepoID, endpointHost(sub.Endpoint))
	}
}

// unregister forgets the endpoint's subscriptions for repoIDs, or all of
// them if repoIDs is empty.
func (p *pusher) unregister(endpoint string, repoIDs []string) (int64, error) {
	n, err := p.store.remove(endpoint, repoIDs)
	if err != nil {
		return n, err
	}
	ids := make(map[string]struct{}, len(repoIDs))
	for _, id := range repoIDs {
		ids[id] = struct{}{}
	}
	p.mu.Lock()
	for key := range p.throttles {
		if key.endpoint != endpoint {
			continue
		}
		if _, ok := ids[key.repoID]; ok || len(ids) == 0 {
			delete(p.throttles, key)
		}
	}
	p.mu.Unlock()
	return n, nil
}

// endpointHost is what gets logged: endpoint paths are capabilities.
func endpointHost(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "?"
	}
	return u.Host
}

func validEndpoint(endpoint string) bool {
	if endpoint == "" || len(endpoint) > maxEndpointLen {
		return false
	}
	u, err := url.Parse(endpoint)
	return err == nil && (u.Scheme == "https" || u.Scheme == "http") && u.Host != ""
}

func clampInterval(secs int64) time.Duration {
	if secs <= 0 {
		return defaultPushInterval
	}
	d := time.Duration(secs) * time.Second
	if secs > int64(maxPushInterval/time.Second) {
		return maxPushInterval
	}
	if d < minPushInterval {
		return minPushInterval
	}
	return d
}

// checkRepoToken validates a library's notification JWT and returns its
// username and expiry.
func checkRepoToken(tokenString, repoID string, now time.Time) (string, time.Time, error) {
	claims := new(myClaims)
	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		return []byte(privateKey), nil
	}, jwt.WithValidMethods([]string{"HS256"}))
	if err != nil || !token.Valid {
		return "", time.Time{}, fmt.Errorf("invalid token")
	}
	if claims.RepoID != repoID {
		return "", time.Time{}, fmt.Errorf("token is for another library")
	}
	if claims.UserName == "" {
		return "", time.Time{}, fmt.Errorf("token has no user")
	}
	if claims.Exp <= now.Unix() {
		return "", time.Time{}, fmt.Errorf("token expired")
	}
	return claims.UserName, time.Unix(claims.Exp, 0), nil
}

type pushRegisterRepo struct {
	RepoID   string `json:"repo_id"`
	Token    string `json:"token"`
	Interval int64  `json:"interval"`
}

type pushRegisterRequest struct {
	Endpoint string             `json:"endpoint"`
	Repos    []pushRegisterRepo `json:"repos"`
}

type pushLease struct {
	RepoID    string `json:"repo_id"`
	ExpiresAt int64  `json:"expires_at"`
}

type pushRejected struct {
	RepoID string `json:"repo_id"`
	Error  string `json:"error"`
}

type pushRegisterResponse struct {
	Repos    []pushLease    `json:"repos"`
	Rejected []pushRejected `json:"rejected"`
}

type pushUnregisterRequest struct {
	Endpoint string   `json:"endpoint"`
	RepoIDs  []string `json:"repo_ids"`
}

func readPushRequest(r *http.Request, v interface{}) *appError {
	if r.Method != http.MethodPost {
		return &appError{Message: "POST only", Code: http.StatusMethodNotAllowed}
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, maxPushBody)).Decode(v); err != nil {
		return &appError{Message: "invalid JSON body", Code: http.StatusBadRequest}
	}
	return nil
}

func writeJSON(rsp http.ResponseWriter, v interface{}) *appError {
	rsp.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(rsp).Encode(v); err != nil {
		return &appError{Error: err, Code: http.StatusInternalServerError}
	}
	return nil
}

// pushRegisterCB: POST /push/register
// {"endpoint": "...", "repos": [{"repo_id", "token", "interval"}]}
func pushRegisterCB(rsp http.ResponseWriter, r *http.Request) *appError {
	var req pushRegisterRequest
	if e := readPushRequest(r, &req); e != nil {
		return e
	}
	if !validEndpoint(req.Endpoint) {
		return &appError{Message: "endpoint must be an http(s) URL", Code: http.StatusBadRequest}
	}
	if len(req.Repos) > maxReposPerRequest {
		return &appError{Message: "too many repos", Code: http.StatusBadRequest}
	}
	now := time.Now()
	out := pushRegisterResponse{Repos: []pushLease{}, Rejected: []pushRejected{}}
	for _, repo := range req.Repos {
		user, exp, err := checkRepoToken(repo.Token, repo.RepoID, now)
		if err != nil {
			out.Rejected = append(out.Rejected, pushRejected{RepoID: repo.RepoID, Error: err.Error()})
			continue
		}
		sub := pushSub{
			Endpoint:  req.Endpoint,
			RepoID:    repo.RepoID,
			User:      user,
			Interval:  clampInterval(repo.Interval),
			ExpiresAt: exp,
		}
		if err := pushes.store.upsert(sub); err != nil {
			return &appError{Error: err, Code: http.StatusInternalServerError}
		}
		out.Repos = append(out.Repos, pushLease{RepoID: repo.RepoID, ExpiresAt: exp.Unix()})
	}
	return writeJSON(rsp, out)
}

// pushUnregisterCB: POST /push/unregister {"endpoint": "...", "repo_ids": [...]}
// Knowing the endpoint is the authorisation: it's a capability already.
func pushUnregisterCB(rsp http.ResponseWriter, r *http.Request) *appError {
	var req pushUnregisterRequest
	if e := readPushRequest(r, &req); e != nil {
		return e
	}
	if !validEndpoint(req.Endpoint) {
		return &appError{Message: "endpoint must be an http(s) URL", Code: http.StatusBadRequest}
	}
	n, err := pushes.unregister(req.Endpoint, req.RepoIDs)
	if err != nil {
		return &appError{Error: err, Code: http.StatusInternalServerError}
	}
	return writeJSON(rsp, map[string]int64{"removed": n})
}
