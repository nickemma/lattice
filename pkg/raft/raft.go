// Package raft contains a small message-driven Raft implementation. It is
// intentionally compact enough to read in one sitting, but it includes the
// mechanics that matter for LATTICE: terms and votes, log matching, quorum
// commits, persistence, snapshots, and replay after restart.
package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

var (
	ErrNoLeader = errors.New("raft: no leader")
	ErrNoQuorum = errors.New("raft: quorum unavailable")
	ErrStopped  = errors.New("raft: node stopped")
	ErrUnknown  = errors.New("raft: unknown node")
	ErrBadSnap  = errors.New("raft: snapshot index is not applied")
)

type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

type Command struct {
	Key    string
	Value  string
	Delete bool
}

type Entry struct {
	Term    uint64
	Index   uint64
	Command Command
}

// PersistentState is the part of a node that must survive a process crash.
// State (leader/follower) and election timers are intentionally volatile.
type PersistentState struct {
	CurrentTerm   uint64            `json:"current_term"`
	VotedFor      int               `json:"voted_for"`
	Log           []Entry           `json:"log"`
	CommitIndex   uint64            `json:"commit_index"`
	SnapshotIndex uint64            `json:"snapshot_index"`
	SnapshotTerm  uint64            `json:"snapshot_term"`
	SnapshotKV    map[string]string `json:"snapshot_kv,omitempty"`
}

// Storage is the durable boundary for a Raft node. Implementations may be a
// memory store for tests or an atomic file store for crash/restart exercises.
type Storage interface {
	Load() (PersistentState, error)
	Save(PersistentState) error
}

type MemoryStorage struct {
	mu    sync.Mutex
	state PersistentState
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{state: PersistentState{VotedFor: -1}} }

func (s *MemoryStorage) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return clonePersistent(s.state), nil
}

func (s *MemoryStorage) Save(state PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = clonePersistent(state)
	return nil
}

type FileStorage struct{ path string }

func NewFileStorage(path string) *FileStorage { return &FileStorage{path: path} }

func (s *FileStorage) Load() (PersistentState, error) {
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return PersistentState{VotedFor: -1}, nil
	}
	if err != nil {
		return PersistentState{}, err
	}
	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return PersistentState{}, fmt.Errorf("raft storage %s: %w", s.path, err)
	}
	return state, nil
}

func (s *FileStorage) Save(state PersistentState) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(s.path), ".raft-state-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(name, s.path)
}

type Node struct {
	ID            int
	State         State
	Term          uint64
	VotedFor      int
	Log           []Entry
	CommitIndex   uint64
	Applied       uint64
	KV            map[string]string
	Alive         bool
	SnapshotIndex uint64
	SnapshotTerm  uint64
	SnapshotKV    map[string]string

	storage        Storage
	persistErr     error
	electionTicks  int
	electionLimit  int
	heartbeatTicks int
	nextIndex      map[int]uint64
	matchIndex     map[int]uint64
	votes          map[int]bool
}

type messageType uint8

const (
	requestVote messageType = iota
	voteResponse
	appendEntries
	appendResponse
	installSnapshot
	snapshotResponse
)

type message struct {
	kind         messageType
	from, to     int
	term         uint64
	lastLogIndex uint64
	lastLogTerm  uint64
	voteGranted  bool
	prevLogIndex uint64
	prevLogTerm  uint64
	entries      []Entry
	leaderCommit uint64
	success      bool
	matchIndex   uint64
	snapshot     PersistentState
}

type Cluster struct {
	mu          sync.Mutex
	nodes       map[int]*Node
	leader      int
	partitioned map[[2]int]bool
	queue       []message
	lastError   error
}

func NewCluster(ids ...int) *Cluster {
	c := &Cluster{nodes: make(map[int]*Node), leader: -1, partitioned: make(map[[2]int]bool)}
	for _, id := range ids {
		node, err := newNode(id, NewMemoryStorage())
		if err != nil {
			c.lastError = err
			continue
		}
		c.nodes[id] = node
	}
	return c
}

// NewPersistentCluster creates a cluster whose node state files are stored in
// dir/node-<id>.json. Starting a new Cluster from the same directory models a
// process restart; no in-memory state is reused.
func NewPersistentCluster(dir string, ids ...int) (*Cluster, error) {
	c := &Cluster{nodes: make(map[int]*Node), leader: -1, partitioned: make(map[[2]int]bool)}
	for _, id := range ids {
		node, err := newNode(id, NewFileStorage(filepath.Join(dir, fmt.Sprintf("node-%d.json", id))))
		if err != nil {
			return nil, err
		}
		c.nodes[id] = node
	}
	return c, nil
}

func newNode(id int, storage Storage) (*Node, error) {
	state, err := storage.Load()
	if err != nil {
		return nil, err
	}
	if state.VotedFor == 0 && state.CurrentTerm == 0 {
		state.VotedFor = -1
	}
	n := &Node{
		ID: id, State: Follower, Term: state.CurrentTerm, VotedFor: state.VotedFor,
		Log: append([]Entry(nil), state.Log...), CommitIndex: state.CommitIndex,
		KV: cloneMap(state.SnapshotKV), Alive: true, SnapshotIndex: state.SnapshotIndex,
		SnapshotTerm: state.SnapshotTerm, SnapshotKV: cloneMap(state.SnapshotKV),
		storage: storage, electionLimit: 3 + id%3, nextIndex: make(map[int]uint64),
		matchIndex: make(map[int]uint64), votes: make(map[int]bool),
	}
	if n.KV == nil {
		n.KV = make(map[string]string)
	}
	if n.SnapshotKV == nil {
		n.SnapshotKV = make(map[string]string)
	}
	n.Applied = n.SnapshotIndex
	if n.CommitIndex < n.Applied {
		n.CommitIndex = n.Applied
	}
	if n.CommitIndex > n.lastIndex() {
		n.CommitIndex = n.lastIndex()
	}
	n.apply()
	return n, nil
}

func (c *Cluster) ElectLeader(preferred int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if preferred >= 0 {
		n, ok := c.nodes[preferred]
		if !ok {
			return ErrUnknown
		}
		if !n.Alive {
			return ErrStopped
		}
		c.startElectionLocked(n)
	} else {
		ids := c.liveIDsLocked()
		if len(ids) == 0 {
			return ErrNoQuorum
		}
		c.startElectionLocked(c.nodes[ids[0]])
	}
	c.drainLocked()
	if c.leader < 0 {
		return ErrNoQuorum
	}
	return nil
}

// Tick advances election and heartbeat timers by one logical tick and then
// drains the message queue. No wall clock is needed, making failure tests
// repeatable and allowing callers to inject delays or reorder delivery later.
func (c *Cluster) Tick() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.nodes {
		if !n.Alive {
			continue
		}
		if n.State == Leader {
			n.heartbeatTicks++
			if n.heartbeatTicks >= 1 {
				n.heartbeatTicks = 0
				c.broadcastAppendLocked(n)
			}
			continue
		}
		n.electionTicks++
		if n.electionTicks >= n.electionLimit {
			c.startElectionLocked(n)
		}
	}
	c.drainLocked()
}

func (c *Cluster) Propose(command Command) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader < 0 {
		return 0, ErrNoLeader
	}
	leader := c.nodes[c.leader]
	if leader == nil || !leader.Alive || leader.State != Leader {
		return 0, ErrNoLeader
	}
	if leader.persistErr != nil {
		return 0, leader.persistErr
	}
	entry := Entry{Term: leader.Term, Index: leader.lastIndex() + 1, Command: command}
	leader.Log = append(leader.Log, entry)
	if err := c.persistLocked(leader); err != nil {
		return 0, err
	}
	c.broadcastAppendLocked(leader)
	c.drainLocked()
	if leader.CommitIndex < entry.Index {
		return 0, ErrNoQuorum
	}
	return entry.Index, nil
}

func (c *Cluster) Kill(id int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[id]
	if !ok {
		return ErrUnknown
	}
	n.Alive = false
	n.State = Follower
	n.votes = make(map[int]bool)
	if c.leader == id {
		c.leader = -1
	}
	return nil
}

func (c *Cluster) Start(id int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[id]
	if !ok {
		return ErrUnknown
	}
	if n.persistErr != nil {
		return n.persistErr
	}
	n.Alive = true
	n.State = Follower
	n.electionTicks = 0
	return nil
}

func (c *Cluster) Partition(a, b int, blocked bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.partitioned[[2]int{a, b}] = blocked
	c.partitioned[[2]int{b, a}] = blocked
}

func (c *Cluster) Leader() (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader < 0 {
		return 0, false
	}
	n := c.nodes[c.leader]
	return c.leader, n != nil && n.Alive && n.State == Leader
}

func (c *Cluster) Read(id int, key string) (string, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[id]
	if !ok {
		return "", false, ErrUnknown
	}
	if !n.Alive {
		return "", false, ErrStopped
	}
	v, found := n.KV[key]
	return v, found, nil
}

// Compact creates a durable snapshot at an applied index and removes the log
// prefix. A lagging follower is later brought up to this point via an
// InstallSnapshot message rather than requiring the old log forever.
func (c *Cluster) Compact(id int, upto uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[id]
	if !ok {
		return ErrUnknown
	}
	if upto > n.Applied || upto <= n.SnapshotIndex {
		return ErrBadSnap
	}
	term, ok := n.termAt(upto)
	if !ok {
		return ErrBadSnap
	}
	remaining := make([]Entry, 0)
	for _, entry := range n.Log {
		if entry.Index > upto {
			remaining = append(remaining, entry)
		}
	}
	n.Log = remaining
	n.SnapshotIndex = upto
	n.SnapshotTerm = term
	n.SnapshotKV = cloneMap(n.KV)
	return c.persistLocked(n)
}

// Snapshot returns a detached view of every node, suitable for diagnostics.
func (c *Cluster) Snapshot() []Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := make([]int, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	result := make([]Node, 0, len(ids))
	for _, id := range ids {
		n := *c.nodes[id]
		n.Log = append([]Entry(nil), c.nodes[id].Log...)
		n.KV = cloneMap(c.nodes[id].KV)
		n.SnapshotKV = cloneMap(c.nodes[id].SnapshotKV)
		n.storage = nil
		n.nextIndex, n.matchIndex, n.votes = nil, nil, nil
		result = append(result, n)
	}
	return result
}

func (c *Cluster) startElectionLocked(candidate *Node) {
	if !candidate.Alive {
		return
	}
	c.leader = -1
	candidate.State = Candidate
	candidate.Term++
	candidate.VotedFor = candidate.ID
	candidate.votes = map[int]bool{candidate.ID: true}
	candidate.electionTicks = 0
	_ = c.persistLocked(candidate)
	lastIndex, lastTerm := candidate.lastIndex(), candidate.lastTerm()
	for id, peer := range c.nodes {
		if id == candidate.ID || !peer.Alive || c.blockedLocked(candidate.ID, id) {
			continue
		}
		c.queue = append(c.queue, message{kind: requestVote, from: candidate.ID, to: id, term: candidate.Term, lastLogIndex: lastIndex, lastLogTerm: lastTerm})
	}
}

func (c *Cluster) becomeLeaderLocked(n *Node) {
	n.State = Leader
	c.leader = n.ID
	n.heartbeatTicks = 0
	last := n.lastIndex()
	for id := range c.nodes {
		n.nextIndex[id] = last + 1
		n.matchIndex[id] = 0
	}
	n.matchIndex[n.ID] = last
	c.broadcastAppendLocked(n)
}

func (c *Cluster) broadcastAppendLocked(leader *Node) {
	for id, peer := range c.nodes {
		if id == leader.ID || !peer.Alive || c.blockedLocked(leader.ID, id) {
			continue
		}
		c.sendAppendLocked(leader, id)
	}
}

func (c *Cluster) sendAppendLocked(leader *Node, peerID int) {
	next := leader.nextIndex[peerID]
	if next == 0 {
		next = leader.lastIndex() + 1
		leader.nextIndex[peerID] = next
	}
	peer := c.nodes[peerID]
	if next <= leader.SnapshotIndex {
		c.queue = append(c.queue, message{kind: installSnapshot, from: leader.ID, to: peerID, term: leader.Term, snapshot: PersistentState{
			CurrentTerm: leader.Term, VotedFor: -1, SnapshotIndex: leader.SnapshotIndex,
			SnapshotTerm: leader.SnapshotTerm, SnapshotKV: cloneMap(leader.SnapshotKV), CommitIndex: leader.SnapshotIndex,
		}})
		return
	}
	prev := next - 1
	prevTerm, ok := leader.termAt(prev)
	if !ok {
		prevTerm = 0
	}
	entries := leader.entriesFrom(next)
	_ = peer
	c.queue = append(c.queue, message{kind: appendEntries, from: leader.ID, to: peerID, term: leader.Term, prevLogIndex: prev, prevLogTerm: prevTerm, entries: entries, leaderCommit: leader.CommitIndex})
}

func (c *Cluster) drainLocked() {
	for steps := 0; len(c.queue) > 0 && steps < 100000; steps++ {
		m := c.queue[0]
		c.queue = c.queue[1:]
		if !c.deliverableLocked(m) {
			continue
		}
		c.handleLocked(m)
	}
	if len(c.queue) > 0 {
		c.lastError = errors.New("raft: message queue did not quiesce")
		c.queue = nil
	}
}

func (c *Cluster) deliverableLocked(m message) bool {
	from, to := c.nodes[m.from], c.nodes[m.to]
	return from != nil && to != nil && from.Alive && to.Alive && !c.blockedLocked(m.from, m.to)
}

func (c *Cluster) handleLocked(m message) {
	n := c.nodes[m.to]
	switch m.kind {
	case requestVote:
		c.handleVoteRequestLocked(n, m)
	case voteResponse:
		c.handleVoteResponseLocked(n, m)
	case appendEntries:
		c.handleAppendLocked(n, m)
	case appendResponse:
		c.handleAppendResponseLocked(n, m)
	case installSnapshot:
		c.handleInstallSnapshotLocked(n, m)
	case snapshotResponse:
		c.handleSnapshotResponseLocked(n, m)
	}
}

func (c *Cluster) handleVoteRequestLocked(n *Node, m message) {
	if m.term < n.Term {
		c.enqueueVoteResponseLocked(n.ID, m.from, n.Term, false)
		return
	}
	if m.term > n.Term {
		c.becomeFollowerLocked(n, m.term)
	}
	upToDate := m.lastLogTerm > n.lastTerm() || (m.lastLogTerm == n.lastTerm() && m.lastLogIndex >= n.lastIndex())
	grant := upToDate && (n.VotedFor == -1 || n.VotedFor == m.from)
	if grant {
		n.VotedFor = m.from
		n.electionTicks = 0
		_ = c.persistLocked(n)
	}
	c.enqueueVoteResponseLocked(n.ID, m.from, n.Term, grant)
}

func (c *Cluster) enqueueVoteResponseLocked(from, to int, term uint64, granted bool) {
	c.queue = append(c.queue, message{kind: voteResponse, from: from, to: to, term: term, voteGranted: granted})
}

func (c *Cluster) handleVoteResponseLocked(n *Node, m message) {
	if n.State != Candidate || m.term < n.Term {
		return
	}
	if m.term > n.Term {
		c.becomeFollowerLocked(n, m.term)
		return
	}
	if m.voteGranted {
		n.votes[m.from] = true
		if len(n.votes) >= c.quorum() {
			c.becomeLeaderLocked(n)
		}
	}
}

func (c *Cluster) handleAppendLocked(n *Node, m message) {
	if m.term < n.Term {
		c.enqueueAppendResponseLocked(n.ID, m.from, n.Term, false, n.lastIndex())
		return
	}
	if m.term > n.Term {
		c.becomeFollowerLocked(n, m.term)
	}
	if n.State != Follower {
		n.State = Follower
	}
	c.leader = m.from
	n.electionTicks = 0
	if m.prevLogIndex > n.lastIndex() {
		c.enqueueAppendResponseLocked(n.ID, m.from, n.Term, false, n.lastIndex())
		return
	}
	if m.prevLogIndex > n.SnapshotIndex {
		term, _ := n.termAt(m.prevLogIndex)
		if term != m.prevLogTerm {
			c.enqueueAppendResponseLocked(n.ID, m.from, n.Term, false, n.lastIndex())
			return
		}
	} else if m.prevLogIndex == n.SnapshotIndex && m.prevLogTerm != n.SnapshotTerm {
		c.enqueueAppendResponseLocked(n.ID, m.from, n.Term, false, n.lastIndex())
		return
	}
	for _, entry := range m.entries {
		if entry.Index <= n.lastIndex() {
			term, _ := n.termAt(entry.Index)
			if term != entry.Term {
				n.truncateAfter(entry.Index - 1)
			}
		}
		if entry.Index > n.lastIndex() {
			n.Log = append(n.Log, entry)
		}
	}
	if m.leaderCommit > n.CommitIndex {
		n.CommitIndex = min(m.leaderCommit, n.lastIndex())
		n.apply()
	}
	if err := c.persistLocked(n); err != nil {
		c.lastError = err
	}
	c.enqueueAppendResponseLocked(n.ID, m.from, n.Term, true, n.lastIndex())
}

func (c *Cluster) enqueueAppendResponseLocked(from, to int, term uint64, success bool, match uint64) {
	c.queue = append(c.queue, message{kind: appendResponse, from: from, to: to, term: term, success: success, matchIndex: match})
}

func (c *Cluster) handleAppendResponseLocked(n *Node, m message) {
	if n.State != Leader || m.term != n.Term {
		if m.term > n.Term {
			c.becomeFollowerLocked(n, m.term)
		}
		return
	}
	if !m.success {
		if n.nextIndex[m.from] > 1 {
			n.nextIndex[m.from]--
		}
		c.sendAppendLocked(n, m.from)
		return
	}
	if m.matchIndex > n.matchIndex[m.from] {
		n.matchIndex[m.from] = m.matchIndex
	}
	n.nextIndex[m.from] = n.matchIndex[m.from] + 1
	c.advanceCommitLocked(n)
	if n.nextIndex[m.from] <= n.lastIndex() {
		c.sendAppendLocked(n, m.from)
	}
}

func (c *Cluster) advanceCommitLocked(leader *Node) {
	for index := leader.lastIndex(); index > leader.CommitIndex; index-- {
		term, ok := leader.termAt(index)
		if !ok || term != leader.Term {
			continue
		}
		acks := 0
		for id, peer := range c.nodes {
			if peer.Alive && (id == leader.ID || leader.matchIndex[id] >= index) {
				acks++
			}
		}
		if acks >= c.quorum() {
			leader.CommitIndex = index
			leader.apply()
			_ = c.persistLocked(leader)
			c.broadcastAppendLocked(leader)
			return
		}
	}
}

func (c *Cluster) handleInstallSnapshotLocked(n *Node, m message) {
	if m.term < n.Term {
		c.queue = append(c.queue, message{kind: snapshotResponse, from: n.ID, to: m.from, term: n.Term, success: false, matchIndex: n.lastIndex()})
		return
	}
	if m.term > n.Term {
		c.becomeFollowerLocked(n, m.term)
	}
	n.State = Follower
	c.leader = m.from
	n.electionTicks = 0
	snapshot := m.snapshot
	if snapshot.SnapshotIndex > n.SnapshotIndex {
		n.SnapshotIndex = snapshot.SnapshotIndex
		n.SnapshotTerm = snapshot.SnapshotTerm
		n.SnapshotKV = cloneMap(snapshot.SnapshotKV)
		n.KV = cloneMap(snapshot.SnapshotKV)
		remaining := make([]Entry, 0)
		for _, entry := range n.Log {
			if entry.Index > n.SnapshotIndex {
				remaining = append(remaining, entry)
			}
		}
		n.Log = remaining
		n.CommitIndex = max(n.CommitIndex, n.SnapshotIndex)
		n.Applied = n.SnapshotIndex
		n.apply()
		if err := c.persistLocked(n); err != nil {
			c.lastError = err
		}
	}
	c.queue = append(c.queue, message{kind: snapshotResponse, from: n.ID, to: m.from, term: n.Term, success: true, matchIndex: n.SnapshotIndex})
}

func (c *Cluster) handleSnapshotResponseLocked(n *Node, m message) {
	if n.State != Leader || m.term != n.Term {
		return
	}
	if m.success {
		n.matchIndex[m.from] = m.matchIndex
		n.nextIndex[m.from] = m.matchIndex + 1
		c.sendAppendLocked(n, m.from)
	}
}

func (c *Cluster) becomeFollowerLocked(n *Node, term uint64) {
	if term > n.Term {
		n.Term = term
		n.VotedFor = -1
	}
	n.State = Follower
	n.electionTicks = 0
	if c.leader == n.ID {
		c.leader = -1
	}
	if err := c.persistLocked(n); err != nil {
		c.lastError = err
	}
}

func (c *Cluster) persistLocked(n *Node) error {
	if n.storage == nil {
		return nil
	}
	err := n.storage.Save(PersistentState{
		CurrentTerm: n.Term, VotedFor: n.VotedFor, Log: append([]Entry(nil), n.Log...),
		CommitIndex: n.CommitIndex, SnapshotIndex: n.SnapshotIndex, SnapshotTerm: n.SnapshotTerm,
		SnapshotKV: cloneMap(n.SnapshotKV),
	})
	n.persistErr = err
	return err
}

func (c *Cluster) quorum() int { return len(c.nodes)/2 + 1 }

func (c *Cluster) liveIDsLocked() []int {
	ids := make([]int, 0)
	for id, n := range c.nodes {
		if n.Alive {
			ids = append(ids, id)
		}
	}
	sort.Ints(ids)
	return ids
}

func (c *Cluster) blockedLocked(a, b int) bool { return c.partitioned[[2]int{a, b}] }

func (n *Node) lastIndex() uint64 {
	if len(n.Log) == 0 {
		return n.SnapshotIndex
	}
	return n.Log[len(n.Log)-1].Index
}

func (n *Node) lastTerm() uint64 {
	if len(n.Log) == 0 {
		return n.SnapshotTerm
	}
	return n.Log[len(n.Log)-1].Term
}

func (n *Node) termAt(index uint64) (uint64, bool) {
	if index == n.SnapshotIndex && index != 0 {
		return n.SnapshotTerm, true
	}
	if index <= n.SnapshotIndex || index == 0 {
		return 0, index == 0
	}
	position := index - n.SnapshotIndex - 1
	if position >= uint64(len(n.Log)) || n.Log[position].Index != index {
		return 0, false
	}
	return n.Log[position].Term, true
}

func (n *Node) entriesFrom(index uint64) []Entry {
	if index <= n.SnapshotIndex {
		return append([]Entry(nil), n.Log...)
	}
	position := index - n.SnapshotIndex - 1
	if position >= uint64(len(n.Log)) {
		return nil
	}
	return append([]Entry(nil), n.Log[position:]...)
}

func (n *Node) truncateAfter(index uint64) {
	if index <= n.SnapshotIndex {
		n.Log = nil
		return
	}
	position := index - n.SnapshotIndex
	if position < uint64(len(n.Log)) {
		n.Log = n.Log[:position]
	}
}

func (n *Node) apply() {
	for n.Applied < n.CommitIndex {
		index := n.Applied + 1
		if index <= n.SnapshotIndex {
			n.Applied = n.SnapshotIndex
			continue
		}
		position := index - n.SnapshotIndex - 1
		if position >= uint64(len(n.Log)) {
			return
		}
		entry := n.Log[position]
		if entry.Command.Delete {
			delete(n.KV, entry.Command.Key)
		} else {
			n.KV[entry.Command.Key] = entry.Command.Value
		}
		n.Applied = index
	}
}

func clonePersistent(state PersistentState) PersistentState {
	state.Log = append([]Entry(nil), state.Log...)
	state.SnapshotKV = cloneMap(state.SnapshotKV)
	return state
}

func cloneMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
