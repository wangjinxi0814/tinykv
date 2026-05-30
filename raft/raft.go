// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"sort"
	"sync"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

// VoteResult 表示一轮选举的当前结果
type VoteResult int

const (
	VotePending VoteResult = iota
	VoteWon
	VoteLost
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// 全局随机源：用于选举超时随机化，避免所有节点同时发起选举
var (
	globalRandMu sync.Mutex
	globalRand   = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func randIntn(n int) int {
	globalRandMu.Lock()
	defer globalRandMu.Unlock()
	return globalRand.Intn(n)
}

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

// validate 参数合理性校验
func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}
	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}
	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}
	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}
	return nil
}

// Progress 表示 Leader 视角下每个 Follower 的日志复制进度，
// Leader 据此决定向各 Follower 发送哪些 entries。
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id   uint64
	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int

	// baseline of election interval
	electionTimeout int

	// 实际使用的随机化选举超时，[electionTimeout, 2*electionTimeout)
	randomizedElectionTimeout int

	heartbeatElapsed int

	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// 动态可替换的 tick 闭包：Follower/Candidate 用 tickElection，Leader 用 tickHeartbeat
	tick func()
}

// newRaft 根据给定 Config 构造一个 Raft 实例
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	raftLog := newLog(c.Storage)
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	// 优先使用 Storage 中持久化的成员列表；测试场景下 c.peers 直接给出
	peers := c.peers
	if len(confState.Nodes) > 0 {
		peers = confState.Nodes
	}

	r := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          raftLog,
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
	}

	for _, p := range peers {
		r.Prs[p] = &Progress{}
	}

	// 启动时默认 Follower 状态（任期沿用 HardState，无 Leader）
	r.becomeFollower(r.Term, None)
	return r
}

// Tick 逻辑计时器
func (r *Raft) Tick() {
	if r.tick != nil {
		r.tick()
	}
}

// Follower 和 Candidate 状态下运行的时钟逻辑
func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomizedElectionTimeout {
		r.electionElapsed = 0
		_ = r.Step(pb.Message{
			From:    r.id,
			To:      r.id,
			MsgType: pb.MessageType_MsgHup,
		})
	}
}

// Leader 状态下运行的时钟逻辑
func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		_ = r.Step(pb.Message{
			From:    r.id,
			To:      r.id,
			MsgType: pb.MessageType_MsgBeat,
		})
	}
}

// [electionTimeout, 2 * electionTimeout)
func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + randIntn(r.electionTimeout)
}

// becomeFollower 把当前节点转为 Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	// 仅在任期真正改变时才清空本任期的投票，避免覆盖已持久化 / 本任期已投出的票
	if term != r.Term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = lead
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.tick = r.tickElection
}

// becomeCandidate 把当前节点转为 Candidate（任期 +1，重置票箱）
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	// 票箱重置；自投票由 campaign 通过 poll 统一记入
	r.votes = make(map[uint64]bool)

	r.tick = r.tickElection
}

// becomeLeader 把当前节点转为 Leader，并初始化所有 Follower 的复制进度
func (r *Raft) becomeLeader() {
	// NOTE: Leader 上任后应当 propose 一条本任期的 noop entry（2AB 实现）
	r.State = StateLeader
	r.Lead = r.id
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.leadTransferee = None
	r.tick = r.tickHeartbeat

	lastIndex := r.RaftLog.LastIndex()
	for peer := range r.Prs {
		r.Prs[peer] = &Progress{
			Match: 0,
			Next:  lastIndex + 1,
		}
	}
	// 追加并广播本任期的 noop entry；appendEntries 内部会把 leader 自身的进度
	// 推进到末尾，followers 的 Next 此前已置为 lastIndex+1 即 noop 的位置
	r.appendEntries([]*pb.Entry{{}})
	r.bcastAppend()
	// 单节点集群：noop 立即满足多数派，直接提交
	r.maybeCommit()
}

// appendEntries 把客户端提议的 entries 追加到 leader 自己的日志，
// 填好 term/index 并推进 leader 自身的 Progress
func (r *Raft) appendEntries(es []*pb.Entry) {
	li := r.RaftLog.LastIndex()
	for i, e := range es {
		e.Term = r.Term
		e.Index = li + 1 + uint64(i)
		r.RaftLog.entries = append(r.RaftLog.entries, *e)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
}

// bcastAppend 向所有 Follower 发送 MsgAppend
func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

// maybeCommit 尝试把 committed 推进到多数派已复制的最大 index。
// 推进成功返回 true。遵循论文 5.4.2：只提交当前任期的日志。
func (r *Raft) maybeCommit() bool {
	matches := make(uint64Slice, 0, len(r.Prs))
	for _, pr := range r.Prs {
		matches = append(matches, pr.Match)
	}
	sort.Sort(matches)
	// 升序排序后，下标 (n-1)/2 处即多数派都能达到的最大 index
	mci := matches[(len(matches)-1)/2]
	if mci <= r.RaftLog.committed {
		return false
	}
	if term, err := r.RaftLog.Term(mci); err != nil || term != r.Term {
		return false
	}
	r.RaftLog.committed = mci
	return true
}

// campaign 发起一次选举：转为 Candidate 并向其他 peer 拉票
func (r *Raft) campaign() {
	r.becomeCandidate()

	// 自投一票，统一走 poll 计票；单节点集群此时已构成多数派，直接登基
	if r.poll(r.id, true) == VoteWon {
		r.becomeLeader()
		return
	}

	// 携带本地日志末尾的 (term, index)，供对方做 5.4.1 的 up-to-date 检查
	lastIndex, lastTerm := r.lastLogIndexTerm()
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      peer,
			LogTerm: lastTerm,
			Index:   lastIndex,
		})
	}
}

// poll 记录一票并返回当前选举结果
func (r *Raft) poll(id uint64, granted bool) VoteResult {
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = granted
	}
	grants, rejects := 0, 0
	for _, v := range r.votes {
		if v {
			grants++
		} else {
			rejects++
		}
	}
	quorum := len(r.Prs)/2 + 1
	switch {
	case grants >= quorum:
		return VoteWon
	case rejects >= quorum:
		return VoteLost
	default:
		return VotePending
	}
}

// send 统一出口：自动填充 From / Term，避免分散在各处手填
func (r *Raft) send(m pb.Message) {
	m.From = r.id
	if m.Term == 0 {
		m.Term = r.Term
	}
	r.msgs = append(r.msgs, m)
}

// lastLogIndexTerm 取本地日志末尾的 (index, term)
func (r *Raft) lastLogIndexTerm() (uint64, uint64) {
	idx := r.RaftLog.LastIndex()
	term, _ := r.RaftLog.Term(idx)
	return idx, term
}

// isLogUpToDate 根据 Raft 论文 5.4.1 判断对方日志是否至少跟我一样新
func (r *Raft) isLogUpToDate(lastTerm, lastIndex uint64) bool {
	myIndex, myTerm := r.lastLogIndexTerm()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= myIndex
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2AB).
	pr := r.Prs[to]
	if pr == nil {
		return false
	}
	prevIndex := pr.Next - 1
	prevTerm, err := r.RaftLog.Term(prevIndex)
	if err != nil {
		// prevIndex 已被快照压缩，无法构造 MsgAppend，需改发快照（2C 处理）
		return false
	}
	ents := r.RaftLog.entriesFrom(pr.Next)
	sendEnts := make([]*pb.Entry, 0, len(ents))
	for i := range ents {
		sendEnts = append(sendEnts, &ents[i])
	}
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		Index:   prevIndex,
		LogTerm: prevTerm,
		Entries: sendEnts,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// 心跳携带的 commit 不能超过该 Follower 已匹配的进度，否则会让落后的
	// Follower 误提交它尚未拥有的日志
	commit := r.RaftLog.committed
	if pr := r.Prs[to]; pr != nil && pr.Match < commit {
		commit = pr.Match
	}
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		Commit:  commit,
	})
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
