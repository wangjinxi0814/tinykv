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
	ID uint64

	peers []uint64

	ElectionTick  int
	HeartbeatTick int

	Storage Storage
	Applied uint64
}

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

// Progress represents a follower's progress in the view of the leader.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

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

	// heartbeat interval
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// 实际使用的随机化选举超时，[electionTimeout, 2*electionTimeout)
	randomizedElectionTimeout int

	heartbeatElapsed int
	electionElapsed  int

	// 动态可替换的 tick 闭包：Follower/Candidate 用 tickElection，Leader 用 tickHeartbeat
	tick func()

	leadTransferee uint64

	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
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

// ------------------------------- 计时器 -------------------------------

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

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + randIntn(r.electionTimeout)
}

// ------------------------------- 状态转换 -------------------------------

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.tick = r.tickElection
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true

	r.tick = r.tickElection
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.tick = r.tickHeartbeat

	lastIndex := r.RaftLog.LastIndex()
	for peer := range r.Prs {
		r.Prs[peer] = &Progress{
			Match: 0,
			Next:  lastIndex + 1,
		}
	}
	// 自己显然已匹配到末尾
	if pr, ok := r.Prs[r.id]; ok {
		pr.Match = lastIndex
		pr.Next = lastIndex + 1
	}
}

// ------------------------------- Step 入口 -------------------------------

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// 收到更高任期的消息：先无条件回退为 Follower
	if m.Term > r.Term {
		lead := None
		switch m.MsgType {
		case pb.MessageType_MsgAppend, pb.MessageType_MsgHeartbeat, pb.MessageType_MsgSnapshot:
			lead = m.From
		}
		r.becomeFollower(m.Term, lead)
	}

	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		// 同 Term 的 Append 视为来自合法 Leader
		if m.Term == r.Term {
			r.Lead = m.From
			r.electionElapsed = 0
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		if m.Term == r.Term {
			r.Lead = m.From
			r.electionElapsed = 0
		}
		r.handleHeartbeat(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgAppend:
		// 同 Term 收到 Append：已有 Leader，回退
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		for peer := range r.Prs {
			if peer == r.id {
				continue
			}
			r.sendHeartbeat(peer)
		}
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeatResponse:
		// 2AB 中会用响应推动日志复制；2AA 阶段忽略
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	}
	return nil
}

// ------------------------------- 选举核心 -------------------------------

// campaign 发起一次选举：转为 Candidate 并向其他 peer 拉票
func (r *Raft) campaign() {
	r.becomeCandidate()

	// 单节点集群：自己就是多数派，直接登基
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)

	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      peer,
			Term:    r.Term,
			LogTerm: lastTerm,
			Index:   lastIndex,
		})
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	reject := true

	// Term 落后则直接拒绝（Step 入口已处理 m.Term > r.Term 的情况，到这里 m.Term <= r.Term）
	if m.Term == r.Term {
		// 本任期还没投过票，或者已经投给了同一个候选人
		canVote := r.Vote == None || r.Vote == m.From
		if canVote && r.isLogUpToDate(m.LogTerm, m.Index) {
			reject = false
			r.Vote = m.From
			r.electionElapsed = 0
		}
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  reject,
	})
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term != r.Term {
		return
	}
	r.votes[m.From] = !m.Reject

	grant, deny := 0, 0
	for _, v := range r.votes {
		if v {
			grant++
		} else {
			deny++
		}
	}

	quorum := len(r.Prs)/2 + 1
	switch {
	case grant >= quorum:
		r.becomeLeader()
	case deny >= quorum:
		// 多数派拒绝，回退为 Follower 等下次超时
		r.becomeFollower(r.Term, None)
	}
}

// isLogUpToDate 根据 Raft 论文 5.4.1 判断对方日志是否至少跟我一样新
func (r *Raft) isLogUpToDate(lastTerm, lastIndex uint64) bool {
	myIndex := r.RaftLog.LastIndex()
	myTerm, _ := r.RaftLog.Term(myIndex)
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= myIndex
}

// ------------------------------- 消息发送 -------------------------------

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2AB).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	})
}

// ------------------------------- 消息处理 -------------------------------

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2AB).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}
	// 收到合法 Leader 的心跳：刷新计时器
	r.electionElapsed = 0
	r.Lead = m.From
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
