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

	// 随机数种子, 用于随机选举超时时间
	rand *rand.Rand

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
		rand:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	for _, p := range peers {
		r.Prs[p] = &Progress{}
	}

	// 从持久化的 HardState 恢复 committed；重启时还要恢复 applied
	if !IsEmptyHardState(hardState) {
		raftLog.committed = hardState.Commit
	}
	if c.Applied > 0 {
		raftLog.applied = c.Applied
	}

	// 启动时默认 Follower 状态（任期沿用 HardState，无 Leader）
	r.becomeFollower(r.Term, None)
	return r
}

// softState 返回当前易失状态（leader + 角色）
func (r *Raft) softState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

// hardState 返回需要持久化的状态（term/vote/commit）
func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
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
	r.randomizedElectionTimeout =
		r.electionTimeout + r.rand.Intn(r.electionTimeout)
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

// bcastHeartbeat 向所有 Follower 发送 MsgHeartbeat
func (r *Raft) bcastHeartbeat() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
}

// maybeCommit 尝试把 committed 推进到多数派已复制的最大 index。
// 推进成功返回 true。
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
	// 遵循论文 5.4.2：只能提交当前 Term 的日志
	if term, err := r.RaftLog.Term(mci); err != nil || term != r.Term {
		return false
	}
	r.RaftLog.committed = mci
	return true
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
