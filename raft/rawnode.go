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

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// SoftState 是否相等（用于判断 Ready 中是否需要携带 SoftState）
func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft
	// 上一次 Ready 已经对外暴露的 soft/hard state，用于检测是否发生变化
	prevSoftSt *SoftState
	prevHardSt pb.HardState
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	r := newRaft(config)
	rn := &RawNode{Raft: r}
	rn.prevSoftSt = r.softState()
	rn.prevHardSt = r.hardState()
	return rn, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	if rn.Raft.tick != nil {
		rn.Raft.tick()
	}
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Advance 通知 raft：这批 Ready 已经处理完（已持久化/已应用/已发送），
// 据此推进 stabled/applied 指针，并更新 prev soft/hard state、清空待发消息。
func (rn *RawNode) Advance(rd Ready) {
	if rd.SoftState != nil {
		rn.prevSoftSt = rd.SoftState
	}
	if !IsEmptyHardState(rd.HardState) {
		rn.prevHardSt = rd.HardState
	}
	// 已持久化的最后一条日志 -> 推进 stabled
	if n := len(rd.Entries); n > 0 {
		rn.Raft.RaftLog.stabled = max(rn.Raft.RaftLog.stabled, rd.Entries[n-1].Index)
	}
	// 已应用的最后一条日志 -> 推进 applied
	if index := rd.AppliedCursor(); index > 0 {
		rn.Raft.RaftLog.applied = max(rn.Raft.RaftLog.applied, index)
	}
	// 消息已交给上层发送；快照已交给上层应用
	rn.Raft.msgs = nil
	rn.Raft.RaftLog.pendingSnapshot = nil
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	r := rn.Raft
	rd := Ready{
		Entries:          r.RaftLog.unstableEntries(),
		CommittedEntries: r.RaftLog.nextEnts(),
		Messages:         r.msgs,
	}
	// soft state 变化才携带（测试用 DeepEqual 严格比对，未变则保持 nil）
	if softSt := r.softState(); !softSt.equal(rn.prevSoftSt) {
		rd.SoftState = softSt
	}
	// hard state 变化才携带，否则保持空 HardState
	if hardSt := r.hardState(); !isHardStateEqual(hardSt, rn.prevHardSt) {
		rd.HardState = hardSt
	}
	if r.RaftLog.pendingSnapshot != nil {
		rd.Snapshot = *r.RaftLog.pendingSnapshot
	}
	return rd
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	r := rn.Raft
	// soft state 变化（角色/leader 变更）
	if !r.softState().equal(rn.prevSoftSt) {
		return true
	}
	// hard state 变化（term/vote/commit 需要持久化）
	if hardSt := r.hardState(); !IsEmptyHardState(hardSt) && !isHardStateEqual(hardSt, rn.prevHardSt) {
		return true
	}
	// 有待应用的快照
	if r.RaftLog.pendingSnapshot != nil {
		return true
	}
	// 有未持久化的日志
	if len(r.RaftLog.unstableEntries()) > 0 {
		return true
	}
	// 有已提交未应用的日志
	if len(r.RaftLog.nextEnts()) > 0 {
		return true
	}
	// 有待发送的消息
	if len(r.msgs) > 0 {
		return true
	}
	return false
}
