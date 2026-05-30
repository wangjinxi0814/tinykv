package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// handleRequestVote 处理 Candidate 拉票请求，回复 MsgRequestVoteResponse
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

	r.send(pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		Reject:  reject,
	})
}

// handleRequestVoteResponse 处理拉票响应，根据 poll 结果决定登基 / 退位 / 继续等待
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// 过期任期的响应直接丢弃，避免影响本轮计票
	if m.Term != r.Term {
		return
	}
	switch r.poll(m.From, !m.Reject) {
	case VoteWon:
		r.becomeLeader()
	case VoteLost:
		// 已被多数派拒绝，本轮选举无望，回退 Follower 等下次超时
		r.becomeFollower(r.Term, None)
	default:
	}
}

// handleAppendEntries 处理来自 Leader 的 AppendEntries RPC
func (r *Raft) handleAppendEntries(m pb.Message) {
	// 1. 如果 leader 的任期小于自己的任期返回 false。(5.1)
	if m.Term < r.Term {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			Reject:  true,
		})
		return
	}
	// 心跳报文：转换为 Follower，更新 term 和 leaderId，重置选举计时器
	r.becomeFollower(m.Term, m.From)

	lastNew := m.Index + uint64(len(m.Entries))
	// prevLogIndex 在快照范围内，直接接受（快照已覆盖）
	if snapIndex := m.Snapshot.GetMetadata().GetIndex(); m.Index < snapIndex {
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = min(m.Commit, lastNew)
		}
		return
	}

	// 2. 如果自己不存在索引、任期和 prevLogIndex、prevLogTerm 匹配的日志返回 false。(5.3)
	if m.Index > r.RaftLog.LastIndex() {
		// Case 3: log 太短，告诉 leader 本地最大 index 以便快速回退
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			Index:   r.RaftLog.LastIndex(),
			Reject:  true,
		})
		return
	}

	// prevLogIndex 处一致性检查
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		// 本地没有这条 或 任期对不上 → 拒绝
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			Index:   m.Index, // 告诉 leader 卡在哪
			Reject:  true,
		})
		return
	}

	// if rf.log[prevLogIndex-rf.lastIncludedIndex].Term != prevLogTerm	 {
	// 	// Case 1 & 2: 有冲突 term
	// 	reply.XTerm = rf.log[prevLogIndex-rf.lastIncludedIndex].Term
	// 	xIdx := prevLogIndex
	// 	for xIdx > rf.lastIncludedIndex && rf.log[xIdx-1-rf.lastIncludedIndex].Term == reply.XTerm {
	// 		xIdx--
	// 	}
	// 	reply.XIndex = xIdx // 绝对索引
	// 	reply.Success = false
	// 	return
	// }

	// 一致性检查通过：截断冲突日志并追加新日志（空 entries 即纯心跳/探测，跳过）
	if len(m.Entries) > 0 {
		r.RaftLog.truncateAndAppend(m.Entries)
	}

	// 4. 如果 leaderCommit>commitIndex, 设置本地 commitIndex 为 leaderCommit 和最新日志索引中较小的一个。
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(lastNew, m.Commit)
	}

	// 追加成功，回复最后一条新日志的 index，供 leader 推进 Match/Next
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		Index:   lastNew,
		Reject:  false,
	})
}

// handleAppendResponse 处理 Follower 对 MsgAppend 的应答（仅 Leader 调用）
func (r *Raft) handleAppendResponse(m pb.Message) {
	pr := r.Prs[m.From]
	if pr == nil {
		return
	}
	if m.Reject {
		// 日志不一致：回退 Next 后重试
		if pr.Next > 1 {
			pr.Next--
		}
		r.sendAppend(m.From)
		return
	}
	if m.Index > pr.Match {
		pr.Match = m.Index
		pr.Next = m.Index + 1
		// 多数派进度可能推进了 committed，需要再次广播让 Follower 同步 commit
		if r.maybeCommit() {
			r.bcastAppend()
		}
	}
}

// handleHeartbeatResponse 处理心跳应答（仅 Leader 调用）：若 Follower 日志落后则补发
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Reject {
		return
	}
	if pr := r.Prs[m.From]; pr != nil && pr.Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

// handleHeartbeat 处理来自 Leader 的 Heartbeat RPC
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Term 较低拒绝心跳
	if m.Term < r.Term {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			To:      m.From,
			Reject:  true,
		})
		return
	}
	// 收到合法 Leader 的心跳：刷新计时器
	r.electionElapsed = 0
	r.Lead = m.From
	// 心跳携带 leader 的 commit，推进本地 committed（不超过本地最新日志）
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
	})
}

// handleSnapshot 处理来自 Leader 的 Snapshot RPC
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}
