package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// handleRequestVote 处理 Candidate 拉票请求，回复 MsgRequestVoteResponse
// 前置条件：m.Term == r.Term（更低任期已在 Step 入口被 reject，更高任期已被拉平）
func (r *Raft) handleRequestVote(m pb.Message) {
	reject := true

	// 本任期还没投过票，或者已经投给了同一个候选人
	canVote := r.Vote == None || r.Vote == m.From
	if canVote && r.isLogUpToDate(m.LogTerm, m.Index) {
		reject = false
		r.Vote = m.From
		r.electionElapsed = 0
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
// 前置条件：m.Term >= r.Term（更低任期已在 Step 入口被 reject 掉）
func (r *Raft) handleAppendEntries(m pb.Message) {
	// 来自合法 Leader：转换为 Follower，更新 term 和 leaderId，重置选举计时器
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
	//
	if pr := r.Prs[m.From]; pr != nil && pr.Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

// handleHeartbeat 处理来自 Leader 的 Heartbeat RPC
// 前置条件：m.Term >= r.Term（更低任期已在 Step 入口被 reject 掉）
func (r *Raft) handleHeartbeat(m pb.Message) {
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

// campaign 发起一次选举：转为 Candidate 并向其他 peer 拉票
func (r *Raft) campaign() {
	r.becomeCandidate()

	// 自投一票，统一走 poll 计票；单节点集群此时已构成多数派，转换为leader
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
