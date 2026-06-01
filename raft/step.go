package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// Step 是所有消息的统一入口，具体消息类型见 eraftpb.proto 中的 MessageType
func (r *Raft) Step(m pb.Message) error {
	// 第一层：按 term 过滤（论文 §5.1）
	switch {
	case m.Term == 0:
		// 本地消息（MsgHup / MsgBeat / MsgPropose 等不带 term），放行

	case m.Term > r.Term:
		// 更高任期：无条件回退为 Follower；仅来自合法 Leader 的消息才记录 Lead
		lead := None
		switch m.MsgType {
		case pb.MessageType_MsgAppend, pb.MessageType_MsgHeartbeat, pb.MessageType_MsgSnapshot:
			lead = m.From
		}
		r.becomeFollower(m.Term, lead)

	case m.Term < r.Term:
		// 更低任期：直接忽略丢弃（对齐 etcd 默认行为）。过期节点迟早会通过收到
		// 更高 term 的消息自己发现落后，无需在这里回任何响应。
		// 注：etcd 在开启 checkQuorum 或对 PreVote 时才会回复，本实现两者都没有。
		return nil
	}

	// 第二层：MsgHup 与投票请求是与状态无关的公共逻辑，集中在这里处理；
	// 其余消息再按角色下发到对应状态机。
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// 已经是 Leader 就忽略（对齐 etcd：MsgHup 在 Leader 状态下不发起选举）；
		// 否则发起竞选（campaign 内部按当前状态决定要不要重置选举状态）
		if r.State != StateLeader {
			r.campaign()
		}
	case pb.MessageType_MsgRequestVote:
		// 投不投票的决策与角色无关：是否已投票 + 日志是否够新
		r.handleRequestVote(m)
	default:
		// 按当前角色分发到 etcd 同款的 stepX 函数。
		// 注意：这里实时读 r.State 派发，而非 etcd 的缓存 r.step 字段——因为
		// TinyKV 的测试（raft_test.go:662/965）会直接给 r.State 赋值而不走
		// becomeXxx，缓存指针会与 State 脱节。函数签名本身与 etcd 完全一致。
		switch r.State {
		case StateFollower:
			return stepFollower(r, m)
		case StateCandidate:
			return stepCandidate(r, m)
		case StateLeader:
			return stepLeader(r, m)
		}
	}
	return nil
}

func stepFollower(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		// 走到这里 m.Term == r.Term，视为来自合法 Leader
		r.Lead = m.From
		r.electionElapsed = 0
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		// Lead / electionElapsed 的刷新已在 handleHeartbeat 中完成
		r.handleHeartbeat(m)
	case pb.MessageType_MsgPropose:
		// discard 或者转发给 leader
	}
	return nil
}

func stepCandidate(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgAppend:
		// 同 Term 收到 Append：已有 Leader，回退
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgPropose:
		// discard
	}
	return nil
}

func stepLeader(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgPropose:
		// 客户端提议：追加到本地日志并广播复制
		if len(m.Entries) > 0 {
			r.appendEntries(m.Entries)
			r.bcastAppend()
			// 单节点集群下没有 Follower 应答，需要在这里直接尝试提交
			r.maybeCommit()
		}
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	}
	return nil
}
