package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// Step 是所有消息的统一入口，具体消息类型见 eraftpb.proto 中的 MessageType
func (r *Raft) Step(m pb.Message) error {
	// 论文 §5.1, 任何时候收到更高任期的消息：先无条件回退为 Follower
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
		// Lead / electionElapsed 的刷新已在 handleHeartbeat 中完成
		r.handleHeartbeat(m)
	case pb.MessageType_MsgPropose:
		// discard 或者转发给leader 
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
	case pb.MessageType_MsgPropose:

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
		// 2AB 中会用响应推动日志复制；2AA 阶段忽略
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	}
	return nil
}