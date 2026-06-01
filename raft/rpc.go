package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// send 统一出口：自动填充 From / Term，避免分散在各处手填
func (r *Raft) send(m pb.Message) {
	m.From = r.id
	if m.Term == 0 {
		m.Term = r.Term
	}
	r.msgs = append(r.msgs, m)
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

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
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
