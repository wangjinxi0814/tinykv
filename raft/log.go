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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	storage Storage

	committed uint64
	applied   uint64
	stabled   uint64

	entries []pb.Entry

	pendingSnapshot *pb.Snapshot
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	return &RaftLog{
		storage:   storage,
		committed: firstIndex - 1,
		applied:   firstIndex - 1,
		stabled:   lastIndex,
		entries:   entries,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

func (l *RaftLog) allEntries() []pb.Entry {
	return l.entries
}

// unstableEntries return all the unstable entries
// 即还未持久化到 Storage 的日志：(stabled, last]
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	offset := l.entries[0].Index
	return l.entries[l.stabled+1-offset:]
}

// entriesFrom 返回 [lo, last] 区间的日志（leader 构造 MsgAppend 时使用）。
// 注意返回的是底层切片，调用方只读使用。
func (l *RaftLog) entriesFrom(lo uint64) []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	offset := l.entries[0].Index
	if lo < offset {
		lo = offset
	}
	pos := lo - offset
	if pos >= uint64(len(l.entries)) {
		return nil
	}
	return l.entries[pos:]
}

// truncateFrom 删除 idx（含）之后的所有日志，并在必要时回退 stabled。
func (l *RaftLog) truncateFrom(idx uint64) {
	if len(l.entries) == 0 {
		return
	}
	offset := l.entries[0].Index
	if idx < offset {
		return
	}
	if pos := idx - offset; pos < uint64(len(l.entries)) {
		l.entries = l.entries[:pos]
	}
	// 被截断的日志若已持久化，需要回退 stabled，让它们重新进入 unstable
	if l.stabled >= idx {
		l.stabled = idx - 1
	}
}

// truncateAndAppend 处理 follower 收到的新日志：从第一个冲突点开始，
// 截断本地冲突日志并追加 leader 的日志；已存在且 term 一致的条目跳过。
func (l *RaftLog) truncateAndAppend(ents []*pb.Entry) {
	for i := range ents {
		e := ents[i]
		if e.Index > l.LastIndex() {
			// 本地没有这条及之后的日志，整段追加
			for _, ne := range ents[i:] {
				l.entries = append(l.entries, *ne)
			}
			return
		}
		term, err := l.Term(e.Index)
		if err != nil || term != e.Term {
			// 冲突：截断 e.Index 及之后，再追加剩余新日志
			l.truncateFrom(e.Index)
			for _, ne := range ents[i:] {
				l.entries = append(l.entries, *ne)
			}
			return
		}
		// 已存在且 term 一致，跳过
	}
}

// nextEnts returns all the committed but not applied entries
// 从 entries 里切出：(applied, committed]
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if l.committed <= l.applied {
		return nil
	}

	offset := l.entries[0].Index

	return l.entries[l.applied+1-offset : l.committed+1-offset]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	i, _ := l.storage.LastIndex()
	return i
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if len(l.entries) > 0 && i >= l.entries[0].Index {
		offset := l.entries[0].Index
		idx := i - offset
		if idx < uint64(len(l.entries)) {
			return l.entries[idx].Term, nil
		}
	}
	return l.storage.Term(i)
}
