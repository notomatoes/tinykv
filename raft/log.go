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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).

	// 日志中第一个条目的索引,即快照条目与日志分界线
	first uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hardState, _, _ := storage.InitialState()
	first, _ := storage.FirstIndex()
	last, _ := storage.LastIndex()
	entries, _ := storage.Entries(first, last+1)

	log := &RaftLog{
		storage:   storage,
		committed: hardState.Commit,
		applied:   first - 1,
		stabled:   last,
		entries:   entries,
		first:     first,
	}

	return log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 && l.LastIndex() > l.stabled {
		return l.entries[l.stabled:]
	}
	return make([]pb.Entry, 0)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.applied-l.first+1 : l.committed-l.first+1]
	}
	return
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).

	// 若entries存在,则返回entries中的lastIndex
	if len(l.entries) != 0 {
		return l.entries[len(l.entries)-1].Index
	}
	// 若entries不存在,则返回storage中的lastIndex
	idx, _ := l.storage.LastIndex()

	return idx
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).

	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	if len(l.entries) > 0 && i >= l.first {
		return l.entries[i-l.first].Term, nil
	}
	return l.storage.Term(i)
}

// 添加数据到RaftLog中,返回最后一条日志的索引
func (l *RaftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}
	// 若索引小于committed,则数据非法
	if k := ents[0].Index - 1; k < l.committed {
		//fmt.Sprintf("append Error,entrie's index(%d) must <= committed(%d).\n", k, l.committed)
		return 0
	}
	k := ents[0].Index
	switch {
	// 若正好紧接着当前数据,则直接append
	case k == l.first+uint64(len(l.entries)):
		l.entries = append(l.entries, ents...)
	// 若比当前entries的起始位置小,则需要用新数据替换旧数据,并更改first和entries
	case k <= l.first:
		l.first = k
		l.entries = ents
	// 默认情况,则说明l.first < k < l.first + uint64(len(l.entries)),则需要拼接
	default:
		l.entries = append([]pb.Entry{}, l.entries[0:k-1]...)
		l.entries = append(l.entries, ents...)
	}

	// 若更改了stabled之前的数据,则更新stabled
	if l.stabled >= k {
		l.stabled = k - 1
	}

	return l.LastIndex()
}

// 判断是否比当前节点的日志更新：1）term是否更大 2）term相同的情况下，索引是否更大
func (l *RaftLog) isUpToDate(lastI, term uint64) bool {
	lastIdx := l.LastIndex()
	lastTerm, _ := l.Term(lastIdx)

	return term > lastTerm || (term == lastTerm && lastI >= lastIdx)
}
