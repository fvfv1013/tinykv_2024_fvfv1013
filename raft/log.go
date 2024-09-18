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
	"fmt"
	"github.com/pingcap-incubator/tinykv/dbg"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pkg/errors"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
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
	compacted uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	// [0+1, compacted] 已压缩
	// [compacted+1, applied] 已应用
	// [applied+1, committed] 已提交
	// [committed+1, stabled] 已持久化
	// [stabled+1, lastIndex] 已接受
	fi, _ := storage.FirstIndex()
	li, _ := storage.LastIndex()
	// fi-1使得包括开始条目前的脏条目，li+1使得刚好包括li指向的条目(li+1不会在内)
	// fi-1会报"requested index is unavailable due to compaction"的错误，手动添加脏条目
	ents := make([]pb.Entry, 1)
	moreents, _ := storage.Entries(fi, li+1)
	for _, m := range moreents {
		ents = append(ents, m)
	}
	l := &RaftLog{
		storage:   storage,
		entries:   ents,
		compacted: fi - 1,
		applied:   fi - 1,
		committed: fi - 1,
		stabled:   li,
	}
	return l
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	fi, _ := l.storage.FirstIndex()
	if fi-1 > l.compacted {
		l.compacted = fi - 1
		// 保留一个脏条目
		l.entries = l.getEntries(l.compacted, l.LastIndex()+1)
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.getEntries(l.compacted+1, l.LastIndex()+1)
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.getEntries(l.stabled+1, l.LastIndex()+1)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return l.getEntries(l.applied+1, l.committed+1)
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.compacted + uint64(len(l.entries)-1)
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if l.InSnapshot(i) {
		return 0, ErrCompacted
	}
	if i > l.LastIndex() {
		return 0, errors.New("索引越界")
	}
	return l.getEntry(i).Term, nil
}

// 自行检查索引值越界和添加脏位
func (l *RaftLog) getEntries(lo uint64, hi uint64) []pb.Entry {
	return l.entries[lo-l.compacted : hi-l.compacted]
}

// 自行检查索引值越界
func (l *RaftLog) getEntry(i uint64) pb.Entry {
	return l.entries[i-l.compacted]
}

// 自行检查索引值越界和添加脏位
func (l *RaftLog) getEntriesPointers(lo uint64, hi uint64) []*pb.Entry {
	ents := make([]*pb.Entry, hi-lo)
	for i := lo; i < hi; i++ {
		ents[i-lo] = &l.entries[i-l.compacted]
	}
	return ents
}

func (l *RaftLog) InSnapshot(i uint64) bool {
	return i <= l.compacted
}

func (l *RaftLog) ApplyTo(i uint64) {
	if i > l.applied {
		l.applied = min(l.committed, i)
	}
}

//func (l *RaftLog) CommitTo(i uint64) {
//	if i > l.committed {
//		l.committed = min(l.LastIndex(), i)
//	}
//}

func (l *RaftLog) AppendAndTag(raftTerm uint64, ents ...pb.Entry) {
	for _, e := range ents {
		e.Index = l.LastIndex() + 1
		e.Term = raftTerm
		l.entries = append(l.entries, e)
	}
}

func (l *RaftLog) AppendAndTagWithPointers(raftTerm uint64, ents []*pb.Entry) {
	for _, e := range ents {
		e.Index = l.LastIndex() + 1
		e.Term = raftTerm
		l.entries = append(l.entries, *e)
	}
}

func (l *RaftLog) AnotherTailWithPointers(previ uint64, ents []*pb.Entry) {
	if previ+1 <= l.committed {
		fmt.Printf("previ:%x, l.committed:%x, ents:%v, localents: %v\n", previ, l.committed, ents, l.entries)
		dbg.Panic("要增加的条目不应该出现在已提交的部分")
		return
	}
	l.entries = l.getEntries(0, previ+1)
	l.stabled = min(l.stabled, l.LastIndex())
	for _, e := range ents {
		l.entries = append(l.entries, *e)
	}
}

// LastTerm 返回最后一个条目的任期，前提是LastIndex!=l.compacted
func (l *RaftLog) LastTerm() uint64 {
	return l.getEntry(l.LastIndex()).Term
}

// 判断请求方日志是否合格
func (l *RaftLog) agree(lasti uint64, lastt uint64) bool {
	return lastt > l.LastTerm() || (lastt == l.LastTerm() && lasti >= l.LastIndex())
}

type PrevCategories int

const (
	OutdatedMatched PrevCategories = iota
	PrevMatched
	ConflictOrNotExist
)

func (l *RaftLog) prevIndexTermQualified(previ uint64, prevt uint64) PrevCategories {
	if previ+1 <= l.committed {
		return OutdatedMatched
	}
	if previ == 0 {
		// 不存在所谓前缀
		if prevt == 0 {
			return PrevMatched
		}
		return ConflictOrNotExist
	}
	if previ <= l.compacted {
		// 已压缩的单独查验任期
		s, _ := l.storage.Snapshot()
		if !IsEmptySnap(&s) && prevt == s.Metadata.Term {
			return PrevMatched
		}
		return ConflictOrNotExist
	}
	if previ > l.LastIndex() {
		// 这意味着对应前缀都还不存在
		return ConflictOrNotExist
	}
	localt, _ := l.Term(previ)
	if localt != prevt {
		// 意味着存在矛盾的条目，删除该条目及其之后的条目
		//l.entries = l.getEntries(0, previ)
		return ConflictOrNotExist
	}
	return PrevMatched
}

func (l *RaftLog) inLogIndeed(i uint64, t uint64) bool {
	if i <= l.committed {
		return true
	}
	if i > l.LastIndex() {
		return false
	}
	localt, err := l.Term(i)
	if err != nil {
		return false
	}
	return localt == t
}

func (l *RaftLog) clearAppliedEntries() {
	l.entries = l.entries[l.applied:]
	l.compacted = l.applied
}

func (l *RaftLog) stableTo(i uint64, t uint64) {
	realt, err := l.Term(i)
	if err != nil {
		return
	}
	if realt == t && i > l.stabled {
		l.stabled = i
	}
}

func (l *RaftLog) snapStabled(i uint64) {
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
		l.pendingSnapshot = nil
	}
}
