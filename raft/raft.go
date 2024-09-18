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
	"github.com/pingcap-incubator/tinykv/dbg"
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
	StatePreCandidate
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
	"StatePreCandidate",
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

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

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
	//heartbeatTimeout int
	// baseline of election interval
	//electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	//heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	//electionElapsed int

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

	disableProposalForwarding bool
	PState                    PeerConfChangeState
	NewPrs                    map[uint64]*Progress
	heartbeatTick             int
	electionTick              int

	voted map[uint64]bool

	// 1. FollowerDuty
	*DutyPrepareCampaign

	// 2. CandidateDuty
	*DutyKeepCampaign
	*DutyRestartTimeoutElection

	// 3. LeaderDuty
	*DutyBeat
	*DutyKeepSyncLog
	*DutyStopTimeoutTransfer

	// 4. PreCandidateDuty
	*DutyKeepPreCampaign
	*DutyDropTimeoutPreElection
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	// 1. initialize
	l := newLog(c.Storage)
	hs, cs, _ := c.Storage.InitialState()

	r := &Raft{
		id:            c.ID,
		RaftLog:       l,
		PState:        PState_NORMAL,
		Prs:           make(map[uint64]*Progress),
		heartbeatTick: c.HeartbeatTick,
		electionTick:  c.ElectionTick,
	}
	pList := c.peers
	if pList == nil {
		pList = cs.Nodes
	}
	for _, id := range pList {
		r.Prs[id] = &Progress{}
	}
	r.becomeFollower(0, None)

	// 2. load hard state
	r.Term = hs.Term
	r.Vote = hs.Vote
	r.CommitTo(hs.Commit)
	r.RaftLog.ApplyTo(c.Applied)
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// 1. 获取前缀索引
	var nexti uint64
	var previ uint64
	nexti = r.Ask(to).Next
	if nexti == 0 {
		dbg.Panic("不能发送脏日志")
		return true
	}
	previ = nexti - 1

	// 2. 根据前缀索引发送消息
	// 前缀不存在
	if previ == 0 {
		mm := pb.Message{MsgType: pb.MessageType_MsgAppend, To: to, From: r.id, Term: r.Term, Entries: r.RaftLog.getEntriesPointers(nexti, r.RaftLog.LastIndex()+1), Commit: r.RaftLog.committed}
		r.msgs = append(r.msgs, mm)
		dbg.Infof(dbg.DLogs, "S%x 向 S%x 发送了增加日志 %v 在时代 %d", r.id, to, mm.Entries, r.Term)
		return true
	}
	// 前缀在快照中
	if r.RaftLog.InSnapshot(previ) {
		return false
	}
	// 前缀未被压缩
	prevt, _ := r.RaftLog.Term(previ)
	mm := pb.Message{MsgType: pb.MessageType_MsgAppend, To: to, From: r.id, Term: r.Term, Index: previ, LogTerm: prevt, Entries: r.RaftLog.getEntriesPointers(nexti, r.RaftLog.LastIndex()+1), Commit: r.RaftLog.committed}
	r.msgs = append(r.msgs, mm)
	dbg.Infof(dbg.DLogs, "S%x 向 S%x 发送了增加日志 %v 在时代 %d", r.id, to, mm.Entries, r.Term)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// 需要求最小值的原因是：小弟提交的日志需要满足三个条件
	// 1. 来自于其现有任期的领导（小于matchIndex）
	// 2. 领导先得提交该日志
	// 3. 它自己得有这个日志（小于LastIndex）
	commit := min(r.RaftLog.committed, r.Ask(to).Match)
	mm := pb.Message{MsgType: pb.MessageType_MsgHeartbeat, To: to, From: r.id, Term: r.Term, Commit: commit}
	r.msgs = append(r.msgs, mm)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.DutyPrepareCampaign.TickAndWork(r)
	case StateCandidate:
		r.DutyRestartTimeoutElection.TickAndWork(r)
		r.DutyKeepCampaign.TickAndWork(r)
	case StateLeader:
		r.DutyBeat.TickAndWork(r)
		r.DutyStopTimeoutTransfer.TickAndWork(r)
		r.DutyKeepSyncLog.TickAndWork(r)
	case StatePreCandidate:
		r.DutyDropTimeoutPreElection.TickAndWork(r)
		r.DutyKeepPreCampaign.TickAndWork(r)
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.DutyPrepareCampaign = new(DutyPrepareCampaign)
	r.DutyPrepareCampaign.Reset(r.heartbeatTick)
	dbg.Infof(dbg.DState, "S%x 成为小弟 在时代 %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term = r.Term + 1
	r.votes = make(map[uint64]bool)
	r.DutyRestartTimeoutElection = new(DutyRestartTimeoutElection)
	r.DutyKeepCampaign = new(DutyKeepCampaign)
	r.DutyRestartTimeoutElection.Reset(r.electionTick)
	r.DutyKeepCampaign.Reset(r.heartbeatTick, r)
	dbg.Infof(dbg.DState, "S%x 成为勇士 在时代 %d", r.id, r.Term)
	r.poll(r.id, true)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// 1. initiate
	r.State = StateLeader
	r.DutyBeat = new(DutyBeat)
	r.DutyStopTimeoutTransfer = new(DutyStopTimeoutTransfer)
	r.DutyKeepSyncLog = new(DutyKeepSyncLog)
	r.DutyBeat.Reset(r.heartbeatTick)
	r.DutyStopTimeoutTransfer.Reset(r.electionTick)
	r.DutyKeepSyncLog.Reset(r.heartbeatTick, r)
	for id, _ := range r.Prs {
		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
		} else {
			r.Prs[id].Match = 0
			r.Prs[id].Next = r.RaftLog.LastIndex() + 1
		}
	}
	if r.PState == PState_PREPARE || r.PState == PState_JOINT {
		for id, _ := range r.NewPrs {
			if id == r.id {
				r.NewPrs[id].Match = r.RaftLog.LastIndex()
			} else {
				r.NewPrs[id].Match = 0
				r.NewPrs[id].Next = r.RaftLog.LastIndex() + 1
			}
		}
	}

	// 2. add a blank entry in leader's term
	blank := pb.Entry{EntryType: pb.EntryType_EntryNormal, Data: nil}
	r.RaftLog.AppendAndTag(r.Term, blank)
	r.Ask(r.id).Match = r.RaftLog.LastIndex()
	r.tryPushCommit()
	dbg.Infof(dbg.DState, "S%x 成为大哥 在时代 %d", r.id, r.Term)
	r.bcastAppend()
}

func (r *Raft) becomePreCandidate() {
	r.State = StatePreCandidate
	r.votes = make(map[uint64]bool)
	r.DutyDropTimeoutPreElection = new(DutyDropTimeoutPreElection)
	r.DutyKeepPreCampaign = new(DutyKeepPreCampaign)
	r.DutyDropTimeoutPreElection.Reset(r.electionTick)
	r.DutyKeepPreCampaign.Reset(r.heartbeatTick, r)
	dbg.Infof(dbg.DState, "S%x 成为预选勇士 在时代 %d", r.id, r.Term)
	r.prePoll(r.id, true)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch {
	case m.Term == 0:
	// local message
	case m.Term < r.Term:
		if m.MsgType == pb.MessageType_MsgRequestVote {
			dbg.Infof(dbg.DVote, "S%x 拒绝为 S%x 投票在时代 %d", r.id, m.From, r.Term)
			mm := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: true}
			r.msgs = append(r.msgs, mm)
		} else if m.MsgType == pb.MessageType_MsgPreVote {
			dbg.Infof(dbg.DVote, "S%x 拒绝为 S%x 预投票在时代 %d", r.id, m.From, r.Term)
			mm := pb.Message{MsgType: pb.MessageType_MsgPreVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: true}
			r.msgs = append(r.msgs, mm)
		}
		return nil
	case m.Term > r.Term:
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	}
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	case StatePreCandidate:
		return r.stepPreCandidate(m)
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Reject == true {
		// 实质上是一个心跳
		r.CommitTo(m.Commit)
		return
	}
	var logX bool = false
	switch r.RaftLog.prevIndexTermQualified(m.Index, m.LogTerm) {
	case ConflictOrNotExist:
		// 前缀矛盾 or 不存在前缀
		dbg.Infof(dbg.DLogs, "S%x 拒绝接受 S%x 的增长日志 %v 在时代 %d", r.id, m.From, m.Entries, r.Term)
		mm := pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term, Reject: true, Index: m.Index, LogTerm: m.LogTerm}
		r.msgs = append(r.msgs, mm)
		return
	case PrevMatched:
		dbg.Infof(dbg.DLogs, "S%x 接受 S%x 的增长日志 %v 在时代 %d", r.id, m.From, m.Entries, r.Term)
		if len(m.Entries) > 0 {
			logX = true
			// 防一手测试程序
			laste := m.Entries[len(m.Entries)-1]
			if r.RaftLog.inLogIndeed(laste.Index, laste.Term) {
				// 即该消息包含的信息已经被完全涵盖，没有必要更新
				break
			}
			r.RaftLog.AnotherTailWithPointers(m.Index, m.Entries)
		}
	case OutdatedMatched:
		dbg.Infof(dbg.DLogs, "S%x 忽视了已匹配的来自 S%x 的增长日志 %v 在时代 %d", r.id, m.From, m.Entries, r.Term)
	}

	mm := pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term, Reject: false, Index: r.RaftLog.LastIndex(), LogTerm: r.RaftLog.LastTerm()}
	r.msgs = append(r.msgs, mm)
	// 等到结束再提交，以防修改已提交日志
	// 另一分支是为了防止在前缀匹配的情况下，消息条目丢失导致的提交信息无效，从而按照过时匹配信息进行处理
	if logX {
		r.CommitTo(m.Commit)
	} else {
		r.CommitTo(min(m.Commit, m.Index))
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.CommitTo(m.Commit)
	mm := pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, To: m.From, From: r.id, Term: r.Term, Index: r.RaftLog.LastIndex(), LogTerm: r.RaftLog.LastTerm()}
	r.msgs = append(r.msgs, mm)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	mi := m.Snapshot.Metadata.Index
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.RaftLog.entries = make([]pb.Entry, 1)
	r.RaftLog.compacted = mi
	r.RaftLog.applied = mi
	r.CommitTo(mi)
	r.RaftLog.stabled = mi
	mm := pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term, Reject: false, Index: m.Snapshot.Metadata.Index, LogTerm: m.Snapshot.Metadata.Term}
	r.msgs = append(r.msgs, mm)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) softState() *SoftState { return &SoftState{Lead: r.Lead, RaftState: r.State} }

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) share(to uint64, m pb.Message) {
	m.To = to
	r.msgs = append(r.msgs, m)
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.hup()
	case pb.MessageType_MsgPreHup:
		r.preHup()
	case pb.MessageType_MsgPropose:
		if r.Lead == None {
			dbg.Infof(dbg.DPROP, "S%x 缺少大哥在时代 %d", r.id, r.Term)
			return ErrProposalDropped
		} else if r.disableProposalForwarding {
			dbg.Infof(dbg.DPROP, "S%x 不接受提议在时代 %d", r.id, r.Term)
			return ErrProposalDropped
		}
		r.share(r.Lead, m)
	case pb.MessageType_MsgAppend:
		r.DutyPrepareCampaign.Reset(r.heartbeatTick)
		r.Lead = m.From
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.DutyPrepareCampaign.Reset(r.heartbeatTick)
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.DutyPrepareCampaign.Reset(r.heartbeatTick)
		r.Lead = m.From
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead == None {
			return nil
		}
		r.share(r.Lead, m)
	case pb.MessageType_MsgRequestVote:
		canVote := (r.Vote == m.From) ||
			(r.Vote == None && r.Lead == None)
		if canVote && r.RaftLog.agree(m.Index, m.LogTerm) {
			dbg.Infof(dbg.DVote, "S%x 为 S%x 投票在时代 %d", r.id, m.From, r.Term)
			mm := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: false}
			r.msgs = append(r.msgs, mm)
			r.DutyPrepareCampaign.Reset(r.heartbeatTick)
			r.Vote = m.From
		} else {
			dbg.Infof(dbg.DVote, "S%x 拒绝为 S%x 投票在时代 %d", r.id, m.From, r.Term)
			mm := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: true}
			r.msgs = append(r.msgs, mm)
		}
	case pb.MessageType_MsgPreVote:
		if r.RaftLog.agree(m.Index, m.LogTerm) {
			dbg.Infof(dbg.DVote, "S%x 为 S%x 预投票在时代 %d", r.id, m.From, r.Term)
			mm := pb.Message{MsgType: pb.MessageType_MsgPreVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: false}
			r.msgs = append(r.msgs, mm)
			r.DutyPrepareCampaign.Reset(r.heartbeatTick)
			r.Vote = m.From
		} else {
			dbg.Infof(dbg.DVote, "S%x 拒绝为 S%x 预投票在时代 %d", r.id, m.From, r.Term)
			mm := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: true}
			r.msgs = append(r.msgs, mm)
		}
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.hup()
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.poll(m.From, !m.Reject)
		res := r.TallyVotes()
		switch res {
		case Win:
			r.becomeLeader()
		case Lose:
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgRequestVote:
		dbg.Infof(dbg.DVote, "S%x 拒绝为 S%x 投票在时代 %d", r.id, m.From, r.Term)
		mm := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: true}
		r.msgs = append(r.msgs, mm)
	case pb.MessageType_MsgPreVote:
		dbg.Infof(dbg.DVote, "S%x 拒绝为 S%x 预投票在时代 %d", r.id, m.From, r.Term)
		mm := pb.Message{MsgType: pb.MessageType_MsgPreVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: true}
		r.msgs = append(r.msgs, mm)
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartBeat()
	case pb.MessageType_MsgPropose:
		if r.Ask(r.id) == nil {
			return ErrProposalDropped
		}
		if r.leadTransferee != None {
			return ErrProposalDropped
		}
		if len(m.Entries) == 0 {
			return ErrProposalDropped
		}
		r.RaftLog.AppendAndTagWithPointers(r.Term, m.Entries)
		r.Ask(r.id).Match = r.RaftLog.LastIndex()
		r.tryPushCommit()
		r.bcastAppend()
	case pb.MessageType_MsgAppendResponse:
		dbg.Infof(dbg.DLogs, "S%x 收到 S%x 的日志增长回应在时代 %d", r.id, m.From, r.Term)
		if m.Reject && r.RaftLog.inLogIndeed(m.Index, m.LogTerm) && r.Ask(m.From).Next == m.Index+1 {
			// 拒绝增长的都会返回增长时的前缀索引和任期，以防止过时消息干扰判断
			if r.Prs[m.From].Next == 0 {
				dbg.Panic("下一条要发送的信息不应该既不存在又被拒绝")
			}
			r.Prs[m.From].Next--
			r.SyncLog(m.From)
		}
		if !m.Reject && m.Index > r.Ask(m.From).Match {
			// 说明m一定是比原来所有都新的消息
			r.Prs[m.From].Match = m.Index
			if success := r.tryPushCommit(); dbg.StrongSyncCommit && success {
				r.bcastAppend()
			}
		}
		if !m.Reject {
			r.Ask(m.From).Next = r.RaftLog.LastIndex() + 1
		}
	case pb.MessageType_MsgHeartbeatResponse:
		if m.Index > r.Ask(m.From).Match {
			r.Ask(m.From).Match = m.Index
			if success := r.tryPushCommit(); dbg.StrongSyncCommit && success {
				r.bcastAppend()
			}
		}
		if m.Index < r.RaftLog.LastIndex() {
			r.SyncLog(m.From)
		}
	case pb.MessageType_MsgRequestVote:
		dbg.Infof(dbg.DVote, "S%x 拒绝为 S%x 投票在时代 %d", r.id, m.From, r.Term)
		mm := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: true}
		r.msgs = append(r.msgs, mm)
	case pb.MessageType_MsgPreVote:
		dbg.Infof(dbg.DVote, "S%x 拒绝为 S%x 预投票在时代 %d", r.id, m.From, r.Term)
		mm := pb.Message{MsgType: pb.MessageType_MsgPreVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: true}
		r.msgs = append(r.msgs, mm)
	}
	return nil
}

func (r *Raft) stepPreCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.hup()
	case pb.MessageType_MsgGiveUp:
		dbg.Infof(dbg.DElec, "S%x 民意调查失败放弃竞选", r.id)
		r.becomeFollower(r.Term, None)
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case pb.MessageType_MsgPreVoteResponse:
		r.poll(m.From, !m.Reject)
		res := r.TallyVotes()
		switch res {
		case Win:
			r.becomeLeader()
		case Lose:
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgRequestVote:
		dbg.Infof(dbg.DVote, "S%x 拒绝为 S%x 投票在时代 %d", r.id, m.From, r.Term)
		mm := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: true}
		r.msgs = append(r.msgs, mm)
	case pb.MessageType_MsgPreVote:
		dbg.Infof(dbg.DVote, "S%x 拒绝为 S%x 预投票在时代 %d", r.id, m.From, r.Term)
		mm := pb.Message{MsgType: pb.MessageType_MsgPreVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: true}
		r.msgs = append(r.msgs, mm)
	}
	return nil
}
