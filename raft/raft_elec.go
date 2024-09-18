package raft

import (
	"github.com/pingcap-incubator/tinykv/dbg"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

type VoteResult uint8

const (
	Lose VoteResult = iota
	Uncertain
	Win
)

func (r *Raft) TallyVotes() VoteResult {
	// 进行计票工作
	// 对偶数个节点进行特别优化
	switch r.PState {
	case PState_NORMAL, PState_PREPARE:
		var granted, rejected int
		n := len(r.Prs)
		var ignoreFirstPeer bool
		if n%2 == 0 {
			ignoreFirstPeer = true
			n = n - 1
		}
		for _, agr := range r.votes {
			if ignoreFirstPeer {
				ignoreFirstPeer = false
				continue
			}
			if agr {
				granted++
			} else {
				rejected++
			}
			if granted > n/2 {
				return Win
			}
			if rejected > n/2 {
				return Lose
			}
		}
	case PState_JOINT:
		var granted1, rejected1 int
		var granted2, rejected2 int
		n1, n2 := len(r.Prs), len(r.NewPrs)
		var ignoreFirstPeer1, ignoreFirstPeer2 bool
		var win1, win2 bool
		if n1%2 == 0 {
			ignoreFirstPeer1 = true
			n1 = n1 - 1
		}
		if n2%2 == 0 {
			ignoreFirstPeer2 = true
			n2 = n2 - 1
		}
		for id, agr := range r.votes {
			if r.Prs[id] != nil {
				if ignoreFirstPeer1 {
					ignoreFirstPeer1 = false
					continue
				}
				if agr {
					granted1++
				} else {
					rejected1++
				}
				if granted1 > n1/2 {
					win1 = true
				}
				if rejected1 > n1/2 {
					return Lose
				}
			} else if r.NewPrs[id] != nil {
				if ignoreFirstPeer2 {
					ignoreFirstPeer2 = false
					continue
				}
				if agr {
					granted2++
				} else {
					rejected2++
				}
				if granted2 > n2/2 {
					win2 = true
				}
				if rejected2 > n2/2 {
					return Lose
				}
			}
			if win1 && win2 {
				return Win
			}
		}
	}
	return Uncertain
}

func (r *Raft) bcastRequestVote() {
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendRequestVote(id)
	}
	if r.PState == PState_JOINT {
		for id, _ := range r.NewPrs {
			if id == r.id {
				continue
			}
			r.sendRequestVote(id)
		}
	}
}

func (r *Raft) inRegion() bool {
	if r.PState == PState_NORMAL || r.PState == PState_PREPARE {
		return r.Prs[r.id] != nil
	} else {
		return r.Prs[r.id] != nil || r.NewPrs[r.id] != nil
	}
}

func (r *Raft) promotable() bool {
	return r.inRegion() && r.RaftLog.pendingSnapshot == nil
}

func (r *Raft) hup() {
	if !r.promotable() {
		dbg.Infof(dbg.DWarn, "S%x 不是可竞选的，在时代 %d", r.id, r.Term)
		return
	}
	e := r.RaftLog.getEntries(r.RaftLog.applied+1, r.RaftLog.committed+1)
	if n := numOfPendingConf(e); n != 0 {
		dbg.Infof(dbg.DWarn, "S%x 不能竞选，还有未完成的配置更改在时代 %d", r.id, r.Term)
		return
	}
	dbg.Infof(dbg.DElec, "S%x 开始了新一轮选举", r.id)
	r.campaign()
}

func (r *Raft) campaign() {
	if !r.promotable() {
		// better safe than sorry.
		dbg.Infof(dbg.DWarn, "S%x 不是可竞选的在时代 %d", r.id, r.Term)
		return
	}
	r.becomeCandidate()
	if res := r.TallyVotes(); res == Win {
		r.becomeLeader()
		return
	}
	r.bcastRequestVote()
}

func (r *Raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

func (r *Raft) bcastHeartBeat() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
	if r.PState == PState_PREPARE || r.PState == PState_JOINT {
		for peer := range r.NewPrs {
			if peer == r.id {
				continue
			}
			r.sendHeartbeat(peer)
		}
	}
}

func (r *Raft) poll(id uint64, granted bool) {
	if granted {
		dbg.Infof(dbg.DVote, "S%x 收到 S%x 的支持票在时代 %d", r.id, id, r.Term)
	} else {
		dbg.Infof(dbg.DVote, "S%x 收到 S%x 的反对票在时代 %d", r.id, id, r.Term)
	}
	r.votes[id] = granted
}

func (r *Raft) sendRequestVote(to uint64) {
	if !r.voted[to] {
		mm := pb.Message{MsgType: pb.MessageType_MsgRequestVote, To: to, From: r.id, Term: r.Term, Index: r.RaftLog.LastIndex(), LogTerm: r.RaftLog.LastTerm()}
		r.msgs = append(r.msgs, mm)
	}
}

func (r *Raft) sendPreVote(to uint64) {
	if !r.voted[to] {
		mm := pb.Message{MsgType: pb.MessageType_MsgPreVote, To: to, From: r.id, Term: r.Term, Index: r.RaftLog.LastIndex(), LogTerm: r.RaftLog.LastTerm()}
		r.msgs = append(r.msgs, mm)
	}
}

func (r *Raft) prePoll(id uint64, granted bool) {
	if granted {
		dbg.Infof(dbg.DVote, "S%x 收到 S%x 的预选支持票在时代 %d", r.id, id, r.Term)
	} else {
		dbg.Infof(dbg.DVote, "S%x 收到 S%x 的预选反对票在时代 %d", r.id, id, r.Term)
	}
	r.votes[id] = granted
}

func (r *Raft) bcastPreVote() {
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendPreVote(id)
	}
	if r.PState == PState_JOINT {
		for id, _ := range r.NewPrs {
			if id == r.id {
				continue
			}
			r.sendPreVote(id)
		}
	}
}

func (r *Raft) preHup() {
	if !r.promotable() {
		dbg.Infof(dbg.DWarn, "S%x 不是可竞选的，在时代 %d", r.id, r.Term)
		return
	}
	e := r.RaftLog.getEntries(r.RaftLog.applied+1, r.RaftLog.committed+1)
	if n := numOfPendingConf(e); n != 0 {
		dbg.Infof(dbg.DWarn, "S%x 不能竞选，还有未完成的配置更改在时代 %d", r.id, r.Term)
		return
	}
	dbg.Infof(dbg.DElec, "S%x 开始了新一轮选举", r.id)
	r.preCampaign()
}

func (r *Raft) preCampaign() {
	if !r.promotable() {
		// better safe than sorry.
		dbg.Infof(dbg.DWarn, "S%x 不是可竞选的在时代 %d", r.id, r.Term)
		return
	}
	r.becomePreCandidate()
	if res := r.TallyVotes(); res == Win {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, To: r.id})
		return
	}
	r.bcastRequestVote()
}
