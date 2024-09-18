package raft

import (
	"github.com/pingcap-incubator/tinykv/dbg"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) bcastAppend() {
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		r.SyncLog(id)
	}
	if r.PState == PState_PREPARE || r.PState == PState_JOINT {
		for id, _ := range r.NewPrs {
			if id == r.id {
				continue
			}
			r.SyncLog(id)
		}
	}
}

func (r *Raft) sendSnapshot(to uint64) {
	if s, err := r.RaftLog.storage.Snapshot(); err != nil && !IsEmptySnap(&s) {
		mm := pb.Message{MsgType: pb.MessageType_MsgSnapshot, To: to, From: r.id, Term: r.Term, Snapshot: &s}
		r.msgs = append(r.msgs, mm)
		dbg.Infof(dbg.DLogs, "S%x 向 S%x 发送了快照在时代 %d", r.id, to, r.Term)
	}
}

func (r *Raft) tryPushCommit() (success bool) {
	// 寻找转判定，状态空间二分法
	// 对偶数个节点做特别优化
	if r.PState == PState_NORMAL || r.PState == PState_PREPARE {
		var leftBound uint64 = r.RaftLog.committed
		var rightBound uint64 = 0
		var n int = len(r.Prs)
		var ignoreFirstPeer bool = false
		if n%2 == 0 {
			ignoreFirstPeer = true
			n = n - 1
		}
		var matchArray []uint64
		for _, pr := range r.Prs {
			if ignoreFirstPeer {
				ignoreFirstPeer = false
				continue
			}
			matchArray = append(matchArray, pr.Match)
		}
		// 将状态空间右边界设置为match的中位数
		rightBound = findKthNum(matchArray, 0, len(matchArray), (n+1)/2)
		fs := findStep(leftBound, rightBound, r.stabledForMajorityPrs)
		fsTerm, err := r.RaftLog.Term(fs)
		if err != nil {
			dbg.Infof(dbg.DWarn, "S%x 获取台阶时代失败 %s 在时代 %d", r.id, err, r.Term)
			return
		}
		if fsTerm != r.Term {
			dbg.Infof(dbg.DWarn, "S%x 尝试提交不属于本时代的条目 索引为 %d 在时代 %d", r.id, fs, r.Term)
			return
		}
		success = fs > r.RaftLog.committed
		if success {
			dbg.Infof(dbg.DComt, "S%x 推动日志提交到 %d", r.id, min(r.RaftLog.LastIndex(), fs))
		}
		r.CommitTo(fs)
		return success
	} else if r.PState == PState_JOINT {
		var leftBound1, leftBound2 uint64 = r.RaftLog.committed, r.RaftLog.committed
		var rightBound1, rightBound2 uint64 = 0, 0
		var n1, n2 int = len(r.Prs), len(r.NewPrs)
		var ignoreFirstPeer1, ignoreFirstPeer2 bool = false, false
		if n1%2 == 0 {
			ignoreFirstPeer1 = true
			n1 = n1 - 1
		}
		if n2%2 == 0 {
			ignoreFirstPeer2 = true
			n2 = n2 - 1
		}
		var matchArray1, matchArray2 []uint64

		// 确定OLD集群的提交位置
		for _, pr := range r.Prs {
			if ignoreFirstPeer1 {
				ignoreFirstPeer1 = false
				continue
			}
			matchArray1 = append(matchArray1, pr.Match)
		}
		// 将状态空间右边界设置为match的中位数
		rightBound1 = findKthNum(matchArray1, 0, len(matchArray1), (n1+1)/2)
		fs1 := findStep(leftBound1, rightBound1, r.stabledForMajorityPrs)

		// 确定NEW集群的提交位置
		for _, pr := range r.NewPrs {
			if ignoreFirstPeer2 {
				ignoreFirstPeer2 = false
				continue
			}
			matchArray2 = append(matchArray2, pr.Match)
		}
		// 将状态空间右边界设置为match的中位数
		rightBound2 = findKthNum(matchArray2, 0, len(matchArray2), (n2+1)/2)
		fs2 := findStep(leftBound2, rightBound2, r.stabledForMajorityNewPrs)

		// 将fs设置为两集群提交位置的较小者
		fs := min(fs1, fs2)
		fsTerm, err := r.RaftLog.Term(fs)
		if err != nil {
			dbg.Infof(dbg.DWarn, "S%x 获取台阶时代失败 %s 在时代 %d", r.id, err, r.Term)
			return
		}
		if fsTerm != r.Term {
			dbg.Infof(dbg.DWarn, "S%x 尝试提交不属于本时代的条目 索引为 %d 在时代 %d", r.id, fs, r.Term)
			return
		}
		success = fs > r.RaftLog.committed
		if success {
			dbg.Infof(dbg.DComt, "S%x 推动日志提交到 %d", r.id, min(r.RaftLog.LastIndex(), fs))
		}
		r.CommitTo(fs)
		return success
	}
	return
}

func (r *Raft) stabledForMajorityPrs(i uint64) bool {
	n := len(r.Prs)
	var ignoreFirstPeer bool = false
	if n%2 == 0 {
		ignoreFirstPeer = true
		n = n - 1
	}
	var reached int = 0
	for _, pr := range r.Prs {
		if ignoreFirstPeer {
			ignoreFirstPeer = false
			continue
		}
		if pr.Match >= i {
			reached++
		}
	}
	return reached > (n >> 1)
}

func (r *Raft) stabledForMajorityNewPrs(i uint64) bool {
	n := len(r.NewPrs)
	var ignoreFirstPeer bool = false
	if n%2 == 0 {
		ignoreFirstPeer = true
		n = n - 1
	}
	var reached int = 0
	for _, pr := range r.Prs {
		if ignoreFirstPeer {
			ignoreFirstPeer = false
			continue
		}
		if pr.Match >= i {
			reached++
		}
	}
	return reached > (n >> 1)
}

func (r *Raft) StableToAndUpdateMatch(i uint64) {
	if i > r.RaftLog.stabled {
		r.RaftLog.stabled = min(r.RaftLog.LastIndex(), i)
		r.Prs[r.id].Match = r.RaftLog.stabled
	}
}

func (r *Raft) CommitTo(i uint64) {
	if i > r.RaftLog.committed {
		r.RaftLog.committed = min(r.RaftLog.LastIndex(), i)
	}
}

func (r *Raft) sendEmptyAppend(to uint64) {
	// 由于检测中包含对于消息类型的检查，故使用该空增加代替心跳
	// 心跳和一般更新日志消息之间使用reject进行区分
	commit := min(r.RaftLog.committed, r.Ask(to).Match)
	mm := pb.Message{MsgType: pb.MessageType_MsgAppend, To: to, From: r.id, Term: r.Term, Reject: true, Commit: commit}
	r.msgs = append(r.msgs, mm)
}

func (r *Raft) Ask(id uint64) *Progress {
	if r.Prs[id] != nil {
		return r.Prs[id]
	} else {
		return r.NewPrs[id]
	}
}

func (r *Raft) SyncLog(to uint64) {
	if to == r.id {
		return
	}
	if r.Ask(to).Next > r.RaftLog.LastIndex() {
		r.sendEmptyAppend(to)
	} else if !r.sendAppend(to) {
		r.sendSnapshot(to)
	}
}
