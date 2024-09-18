package raft

import (
	"github.com/pingcap-incubator/tinykv/dbg"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// 1. Follower's

type DutyPrepareCampaign struct {
	HeartbeatElapsed           int
	HeartbeatTimeoutRandomized int
}

func (d *DutyPrepareCampaign) Reset(heartbeatTick int) {
	d.HeartbeatElapsed = 0
	d.HeartbeatTimeoutRandomized = 10*heartbeatTick + rand.Int()%(10*heartbeatTick)
}

func (d *DutyPrepareCampaign) TickAndWork(self *Raft) {
	d.HeartbeatElapsed++
	if d.HeartbeatElapsed >= d.HeartbeatTimeoutRandomized {
		d.HeartbeatElapsed = 0
		if dbg.EnablePreCandidate {
			self.Step(pb.Message{MsgType: pb.MessageType_MsgPreHup, To: self.id})
		} else {
			self.Step(pb.Message{MsgType: pb.MessageType_MsgHup, To: self.id})
		}
	}
}

// 2. Candidate's

type DutyRestartTimeoutElection struct {
	ElectionElapsed           int
	ElectionTimeoutRandomized int
}

func (d *DutyRestartTimeoutElection) Reset(electionTick int) {
	d.ElectionElapsed = 0
	d.ElectionTimeoutRandomized = electionTick + rand.Int()%electionTick
}

func (d *DutyRestartTimeoutElection) TickAndWork(self *Raft) {
	d.ElectionElapsed++
	if d.ElectionElapsed >= d.ElectionTimeoutRandomized {
		d.ElectionElapsed = 0
		self.Step(pb.Message{MsgType: pb.MessageType_MsgHup, To: self.id})
	}
}

type DutyKeepCampaign struct {
	public             map[uint64]bool
	requestVoteElapsed int
	requestVoteTimeout int
}

func (d *DutyKeepCampaign) Reset(heartbeatTick int, self *Raft) {
	d.requestVoteTimeout = 3 * heartbeatTick
	d.requestVoteElapsed = 0
	d.public = make(map[uint64]bool)
	for id, _ := range self.Prs {
		if id == self.id {
			continue
		}
		d.public[id] = true
	}
	if self.PState == PState_JOINT {
		for id, _ := range self.NewPrs {
			if id == self.id {
				continue
			}
			d.public[id] = true
		}
	}
}

func (d *DutyKeepCampaign) TickAndWork(self *Raft) {
	if d.requestVoteElapsed >= d.requestVoteTimeout {
		d.requestVoteElapsed = 0
		for id, _ := range d.public {
			if self.votes[id] == false {
				self.sendRequestVote(id)
			}
		}
	}
}

// 3. Leader's

type DutyBeat struct {
	HeartbeatElapsed int
	HeartbeatTimeout int
}

func (d *DutyBeat) Reset(heartbeatTick int) {
	d.HeartbeatElapsed = 0
	d.HeartbeatTimeout = heartbeatTick
}

func (d *DutyBeat) TickAndWork(self *Raft) {
	d.HeartbeatElapsed++
	if d.HeartbeatElapsed >= d.HeartbeatTimeout {
		d.HeartbeatElapsed = 0
		self.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, To: self.id})
	}
}

type DutyStopTimeoutTransfer struct {
	TransferElapsed int
	TransferTimeout int
}

func (d *DutyStopTimeoutTransfer) Reset(electionTick int) {
	d.TransferElapsed = 0
	d.TransferTimeout = electionTick
}

func (d *DutyStopTimeoutTransfer) TickAndWork(self *Raft) {
	d.TransferElapsed++
	if d.TransferElapsed >= d.TransferTimeout {
		d.TransferElapsed = 0
		self.abortLeaderTransfer()
	}
}

type DutyKeepSyncLog struct {
	sendAppendElapsed map[uint64]int
	sendAppendTimeout int
}

func (d *DutyKeepSyncLog) Reset(heartbeatTick int, self *Raft) {
	d.sendAppendTimeout = 3 * heartbeatTick
	d.sendAppendElapsed = make(map[uint64]int)
	for id, _ := range self.Prs {
		if id == self.id {
			continue
		}
		d.sendAppendElapsed[id] = 0
	}
	if self.PState == PState_PREPARE || self.PState == PState_JOINT {
		for id, _ := range self.NewPrs {
			if id == self.id {
				continue
			}
			d.sendAppendElapsed[id] = 0
		}
	}
}

func (d *DutyKeepSyncLog) TickAndWork(self *Raft) {
	for id, _ := range d.sendAppendElapsed {
		d.sendAppendElapsed[id]++
		if d.sendAppendElapsed[id] >= d.sendAppendTimeout {
			d.sendAppendElapsed[id] = 0
			self.SyncLog(id)
		}
	}
}

// 4. PreCandidate's

type DutyDropTimeoutPreElection struct {
	PreElectionElapsed           int
	PreElectionTimeoutRandomized int
}

func (d *DutyDropTimeoutPreElection) Reset(electionTick int) {
	d.PreElectionElapsed = 0
	d.PreElectionTimeoutRandomized = electionTick + rand.Int()%electionTick
}

func (d *DutyDropTimeoutPreElection) TickAndWork(self *Raft) {
	d.PreElectionElapsed++
	if d.PreElectionElapsed >= d.PreElectionTimeoutRandomized {
		d.PreElectionElapsed = 0
		self.Step(pb.Message{MsgType: pb.MessageType_MsgGiveUp, To: self.id})
	}
}

type DutyKeepPreCampaign struct {
	public             map[uint64]bool
	requestVoteElapsed int
	requestVoteTimeout int
}

func (d *DutyKeepPreCampaign) Reset(heartbeatTick int, self *Raft) {
	d.requestVoteTimeout = 3 * heartbeatTick
	d.requestVoteElapsed = 0
	d.public = make(map[uint64]bool)
	for id, _ := range self.Prs {
		if id == self.id {
			continue
		}
		d.public[id] = true
	}
	if self.PState == PState_JOINT {
		for id, _ := range self.NewPrs {
			if id == self.id {
				continue
			}
			d.public[id] = true
		}
	}
}

func (d *DutyKeepPreCampaign) TickAndWork(self *Raft) {
	if d.requestVoteElapsed >= d.requestVoteTimeout {
		d.requestVoteElapsed = 0
		for id, _ := range d.public {
			if self.votes[id] == false {
				self.sendPreVote(id)
			}
		}
	}
}
