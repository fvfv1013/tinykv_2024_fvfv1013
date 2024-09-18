package raft

type PeerConfChangeState int

const (
	PState_NORMAL PeerConfChangeState = iota
	PState_PREPARE
	PState_JOINT
)
