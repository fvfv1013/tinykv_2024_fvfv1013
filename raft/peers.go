package raft

type peers struct {
	old         map[uint64]bool
	jointPeriod bool
	new         map[uint64]bool
}
