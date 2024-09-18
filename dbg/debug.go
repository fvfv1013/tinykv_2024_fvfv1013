package dbg

import (
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"log"
	"os"
	"strconv"
	"time"
)

const Chinese = true

func NotNil(v interface{}) {
	if v == nil {
		panic(v)
	}
}

func Nil(v interface{}) {
	if v != nil {
		panic(v)
	}
}

func Panic(v interface{}) {
	panic(v)
}

const debug int = 0
const chinese bool = true

// 更强地同步提交索引
const StrongSyncCommit bool = true

// 启用PreCandidate
const EnablePreCandidate = false

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type LogTopic string

const (
	DElec  LogTopic = "ELEC"
	DBeat  LogTopic = "BEAT"
	DLogs  LogTopic = "LOG1"
	DComt  LogTopic = "LOG2"
	DVote  LogTopic = "VOTE"
	DSnap  LogTopic = "SNAP"
	DLead  LogTopic = "LEAD"
	DTime  LogTopic = "TIME"
	DState LogTopic = "LEAD"
	DWarn  LogTopic = "WARN"
	DPROP  LogTopic = "PROP"
)

var MessageType_LogTopic = map[int32]LogTopic{
	0:  DElec, //"MsgHup",
	1:  DBeat, //"MsgBeat",
	2:  DLogs, //"MsgPropose",
	3:  DLogs, //"MsgAppend",
	4:  DLogs, //"MsgAppendResponse",
	5:  DVote, //"MsgRequestVote",
	6:  DVote, //"MsgRequestVoteResponse",
	7:  DSnap, //"MsgSnapshot",
	8:  DBeat, //"MsgHeartbeat",
	9:  DBeat, //"MsgHeartbeatResponse",
	11: DLead, //"MsgTransferLeader",
	12: DTime, //"MsgTimeoutNow",
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func MsgInfof(msg pb.Message) {
	if debug >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(MessageType_LogTopic[int32(msg.MsgType)]))
		var format string
		switch msg.MsgType {
		// 'MessageType_MsgHup' is a local message used for election. If an election timeout happened,
		// the node should pass 'MessageType_MsgHup' to its Step method and start a new election.
		case pb.MessageType_MsgHup:
			if chinese {
				format = fmt.Sprintf("S%d S%d开始了新一届选举", msg.To, msg.To)
			} else {
				format = fmt.Sprintf("S%d Start a new election", msg.To)
			}
		// 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
		// of the 'MessageType_MsgHeartbeat' type to its followers.
		case pb.MessageType_MsgBeat:
			if chinese {
				format = fmt.Sprintf("S%d <- S%d S%d被S%d心跳", msg.To, msg.From, msg.To, msg.From)
			} else {
				format = fmt.Sprintf("S%d <- S%d Receive leadership declaration(heartbeat)", msg.To, msg.From)
			}
		// 'MessageType_MsgPropose' is a local message that proposes to append data to the leader's log entries.
		case pb.MessageType_MsgPropose:
			if chinese {
				format = fmt.Sprintf("S%d S%d想要增加数据Data", msg.To, msg.To)
			} else {
				format = fmt.Sprintf("S%d <- S%d Receive log propose", msg.To, msg.From)
			}

		// 'MessageType_MsgAppend' contains log entries to replicate.
		case pb.MessageType_MsgAppend:
			if chinese {
				format = fmt.Sprintf("S%d <- S%d S%d被S%d扩充日志", msg.To, msg.From, msg.To, msg.From)
			} else {
				format = fmt.Sprintf("S%d <- S%d Receive log append", msg.To, msg.From)
			}
		// 'MessageType_MsgAppendResponse' is response to log replication request('MessageType_MsgAppend').
		case pb.MessageType_MsgAppendResponse:
			if chinese {
				format = fmt.Sprintf("S%d <- S%d S%d回应了日志扩充", msg.To, msg.From, msg.From)
			} else {
				format = fmt.Sprintf("S%d <- S%d Receive response to append", msg.To, msg.From)
			}

			// 'MessageType_MsgRequestVote' requests votes for election.
		case pb.MessageType_MsgRequestVote:
			if chinese {
				format = fmt.Sprintf("S%d <- S%d S%d被S%d拉票", msg.To, msg.From, msg.To, msg.From)
			} else {
				format = fmt.Sprintf("S%d <- S%d Is asked for vote", msg.To, msg.From)
			}
		// 'MessageType_MsgRequestVoteResponse' contains responses from voting request.
		case pb.MessageType_MsgRequestVoteResponse:
			if chinese {
				format = fmt.Sprintf("S%d <- S%d 得到S%d选票", msg.To, msg.From, msg.From)
			} else {
				format = fmt.Sprintf("S%d <- S%d Got vote", msg.To, msg.From)
			}
		// 'MessageType_MsgSnapshot' requests to install a snapshot message.
		case pb.MessageType_MsgSnapshot:
			if chinese {
				format = fmt.Sprintf("S%d <- S%d S%d被S%d要求下载快照", msg.To, msg.From, msg.To, msg.From)
			} else {
				format = fmt.Sprintf("S%d <- S%d ", msg.To, msg.From)
			}

		// 'MessageType_MsgHeartbeat' sends heartbeat from leader to its followers.
		case pb.MessageType_MsgHeartbeat:
			if chinese {
				format = fmt.Sprintf("S%d <- S%d S%d被S%d心跳", msg.To, msg.From, msg.To, msg.From)
			} else {
				format = fmt.Sprintf("S%d <- S%d receive heartbeat", msg.To, msg.From)
			}
		// 'MessageType_MsgHeartbeatResponse' is a response to 'MessageType_MsgHeartbeat'.
		case pb.MessageType_MsgHeartbeatResponse:
			if chinese {
				format = fmt.Sprintf("S%d <- S%d S%d回应S%d心跳", msg.To, msg.From, msg.From, msg.To)
			} else {

			}
		// 'MessageType_MsgTransferLeader' requests the leader to transfer its leadership.
		case pb.MessageType_MsgTransferLeader:
			if chinese {
				format = fmt.Sprintf("S%d 成为大哥", msg.To)
			} else {
				format = fmt.Sprintf("S%d become leader", msg.To)

			}

		// 'MessageType_MsgTimeoutNow' send from the leader to the leadership transfer target, to let
		// the transfer target timeout immediately and start a new election.
		case pb.MessageType_MsgTimeoutNow:
			if chinese {
				format = fmt.Sprintf("S%d 超时", msg.To)
			} else {
				format = fmt.Sprintf("S%d Timeout", msg.To)
			}
		}
		format = prefix + format
		fmt.Println(format)
	}
}

func Infof(topic LogTopic, format string, args ...interface{}) {
	if debug >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, topic)
		//prefix := fmt.Sprintf("%v ", topic)
		ctx := fmt.Sprintf(format, args...)
		fmt.Printf("%s%s\n", prefix, ctx)
	}
}

// func Prettier_Debug(topic LogTopic, format string, a ...interface{}) {
// 	if debug >= 1 {
// 		time := time.Since(debugStart).Microseconds()
// 		time /= 100
// 		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
// 		format = prefix + format
// 		log.Printf(format, a...)
// 	}
// }
