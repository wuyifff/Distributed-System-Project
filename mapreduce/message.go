package mapreduce

import "time"

// MsgType
const (
	LEADER_ID       = 0
	MAP_REQUEST     = 100
	REDUCE_REQUEST  = 101
	QUERY_REQUEST   = 102
	JOIN_REQUEST    = 103
	MAP_ACK         = 200
	REDUCE_ACK      = 201
	QUERY_ACK       = 202
	MAP_START       = 300
	REDUCE_START    = 301
	QUERY_START     = 302
	MAP_RESULT      = 400
	REDUCE_RESULT   = 401
	QUERY_RESULT    = 402
	MAP_FINISH      = 500
	REDUCE_FINISH   = 501
	QUERY_FINISH    = 502
	JOIN_FINISH     = 503
	QUERY_ERROR     = 602
	NO_LIVING_NODES = -1
)

const (
	DELETE_INPUT = 1
	NO_DELETE    = 2
)

type MapReduceMsg struct {
	MsgType      int           `json:"msgtype"`
	Executable   string        `json:"executable"`
	NumNodes     int           `json:"numnodes"`
	Prefix       string        `json:"prefix"`
	SrcDir       string        `json:"srcdir"`
	KeyList      []string      `json:"keyList"`
	DestFileName string        `json:"destFileName"`
	Command      int           `json:"command"`
	ID           int64         `json:"id"`
	FileName     string        `json:"filename"`
	JobID        int           `json:"jobid"`
	Dataset      []string      `json:"dataset"`
	Query        string        `json:"query"`
	Fileds       []string      `json:"fileds"`
	Time         time.Time     `json:"starttime"`
	Duration     time.Duration `json:"duration"`
	Extra        string        `json:"extra"`
}

type JobInfo struct {
	jobID      int
	initiator  int64
	originNode int64
	fileName   string
}
