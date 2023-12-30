package consts

// message type consts
const (
	LEADER_MSG_DOWN      = 0
	LEADER_MSG_GET       = 1
	LEADER_MSG_PUT       = 2
	LEADER_MSG_DEL       = 3
	LEADER_MSG_REPLICA   = 4
	LEADER_MSG_MULTIREAD = 100
)

const (
	LEADER_RESP_OK = 10
	LEADER_RESP_NO = 11
)

type LeaderMsg struct {
	MsgType int32   `json:"msg___type"`
	SelfID  int64   `json:"self_id"`
	MachID  int64   `json:"mach_id"`
	FileID  string  `json:"file_id"`
	Replica []int64 `json:"replica"`
}
type LeaderResp struct {
	RespType    int32   `json:"msg___type"`
	ReplicaList []int64 `json:"replica_list"`
}
type ListUpdate struct {
	FileID      string  `json:"file_id"`
	ReplicaList []int64 `json:"replica_list"`
}
type Header struct {
	Len int64 `json:"len"`
}
