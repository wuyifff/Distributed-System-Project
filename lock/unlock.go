package lock

import (
	"encoding/json"
	"net"
)

func ReadUNLock(conn net.Conn, myID int64) {
	request := LockRequest{
		MsgType:     READ_UNLOCK,
		RequestorID: myID,
	}
	marshal, _ := json.Marshal(request)
	_, _ = conn.Write(marshal)
	conn.Close()
}

func WriteUNLock(conn net.Conn, myID int64) {
	request := LockRequest{
		MsgType:     WRITE_UNLOCK,
		RequestorID: myID,
	}
	marshal, _ := json.Marshal(request)
	_, _ = conn.Write(marshal)
	conn.Close()
}
