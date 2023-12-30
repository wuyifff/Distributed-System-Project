package gossip

import (
	"strings"
	"time"
)

// Member represents one single record
type Member struct {
	ID        int64     `json:"id"`
	Addr      string    `json:"addr"`      // ip address and port
	Heartbeat int64     `json:"heartbeat"` // heartbeat
	LocalTime time.Time `json:"localtime"` // local time
	Status    string    `json:"status"`    // status like "ALIVE" or "FAILED" or "Suspected"
}

type MembershipMap map[int64]Member

// NewMember create a new Member
func NewMember(id int64, addr string, heartbeat int64, status string) Member {
	return Member{
		ID:        id,
		Addr:      addr,
		Heartbeat: heartbeat,
		LocalTime: time.Now(),
		Status:    status,
	}
}

func (m MembershipMap) GetHostName(id int64) string {
	addr := m[id].Addr
	hostName := strings.Split(addr, ":")[0]
	return hostName
}
