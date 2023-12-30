package lock

const (
	READ_LOCK    = 0
	READ_UNLOCK  = 1
	WRITE_LOCK   = 2
	WRITE_UNLOCK = 3
)

const (
	WAIT = 0
	OK   = 1
)

type LockRequest struct {
	MsgType     int   `json:"msg_type"`
	RequestorID int64 `json:"requestor_id"`
}

type LockResponse struct {
	MsgType int `json:"msg_type"`
}

type Queue []interface{}

func (q *Queue) Enqueue(value interface{}) {
	*q = append(*q, value)
}

func (q *Queue) Dequeue() (interface{}, bool) {
	if len(*q) == 0 {
		return nil, false
	}
	value := (*q)[0]
	*q = (*q)[1:]
	return value, true
}

type Lock struct {
	WriteSemaphore int
	ReadSemaphore  int
	Que            Queue
}

func NewLock() *Lock {
	return &Lock{
		WriteSemaphore: 1,
		ReadSemaphore:  2,
		Que:            make(Queue, 0),
	}
}
