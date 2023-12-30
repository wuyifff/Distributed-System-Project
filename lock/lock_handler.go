package lock

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
)

func HandleWriteLock(conn net.Conn, lock *Lock, mu *sync.Mutex, remoteAddr string) {
	if lock.WriteSemaphore == 1 && lock.ReadSemaphore == 2 {
		mu.Lock()
		lock.WriteSemaphore -= 1
		response := LockResponse{
			MsgType: OK,
		}
		marshal, _ := json.Marshal(response)
		_, _ = conn.Write(marshal)
		mu.Unlock()
		// fmt.Printf("Give write lock to %s\tCurrent read Semaphore: %d, write Semaphore: %d\n\n", remoteAddr, lock.ReadSemaphore, lock.WriteSemaphore)
	} else {
		fmt.Printf("Access from %s refused.\n", remoteAddr)
		response := LockResponse{
			MsgType: WAIT,
		}
		marshal, _ := json.Marshal(response)
		_, _ = conn.Write(marshal)
	}
	// fmt.Printf("\n")
}

func HandleReadLock(conn net.Conn, lock *Lock, mu *sync.Mutex, remoteAddr string) {
	if lock.ReadSemaphore >= 1 && lock.WriteSemaphore == 1 {
		mu.Lock()
		lock.ReadSemaphore -= 1
		response := LockResponse{
			MsgType: OK,
		}
		marshal, _ := json.Marshal(response)
		_, _ = conn.Write(marshal)
		mu.Unlock()
		// fmt.Printf("Give read lock to %s\tCurrent read Semaphore: %d, write Semaphore: %d\n\n", remoteAddr, lock.ReadSemaphore, lock.WriteSemaphore)
	} else {
		fmt.Printf("Access from %s refused.\n", remoteAddr)
		response := LockResponse{
			MsgType: WAIT,
		}
		marshal, _ := json.Marshal(response)
		_, _ = conn.Write(marshal)
	}
	// fmt.Printf("\n")
}
