package lock

import (
	"net"
	"sync"
)

func HandleReadUNLock(conn net.Conn, lock *Lock, mu *sync.Mutex, remoteAddr string) {
	mu.Lock()
	lock.ReadSemaphore += 1
	mu.Unlock()
	// fmt.Printf("Read lock release. Current read Semaphore: %d, write Semaphore: %d\n", lock.ReadSemaphore, lock.WriteSemaphore)
}

func HandleWriteUNLock(conn net.Conn, lock *Lock, mu *sync.Mutex, remoteAddr string) {
	mu.Lock()
	lock.WriteSemaphore += 1
	mu.Unlock()
	// fmt.Printf("Write lock release. Current read Semaphore: %d, write Semaphore: %d\n", lock.ReadSemaphore, lock.WriteSemaphore)
}
