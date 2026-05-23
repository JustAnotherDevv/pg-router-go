//go:build !linux

package multiproc

import "sync"

func IsWorker() bool            { return false }
func WorkerID() int             { return 0 }
func SpawnWorkers(int, string) *sync.WaitGroup { return nil }
