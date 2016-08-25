package stats

import (
	"runtime"
)

//内存的状态
type MemStatus struct {
	Goroutines int
	All        uint64
	Used       uint64
	Free       uint64
	Self       uint64
	Heap       uint64
	Stack      uint64
}

//构造函数
func MemStat() MemStatus {
	mem := MemStatus{}
	mem.Goroutines = runtime.NumGoroutine()
	memStat := new(runtime.MemStats)
	runtime.ReadMemStats(memStat)
	mem.Self = memStat.Alloc
	mem.Heap = memStat.HeapAlloc
	mem.Stack = memStat.StackInuse

	mem.fillInStatus()
	return mem
}
