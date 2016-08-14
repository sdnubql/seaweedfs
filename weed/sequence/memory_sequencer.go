package sequence

import (
	"sync"
)

// just for testing
//内存计数器结构
type MemorySequencer struct {
	//计数器
	counter uint64
	//锁
	sequenceLock sync.Mutex
}

//内存计数器构造函数
func NewMemorySequencer() (m *MemorySequencer) {
	m = &MemorySequencer{counter: 1}
	return
}

//下一个文件id
func (m *MemorySequencer) NextFileId(count uint64) (uint64, uint64) {
	//加锁
	m.sequenceLock.Lock()
	defer m.sequenceLock.Unlock()
	//取数
	ret := m.counter
	//对计数器+count
	m.counter += uint64(count)
	return ret, count
}

//设置新值
func (m *MemorySequencer) SetMax(seenValue uint64) {
	m.sequenceLock.Lock()
	defer m.sequenceLock.Unlock()
	if m.counter <= seenValue {
		m.counter = seenValue + 1
	}
}

//查看当前值
func (m *MemorySequencer) Peek() uint64 {
	return m.counter
}
