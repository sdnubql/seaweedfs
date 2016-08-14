package sequence

//发号器接口
type Sequencer interface {
	NextFileId(count uint64) (uint64, uint64)
	SetMax(uint64)
	Peek() uint64
}
