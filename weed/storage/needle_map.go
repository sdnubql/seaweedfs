package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/util"
)

//定义了针文件的映射类型
type NeedleMapType int

const (
	//定义常量
	NeedleMapInMemory NeedleMapType = iota
	NeedleMapLevelDb
	NeedleMapBoltDb
)

const (
	//真文件的索引大小
	NeedleIndexSize = 16
)

//定义NeedleMapper接口，实现下面的一坨方法
type NeedleMapper interface {
	Put(key uint64, offset uint32, size uint32) error
	Get(key uint64) (element *NeedleValue, ok bool)
	Delete(key uint64) error
	Close()
	Destroy() error
	ContentSize() uint64
	DeletedSize() uint64
	FileCount() int
	DeletedCount() int
	MaxFileKey() uint64
	IndexFileSize() uint64
	IndexFileContent() ([]byte, error)
	IndexFileName() string
}

//基本针映射结构
type baseNeedleMapper struct {
	indexFile           *os.File   //文件句柄
	indexFileAccessLock sync.Mutex //锁

	mapMetric
}

//索引文件的大小
func (nm *baseNeedleMapper) IndexFileSize() uint64 {
	//读取索引文件的状态
	stat, err := nm.indexFile.Stat()
	if err == nil {
		//返回索引文件的大小
		return uint64(stat.Size())
	}
	return 0
}

//返回索引文件的名称
func (nm *baseNeedleMapper) IndexFileName() string {
	return nm.indexFile.Name()
}

//获取文件的位置
func idxFileEntry(bytes []byte) (key uint64, offset uint32, size uint32) {
	//前8个字节key
	key = util.BytesToUint64(bytes[:8])
	//4个字节为起点
	offset = util.BytesToUint32(bytes[8:12])
	//4个字节为大小
	size = util.BytesToUint32(bytes[12:16])
	return
}

//追加索引文件
func (nm *baseNeedleMapper) appendToIndexFile(key uint64, offset uint32, size uint32) error {
	//申请内存
	bytes := make([]byte, 16)
	//封装key到bytes容器中
	util.Uint64toBytes(bytes[0:8], key)
	//封装起点到bytes容器中
	util.Uint32toBytes(bytes[8:12], offset)
	//封装大小到bytes容器中
	util.Uint32toBytes(bytes[12:16], size)
	//加锁
	nm.indexFileAccessLock.Lock()
	defer nm.indexFileAccessLock.Unlock()
	//定位到文件末尾
	if _, err := nm.indexFile.Seek(0, 2); err != nil {
		return fmt.Errorf("cannot seek end of indexfile %s: %v",
			nm.indexFile.Name(), err)
	}
	//写索引信息到索引文件中
	_, err := nm.indexFile.Write(bytes)
	return err
}

//获取索引文件的内容
func (nm *baseNeedleMapper) IndexFileContent() ([]byte, error) {
	//加锁
	nm.indexFileAccessLock.Lock()
	defer nm.indexFileAccessLock.Unlock()
	//根据名称读取内容
	return ioutil.ReadFile(nm.indexFile.Name())
}

type mapMetric struct {
	indexFile *os.File

	DeletionCounter     int    `json:"DeletionCounter"`
	FileCounter         int    `json:"FileCounter"`
	DeletionByteCounter uint64 `json:"DeletionByteCounter"`
	FileByteCounter     uint64 `json:"FileByteCounter"`
	MaximumFileKey      uint64 `json:"MaxFileKey"`
}

//变更删除的数量,增加删除的次数，和删除的字节的大小
func (mm *mapMetric) logDelete(deletedByteCount uint32) {
	//增加删除字节的大小
	mm.DeletionByteCounter = mm.DeletionByteCounter + uint64(deletedByteCount)
	//增加删除的次数
	mm.DeletionCounter++
}

func (mm *mapMetric) logPut(key uint64, oldSize uint32, newSize uint32) {
	if key > mm.MaximumFileKey {
		mm.MaximumFileKey = key
	}
	mm.FileCounter++
	mm.FileByteCounter = mm.FileByteCounter + uint64(newSize)
	if oldSize > 0 {
		mm.DeletionCounter++
		mm.DeletionByteCounter = mm.DeletionByteCounter + uint64(oldSize)
	}
}

//返回内容大小
func (mm mapMetric) ContentSize() uint64 {
	return mm.FileByteCounter
}

//删除删除的文件大小
func (mm mapMetric) DeletedSize() uint64 {
	return mm.DeletionByteCounter
}

//返回文件查询的次数
func (mm mapMetric) FileCount() int {
	return mm.FileCounter
}

//返回删除的次数
func (mm mapMetric) DeletedCount() int {
	return mm.DeletionCounter
}

//返回最大的文件key
func (mm mapMetric) MaxFileKey() uint64 {
	return mm.MaximumFileKey
}
