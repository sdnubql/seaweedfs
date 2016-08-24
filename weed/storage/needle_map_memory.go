package storage

import (
	"io"
	"os"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

//真文件的map
type NeedleMap struct {
	m CompactMap
	//基类
	baseNeedleMapper
}

//NeedleMap的构造函数
func NewNeedleMap(file *os.File) *NeedleMap {
	//初始化m
	nm := &NeedleMap{
		m: NewCompactMap(),
	}
	//对indexFile进行赋值
	nm.indexFile = file
	return nm
}

const (
	RowsToRead = 1024
)

//加载NeedleMap
func LoadNeedleMap(file *os.File) (*NeedleMap, error) {
	//先初始化NeedleMap
	nm := NewNeedleMap(file)
	e := WalkIndexFile(file, func(key uint64, offset, size uint32) error {
		if key > nm.MaximumFileKey {
			nm.MaximumFileKey = key
		}
		nm.FileCounter++
		nm.FileByteCounter = nm.FileByteCounter + uint64(size)
		if offset > 0 {
			oldSize := nm.m.Set(Key(key), offset, size)
			glog.V(3).Infoln("reading key", key, "offset", offset*NeedlePaddingSize, "size", size, "oldSize", oldSize)
			if oldSize > 0 {
				nm.DeletionCounter++
				nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
			}
		} else {
			oldSize := nm.m.Delete(Key(key))
			glog.V(3).Infoln("removing key", key, "offset", offset*NeedlePaddingSize, "size", size, "oldSize", oldSize)
			nm.DeletionCounter++
			nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
		}
		return nil
	})
	glog.V(1).Infoln("max file key:", nm.MaximumFileKey)
	return nm, e
}

// walks through the index file, calls fn function with each key, offset, size
// stops with the error returned by the fn function
func WalkIndexFile(r *os.File, fn func(key uint64, offset, size uint32) error) error {
	var readerOffset int64
	bytes := make([]byte, 16*RowsToRead)
	count, e := r.ReadAt(bytes, readerOffset)
	glog.V(3).Infoln("file", r.Name(), "readerOffset", readerOffset, "count", count, "e", e)
	readerOffset += int64(count)
	var (
		key          uint64
		offset, size uint32
		i            int
	)

	for count > 0 && e == nil || e == io.EOF {
		for i = 0; i+16 <= count; i += 16 {
			key, offset, size = idxFileEntry(bytes[i : i+16])
			if e = fn(key, offset, size); e != nil {
				return e
			}
		}
		if e == io.EOF {
			return nil
		}
		count, e = r.ReadAt(bytes, readerOffset)
		glog.V(3).Infoln("file", r.Name(), "readerOffset", readerOffset, "count", count, "e", e)
		readerOffset += int64(count)
	}
	return e
}

//设置,新增或者修改
func (nm *NeedleMap) Put(key uint64, offset uint32, size uint32) error {
	oldSize := nm.m.Set(Key(key), offset, size)
	//记录日志，记录最大的key，删除的次数，和大小，增加的次数和大小
	nm.logPut(key, oldSize, size)
	//追加到索引文件
	return nm.appendToIndexFile(key, offset, size)
}

//查找,指定key对应的内容,先找slice，再找 map，再找slice
func (nm *NeedleMap) Get(key uint64) (element *NeedleValue, ok bool) {
	element, ok = nm.m.Get(Key(key))
	return
}

//删除指定的key,先找cm中的index，然后在把slice中的size设置为0，然后如果在溢出表里面，删除,为啥还有可能在溢出表呢
func (nm *NeedleMap) Delete(key uint64) error {
	//删除内容
	deletedBytes := nm.m.Delete(Key(key))
	//记录删除的次数和删除的内容的大小
	nm.logDelete(deletedBytes)
	//为啥还更新到索引文件中呢,是为了重启时从从磁盘重建？
	return nm.appendToIndexFile(key, 0, 0)
}

//关闭打开的索引文件
func (nm *NeedleMap) Close() {
	//close nm的indexFile文件句柄
	_ = nm.indexFile.Close()
}

//销毁
func (nm *NeedleMap) Destroy() error {
	nm.Close()                            //先关闭nm
	return os.Remove(nm.indexFile.Name()) //删除索引文件
}
