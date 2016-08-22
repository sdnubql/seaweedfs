package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

// isFileUnchanged checks whether this needle to write is same as last one.
// It requires serialized access in the same volume.
//文件是否没有变化，防止重复提交
func (v *Volume) isFileUnchanged(n *Needle) bool {
	//如果有ttl返回false,这个是为啥？？？？？？？？没有看懂,为啥有ttl时间就认为没有相同呢,因为会自动过期？
	if v.Ttl.String() != "" {
		return false
	}
	//根据id获取
	nv, ok := v.nm.Get(n.Id)
	//根据Offset去截取
	if ok && nv.Offset > 0 {
		oldNeedle := new(Needle)
		err := oldNeedle.ReadData(v.dataFile, int64(nv.Offset)*NeedlePaddingSize, nv.Size, v.Version())
		if err != nil {
			glog.V(0).Infof("Failed to check updated file %v", err)
			return false
		}
		defer oldNeedle.ReleaseMemory()
		//比较checksum，和字节比较
		if oldNeedle.Checksum == n.Checksum && bytes.Equal(oldNeedle.Data, n.Data) {
			n.DataSize = oldNeedle.DataSize
			return true
		}
	}
	return false
}

// Destroy removes everything related to this volume
//销毁掉此卷关联的所有的信息
func (v *Volume) Destroy() (err error) {
	if v.readOnly { //如果卷只读，报错
		err = fmt.Errorf("%s is read-only", v.dataFile.Name())
		return
	}
	v.Close()
	//删除文件
	err = os.Remove(v.dataFile.Name())
	if err != nil {
		return
	}
	//销毁
	err = v.nm.Destroy()
	return
}

// AppendBlob append a blob to end of the data file, used in replication
//追加对象文件到数据文件末尾，用于复制
func (v *Volume) AppendBlob(b []byte) (offset int64, err error) {
	if v.readOnly { //如果只读报错
		err = fmt.Errorf("%s is read-only", v.dataFile.Name())
		return
	}
	//加锁
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	//倒着读文件
	if offset, err = v.dataFile.Seek(0, 2); err != nil {
		glog.V(0).Infof("failed to seek the end of file: %v", err)
		return
	}
	//ensure file writing starting from aligned positions
	if offset%NeedlePaddingSize != 0 { //如果没有对齐
		//重新对齐
		offset = offset + (NeedlePaddingSize - offset%NeedlePaddingSize)
		if offset, err = v.dataFile.Seek(offset, 0); err != nil {
			glog.V(0).Infof("failed to align in datafile %s: %v", v.dataFile.Name(), err)
			return
		}
	}
	//把内容写到数据文件中
	v.dataFile.Write(b)
	return
}

//写文件
func (v *Volume) writeNeedle(n *Needle) (size uint32, err error) {
	glog.V(4).Infof("writing needle %s", NewFileIdFromNeedle(v.Id, n).String())
	if v.readOnly { //如果卷只读，报错
		err = fmt.Errorf("%s is read-only", v.dataFile.Name())
		return
	}
	//加锁
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	//如果文件已经写过，直接返回
	if v.isFileUnchanged(n) {
		size = n.DataSize
		glog.V(4).Infof("needle is unchanged!")
		return
	}
	var offset int64
	//定位到末尾
	if offset, err = v.dataFile.Seek(0, 2); err != nil {
		glog.V(0).Infof("failed to seek the end of file: %v", err)
		return
	}

	//ensure file writing starting from aligned positions
	//如果没有对齐
	if offset%NeedlePaddingSize != 0 {
		offset = offset + (NeedlePaddingSize - offset%NeedlePaddingSize)
		//对齐节点
		if offset, err = v.dataFile.Seek(offset, 0); err != nil {
			glog.V(0).Infof("failed to align in datafile %s: %v", v.dataFile.Name(), err)
			return
		}
	}
	//写内容，如果出错了，truncate，已经写过了的内容
	if size, err = n.Append(v.dataFile, v.Version()); err != nil {
		if e := v.dataFile.Truncate(offset); e != nil {
			err = fmt.Errorf("%s\ncannot truncate %s: %v", err, v.dataFile.Name(), e)
		}
		return
	}
	跟id去获取写入的内容
	nv, ok := v.nm.Get(n.Id)
	//如果不对，写日志
	if !ok || int64(nv.Offset)*NeedlePaddingSize < offset {
		if err = v.nm.Put(n.Id, uint32(offset/NeedlePaddingSize), n.Size); err != nil {
			glog.V(4).Infof("failed to save in needle map %d: %v", n.Id, err)
		}
	}
	//设置最大修改时间
	if v.lastModifiedTime < n.LastModified {
		v.lastModifiedTime = n.LastModified
	}
	return
}

func (v *Volume) deleteNeedle(n *Needle) (uint32, error) {
	glog.V(4).Infof("delete needle %s", NewFileIdFromNeedle(v.Id, n).String())
	if v.readOnly {
		return 0, fmt.Errorf("%s is read-only", v.dataFile.Name())
	}
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	nv, ok := v.nm.Get(n.Id)
	//fmt.Println("key", n.Id, "volume offset", nv.Offset, "data_size", n.Size, "cached size", nv.Size)
	if ok {
		size := nv.Size
		if err := v.nm.Delete(n.Id); err != nil {
			return size, err
		}
		if _, err := v.dataFile.Seek(0, 2); err != nil {
			return size, err
		}
		n.Data = nil
		_, err := n.Append(v.dataFile, v.Version())
		return size, err
	}
	return 0, nil
}

// read fills in Needle content by looking up n.Id from NeedleMapper
func (v *Volume) readNeedle(n *Needle) (int, error) {
	nv, ok := v.nm.Get(n.Id)
	if !ok || nv.Offset == 0 {
		return -1, errors.New("Not Found")
	}
	err := n.ReadData(v.dataFile, int64(nv.Offset)*NeedlePaddingSize, nv.Size, v.Version())
	if err != nil {
		return 0, err
	}
	bytesRead := len(n.Data)
	if !n.HasTtl() {
		return bytesRead, nil
	}
	ttlMinutes := n.Ttl.Minutes()
	if ttlMinutes == 0 {
		return bytesRead, nil
	}
	if !n.HasLastModifiedDate() {
		return bytesRead, nil
	}
	if uint64(time.Now().Unix()) < n.LastModified+uint64(ttlMinutes*60) {
		return bytesRead, nil
	}
	n.ReleaseMemory()
	return -1, errors.New("Not Found")
}

func ScanVolumeFile(dirname string, collection string, id VolumeId,
	needleMapKind NeedleMapType,
	visitSuperBlock func(SuperBlock) error,
	readNeedleBody bool,
	visitNeedle func(n *Needle, offset int64) error) (err error) {
	var v *Volume
	if v, err = loadVolumeWithoutIndex(dirname, collection, id, needleMapKind); err != nil {
		return fmt.Errorf("Failed to load volume %d: %v", id, err)
	}
	if err = visitSuperBlock(v.SuperBlock); err != nil {
		return fmt.Errorf("Failed to process volume %d super block: %v", id, err)
	}

	version := v.Version()

	offset := int64(SuperBlockSize)
	n, rest, e := ReadNeedleHeader(v.dataFile, version, offset)
	if e != nil {
		err = fmt.Errorf("cannot read needle header: %v", e)
		return
	}
	for n != nil {
		if readNeedleBody {
			if err = n.ReadNeedleBody(v.dataFile, version, offset+int64(NeedleHeaderSize), rest); err != nil {
				glog.V(0).Infof("cannot read needle body: %v", err)
				//err = fmt.Errorf("cannot read needle body: %v", err)
				//return
			}
			if n.DataSize >= n.Size {
				// this should come from a bug reported on #87 and #93
				// fixed in v0.69
				// remove this whole "if" clause later, long after 0.69
				oldRest, oldSize := rest, n.Size
				padding := NeedlePaddingSize - ((n.Size + NeedleHeaderSize + NeedleChecksumSize) % NeedlePaddingSize)
				n.Size = 0
				rest = n.Size + NeedleChecksumSize + padding
				if rest%NeedlePaddingSize != 0 {
					rest += (NeedlePaddingSize - rest%NeedlePaddingSize)
				}
				glog.V(4).Infof("Adjusting n.Size %d=>0 rest:%d=>%d %+v", oldSize, oldRest, rest, n)
			}
		}
		if err = visitNeedle(n, offset); err != nil {
			glog.V(0).Infof("visit needle error: %v", err)
		}
		offset += int64(NeedleHeaderSize) + int64(rest)
		glog.V(4).Infof("==> new entry offset %d", offset)
		if n, rest, err = ReadNeedleHeader(v.dataFile, version, offset); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("cannot read needle header: %v", err)
		}
		glog.V(4).Infof("new entry needle size:%d rest:%d", n.Size, rest)
	}

	return
}
