package storage

import (
	"fmt"
	"os"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
)

const (
	SuperBlockSize = 8
)

/*
* Super block currently has 8 bytes allocated for each volume.
* Byte 0: version, 1 or 2
* Byte 1: Replica Placement strategy, 000, 001, 002, 010, etc
* Byte 2 and byte 3: Time to live. See TTL for definition
* Byte 4 and byte 5: The number of times the volume has been compacted.
* Rest bytes: Reserved
 */
//超级块结构
type SuperBlock struct {
	version          Version
	ReplicaPlacement *ReplicaPlacement
	Ttl              *TTL
	CompactRevision  uint16
}

//返回超级块的版本
func (s *SuperBlock) Version() Version {
	return s.version
}

//超级块的字节表示
func (s *SuperBlock) Bytes() []byte {
	//超级块的内存申请
	header := make([]byte, SuperBlockSize)
	//第一个字节存储版本
	header[0] = byte(s.version)
	//第二个字节存储复制策略
	header[1] = s.ReplicaPlacement.Byte()
	//2,3两个字节存储超时时间
	s.Ttl.ToBytes(header[2:4])
	//4，5两个字节存储卷已经压实的时间
	util.Uint16toBytes(header[4:6], s.CompactRevision)
	return header
}

func (v *Volume) maybeWriteSuperBlock() error {
	//获取卷文件的信息
	stat, e := v.dataFile.Stat()
	//如果有错记录日志
	if e != nil {
		glog.V(0).Infof("failed to stat datafile %s: %v", v.dataFile, e)
		return e
	}
	//如果状态的大小为0
	if stat.Size() == 0 {
		v.SuperBlock.version = CurrentVersion
		_, e = v.dataFile.Write(v.SuperBlock.Bytes())
		//写超级块到文件中
		if e != nil && os.IsPermission(e) {
			//read-only, but zero length - recreate it!
			//长度为0时，只读时，重建更改只读的状态
			if v.dataFile, e = os.Create(v.dataFile.Name()); e == nil {
				if _, e = v.dataFile.Write(v.SuperBlock.Bytes()); e == nil {
					v.readOnly = false
				}
			}
		}
	}
	return e
}

//读取超级块
func (v *Volume) readSuperBlock() (err error) {
	//定位到卷到文件到开头
	if _, err = v.dataFile.Seek(0, 0); err != nil {
		return fmt.Errorf("cannot seek to the beginning of %s: %v", v.dataFile.Name(), err)
	}
	//初始化超级块内存
	header := make([]byte, SuperBlockSize)
	//读取文件数据到header
	if _, e := v.dataFile.Read(header); e != nil {
		return fmt.Errorf("cannot read volume %d super block: %v", v.Id, e)
	}
	//解析头文件到卷到超级块属性中
	v.SuperBlock, err = ParseSuperBlock(header)
	return err
}

//解析超级块
func ParseSuperBlock(header []byte) (superBlock SuperBlock, err error) {
	//直接强制类型转换第一个字节作为版本
	superBlock.version = Version(header[0])
	//从第一个节点，解析复制策略的类型
	if superBlock.ReplicaPlacement, err = NewReplicaPlacementFromByte(header[1]); err != nil {
		err = fmt.Errorf("cannot read replica type: %s", err.Error())
	}
	//从2，3字节解析过期时间
	superBlock.Ttl = LoadTTLFromBytes(header[2:4])
	//从4，5字节解析压实时间
	superBlock.CompactRevision = util.BytesToUint16(header[4:6])
	return
}
