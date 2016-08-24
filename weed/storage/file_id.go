package storage

import (
	"encoding/hex"
	"errors"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
)

//文件id
type FileId struct {
	VolumeId VolumeId //卷id
	Key      uint64   //文件key
	Hashcode uint32   //hashcode
}

//通过针解析文件id,针的id作为key，针的cookie，作为hashcode
func NewFileIdFromNeedle(VolumeId VolumeId, n *Needle) *FileId {
	return &FileId{VolumeId: VolumeId, Key: n.Id, Hashcode: n.Cookie}
}

//文件id的构造函数,卷id，key，还要hashcode
func NewFileId(VolumeId VolumeId, Key uint64, Hashcode uint32) *FileId {
	return &FileId{VolumeId: VolumeId, Key: Key, Hashcode: Hashcode}
}

//通过fid，解析文件id
func ParseFileId(fid string) (*FileId, error) {
	//fid用，分割
	a := strings.Split(fid, ",")
	//如果不包含两部分，报错
	if len(a) != 2 {
		glog.V(1).Infoln("Invalid fid ", fid, ", split length ", len(a))
		return nil, errors.New("Invalid fid " + fid)
	}
	//解析卷id和，hashstring
	vid_string, key_hash_string := a[0], a[1]
	volumeId, _ := NewVolumeId(vid_string)
	//解析key和hash
	key, hash, e := ParseKeyHash(key_hash_string)
	return &FileId{VolumeId: volumeId, Key: key, Hashcode: hash}, e
}

//fileId的打印函数
func (n *FileId) String() string {
	bytes := make([]byte, 12)
	util.Uint64toBytes(bytes[0:8], n.Key)
	util.Uint32toBytes(bytes[8:12], n.Hashcode)
	nonzero_index := 0
	for ; bytes[nonzero_index] == 0; nonzero_index++ {
	}
	return n.VolumeId.String() + "," + hex.EncodeToString(bytes[nonzero_index:])
}
