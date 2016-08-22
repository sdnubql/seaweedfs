package storage

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"sort"
)

//卷信息结构
type VolumeInfo struct {
	Id               VolumeId          //卷id
	Size             uint64            //大小
	ReplicaPlacement *ReplicaPlacement //复制策略
	Ttl              *TTL              //过期时间
	Collection       string            //集合
	Version          Version           //版本
	FileCount        int               //文件数
	DeleteCount      int               //删除数
	DeletedByteCount uint64            //被删除的数
	ReadOnly         bool              //是否只读
}

//卷信息的构造函数
func NewVolumeInfo(m *operation.VolumeInformationMessage) (vi VolumeInfo, err error) {
	vi = VolumeInfo{
		Id:               VolumeId(*m.Id), //卷id
		Size:             *m.Size,         //卷大小
		Collection:       *m.Collection,   //卷的集合
		FileCount:        int(*m.FileCount),
		DeleteCount:      int(*m.DeleteCount),
		DeletedByteCount: *m.DeletedByteCount,
		ReadOnly:         *m.ReadOnly,         //是否只读
		Version:          Version(*m.Version), //版本
	}
	//复制策略
	rp, e := NewReplicaPlacementFromByte(byte(*m.ReplicaPlacement))
	if e != nil {
		return vi, e
	}
	vi.ReplicaPlacement = rp
	//设置ttl
	vi.Ttl = LoadTTLFromUint32(*m.Ttl)
	return vi, nil
}

//volumeinfo的打印方法
func (vi VolumeInfo) String() string {
	return fmt.Sprintf("Id:%d, Size:%d, ReplicaPlacement:%s, Collection:%s, Version:%v, FileCount:%d, DeleteCount:%d, DeletedByteCount:%d, ReadOnly:%v",
		vi.Id, vi.Size, vi.ReplicaPlacement, vi.Collection, vi.Version, vi.FileCount, vi.DeleteCount, vi.DeletedByteCount, vi.ReadOnly)
}

/*VolumesInfo sorting*/

//卷信息数组
type volumeInfos []*VolumeInfo

//卷信息数组长度
func (vis volumeInfos) Len() int {
	return len(vis)
}

//判断卷id的比较
func (vis volumeInfos) Less(i, j int) bool {
	return vis[i].Id < vis[j].Id
}

//交换
func (vis volumeInfos) Swap(i, j int) {
	vis[i], vis[j] = vis[j], vis[i]
}

//排序
func sortVolumeInfos(vis volumeInfos) {
	sort.Sort(vis)
}
