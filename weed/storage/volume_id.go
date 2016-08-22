package storage

import (
	"strconv"
)

//定义卷id
type VolumeId uint32

//将string型的卷id转换成特定类型的卷的id
func NewVolumeId(vid string) (VolumeId, error) {
	volumeId, err := strconv.ParseUint(vid, 10, 64)
	return VolumeId(volumeId), err
}

//将卷id字符串化
func (vid *VolumeId) String() string {
	return strconv.FormatUint(uint64(*vid), 10)
}

//获取下一个卷id
func (vid *VolumeId) Next() VolumeId {
	return VolumeId(uint32(*vid) + 1)
}
