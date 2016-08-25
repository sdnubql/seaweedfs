package storage

import (
	"errors"
	"fmt"
)

//复制策略
type ReplicaPlacement struct {
	//相同机架的数量
	SameRackCount int
	//不同机架的数量
	DiffRackCount int
	//不同数据中心的数量
	DiffDataCenterCount int
}

//从字符串中解析复制策略
func NewReplicaPlacementFromString(t string) (*ReplicaPlacement, error) {
	//实例化ReplicaPlacement
	rp := &ReplicaPlacement{}
	//循环读取字符串
	for i, c := range t {
		//获取差值
		count := int(c - '0')
		//强制判断范围
		if 0 <= count && count <= 2 {
			//第一位，代表不同的数据中心的数量
			//第二位，代表不同机架的数量
			//第三位，代表相同机架的数量
			switch i {
			case 0:
				rp.DiffDataCenterCount = count
			case 1:
				rp.DiffRackCount = count
			case 2:
				rp.SameRackCount = count
			}
		} else {
			//超出范围报错
			return rp, errors.New("Unknown Replication Type:" + t)
		}
	}
	return rp, nil
}

//从一个byte中解析复制策略,先把字节强转字符串,不够的用0代替,然后调用底层方法
func NewReplicaPlacementFromByte(b byte) (*ReplicaPlacement, error) {
	return NewReplicaPlacementFromString(fmt.Sprintf("%03d", b))
}

//byte化
func (rp *ReplicaPlacement) Byte() byte {
	ret := rp.DiffDataCenterCount*100 + rp.DiffRackCount*10 + rp.SameRackCount
	return byte(ret)
}

//字符串化，复制策略配置,
func (rp *ReplicaPlacement) String() string {
	b := make([]byte, 3)
	b[0] = byte(rp.DiffDataCenterCount + '0')
	b[1] = byte(rp.DiffRackCount + '0')
	b[2] = byte(rp.SameRackCount + '0')
	return string(b)
}

//获取复制的数量,三个数量相加在加1
func (rp *ReplicaPlacement) GetCopyCount() int {
	return rp.DiffDataCenterCount + rp.DiffRackCount + rp.SameRackCount + 1
}
