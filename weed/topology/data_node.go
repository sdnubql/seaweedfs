package topology

import (
	"fmt"
	"strconv"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

//数据节点
type DataNode struct {
	NodeImpl
	//存储卷
	volumes map[storage.VolumeId]storage.VolumeInfo
	//ip
	Ip string
	//端口
	Port int
	//公共url
	PublicUrl string
	//最后一次访问时间
	LastSeen int64 // unix time in seconds
	//是否是死节点
	Dead bool
}

//数据节点的构造函数
func NewDataNode(id string) *DataNode {
	//按照数据节点类型分配内存
	s := &DataNode{}
	//设置节点id
	s.id = NodeId(id)
	//设置节点类型
	s.nodeType = "DataNode"
	//设置卷信息
	s.volumes = make(map[storage.VolumeId]storage.VolumeInfo)
	//设置value
	s.NodeImpl.value = s
	return s
}

//数据节点的打印函数
func (dn *DataNode) String() string {
	//加锁
	dn.RLock()
	defer dn.RUnlock()
	//格式化，属性
	return fmt.Sprintf("Node:%s, volumes:%v, Ip:%s, Port:%d, PublicUrl:%s, Dead:%v", dn.NodeImpl.String(), dn.volumes, dn.Ip, dn.Port, dn.PublicUrl, dn.Dead)
}

//添加或者更新卷
func (dn *DataNode) AddOrUpdateVolume(v storage.VolumeInfo) {
	//加锁
	dn.Lock()
	defer dn.Unlock()
	//查找卷id是否已经存在
	if _, ok := dn.volumes[v.Id]; !ok { //如果不存在，去设置
		dn.volumes[v.Id] = v
		dn.UpAdjustVolumeCountDelta(1) //更新卷的数量
		if !v.ReadOnly {               //如果不是只读卷
			dn.UpAdjustActiveVolumeCountDelta(1) //更新激活状态的卷数量
		}
		dn.UpAdjustMaxVolumeId(v.Id) //更新最大的卷id
	} else { //存在时直接更新
		dn.volumes[v.Id] = v
	}
}

//更新卷信息
func (dn *DataNode) UpdateVolumes(actualVolumes []storage.VolumeInfo) (deletedVolumes []storage.VolumeInfo) {
	//分配空间
	actualVolumeMap := make(map[storage.VolumeId]storage.VolumeInfo)
	//根据传过来的值进行赋值
	for _, v := range actualVolumes {
		actualVolumeMap[v.Id] = v
	}
	dn.RLock()
	//循环数据节点的卷列表
	for vid, v := range dn.volumes {
		if _, ok := actualVolumeMap[vid]; !ok { //如果在列表中，直接删除
			glog.V(0).Infoln("Deleting volume id:", vid)
			delete(dn.volumes, vid)
			deletedVolumes = append(deletedVolumes, v)
			dn.UpAdjustVolumeCountDelta(-1)       //调整卷数量
			dn.UpAdjustActiveVolumeCountDelta(-1) //调整激活状态的卷数量
		}
	} //TODO: adjust max volume id, if need to reclaim volume ids
	dn.RUnlock()
	//循环添加到待处理的卷列表
	for _, v := range actualVolumes {
		dn.AddOrUpdateVolume(v)
	}
	return
}

//获取数据节点的所有卷
func (dn *DataNode) GetVolumes() (ret []storage.VolumeInfo) {
	//加锁
	dn.RLock()
	//循环卷列表,往队列中加入卷
	for _, v := range dn.volumes {
		ret = append(ret, v)
	}
	dn.RUnlock()
	return ret
}

//根据卷id返回信息
func (dn *DataNode) GetVolumesById(id storage.VolumeId) (storage.VolumeInfo, error) {
	dn.RLock()
	defer dn.RUnlock()
	//根据卷id查找卷map
	v_info, ok := dn.volumes[id]
	if ok { //如果存在，直接返回卷信息
		return v_info, nil
	} else { //如果不存在，报错
		return storage.VolumeInfo{}, fmt.Errorf("volumeInfo not found")
	}
}

//获取数据中心
func (dn *DataNode) GetDataCenter() *DataCenter {
	return dn.Parent().Parent().(*NodeImpl).value.(*DataCenter)
}

//获取机架
func (dn *DataNode) GetRack() *Rack {
	return dn.Parent().(*NodeImpl).value.(*Rack)
}

//获取拓扑
func (dn *DataNode) GetTopology() *Topology {
	//得到rack节点
	p := dn.Parent()
	//循环获取父节点
	for p.Parent() != nil {
		p = p.Parent()
	}
	t := p.(*Topology)
	return t
}

//进行查找匹配
func (dn *DataNode) MatchLocation(ip string, port int) bool {
	return dn.Ip == ip && dn.Port == port
}

//返回url
func (dn *DataNode) Url() string {
	return dn.Ip + ":" + strconv.Itoa(dn.Port)
}

//数据节点转换成map
func (dn *DataNode) ToMap() interface{} {
	ret := make(map[string]interface{})
	ret["Url"] = dn.Url()
	ret["Volumes"] = dn.GetVolumeCount()
	ret["Max"] = dn.GetMaxVolumeCount()
	ret["Free"] = dn.FreeSpace()
	ret["PublicUrl"] = dn.PublicUrl
	return ret
}
