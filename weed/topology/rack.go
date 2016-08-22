package topology

import (
	"strconv"
	"time"
)

//Rack数据类型
type Rack struct {
	NodeImpl
}

//初始化rack
func NewRack(id string) *Rack {
	//初始化数据
	r := &Rack{}
	//重置节点id
	r.id = NodeId(id)
	//重置节点类型
	r.nodeType = "Rack"
	//子节点初始化
	r.children = make(map[NodeId]Node)
	//value赋值
	r.NodeImpl.value = r
	return r
}

//根据ip和端口找数据节点
func (r *Rack) FindDataNode(ip string, port int) *DataNode {
	//循环子节点，把子节点转换成数据节点类型，然后匹配，匹配成功后返回子节点
	for _, c := range r.Children() {
		dn := c.(*DataNode)
		if dn.MatchLocation(ip, port) {
			return dn
		}
	}
	return nil
}

//获取或者创建数据节点
func (r *Rack) GetOrCreateDataNode(ip string, port int, publicUrl string, maxVolumeCount int) *DataNode {
	//循环子节点
	for _, c := range r.Children() {
		//把子节点转换成数据节点类型
		dn := c.(*DataNode)
		//根据ip和端口进行匹配，如果存在，更新最后一次查看时间，如果此几点是死节点
		if dn.MatchLocation(ip, port) {
			dn.LastSeen = time.Now().Unix()
			if dn.Dead { //如果是死节点，则进行处理
				dn.Dead = false
				r.GetTopology().chanRecoveredDataNodes <- dn
				dn.UpAdjustMaxVolumeCountDelta(maxVolumeCount - dn.maxVolumeCount)
			}
			return dn
		}
	}
	//不存在，这创建此数据节点
	dn := NewDataNode(ip + ":" + strconv.Itoa(port))
	//增加ip
	dn.Ip = ip
	//增加端口
	dn.Port = port
	//增加公共url
	dn.PublicUrl = publicUrl
	//增加最大卷数量
	dn.maxVolumeCount = maxVolumeCount
	//增加上次查看时间
	dn.LastSeen = time.Now().Unix()
	//添加到子列表中
	r.LinkChildNode(dn)
	return dn
}

//转成映射
func (r *Rack) ToMap() interface{} {
	m := make(map[string]interface{})
	m["Id"] = r.Id()
	m["Max"] = r.GetMaxVolumeCount()
	m["Free"] = r.FreeSpace()
	var dns []interface{}
	//循环映射子节点
	for _, c := range r.Children() {
		dn := c.(*DataNode)
		dns = append(dns, dn.ToMap())
	}
	m["DataNodes"] = dns
	return m
}
