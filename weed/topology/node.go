package topology

import (
	"errors"
	"math/rand"
	"strings"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

//定义类型NodeId
type NodeId string

//定义节点接口,实现下面一坨方法的，类型都叫做节点类型
type Node interface {
	Id() NodeId
	String() string
	FreeSpace() int
	ReserveOneVolume(r int) (*DataNode, error)
	UpAdjustMaxVolumeCountDelta(maxVolumeCountDelta int)
	UpAdjustVolumeCountDelta(volumeCountDelta int)
	UpAdjustActiveVolumeCountDelta(activeVolumeCountDelta int)
	UpAdjustMaxVolumeId(vid storage.VolumeId)

	GetVolumeCount() int
	GetActiveVolumeCount() int
	GetMaxVolumeCount() int
	GetMaxVolumeId() storage.VolumeId
	SetParent(Node)
	LinkChildNode(node Node)
	UnlinkChildNode(nodeId NodeId)
	CollectDeadNodeAndFullVolumes(freshThreshHold int64, volumeSizeLimit uint64)
	//是否是数据节点
	IsDataNode() bool
	IsRack() bool
	IsDataCenter() bool
	Children() []Node
	Parent() Node

	GetValue() interface{} //get reference to the topology,dc,rack,datanode
}

//节点类型的结构
type NodeImpl struct {
	//节点id
	id NodeId
	//卷数量
	volumeCount int
	//激活状态的卷数量
	activeVolumeCount int
	//最大的卷数量
	maxVolumeCount int
	//父节点
	parent Node
	//锁
	sync.RWMutex // lock children
	//子节点
	children map[NodeId]Node
	//最大卷id
	maxVolumeId storage.VolumeId

	//for rack, data center, topology
	//节点类型
	nodeType string
	//当前值
	value interface{}
}

// the first node must satisfy filterFirstNodeFn(), the rest nodes must have one free slot
//随机选取节点，第一个节点需要满足filterFirstNodeFn，剩下的节点需要有一个空闲的槽
func (n *NodeImpl) RandomlyPickNodes(numberOfNodes int, filterFirstNodeFn func(dn Node) error) (firstNode Node, restNodes []Node, err error) {
	//初始化一个节点slice，长度为0，最大可能长度为节点的子节点数量
	candidates := make([]Node, 0, len(n.children))
	//定义错误的slice
	var errs []string
	//加锁
	n.RLock()
	//遍历子节点,如果节点满足filterFirstNodeFn，则添加到候选节点slice
	for _, node := range n.children {
		if err := filterFirstNodeFn(node); err == nil {
			candidates = append(candidates, node)
		} else {
			errs = append(errs, string(node.Id())+":"+err.Error())
		}
	}
	n.RUnlock()
	//如果候选节点slice的长度为0，报错
	if len(candidates) == 0 {
		return nil, nil, errors.New("No matching data node found! \n" + strings.Join(errs, "\n"))
	}
	//从候选节点slice中随便选出来一个,作为主节点
	firstNode = candidates[rand.Intn(len(candidates))]
	//记录日志
	glog.V(2).Infoln(n.Id(), "picked main node:", firstNode.Id())

	//剩余节点的slice容器,个数为numberOfNodes -1
	restNodes = make([]Node, numberOfNodes-1)
	//清空候选节点slice容器
	candidates = candidates[:0]
	//加锁
	n.RLock()
	//遍历孩子节点
	for _, node := range n.children {
		//如果是刚才选中的第一个节点就continue
		if node.Id() == firstNode.Id() {
			continue
		}
		//如果子节点的剩余空间<=0 继续
		if node.FreeSpace() <= 0 {
			continue
		}
		//记录日志
		glog.V(2).Infoln("select rest node candidate:", node.Id())
		//符合条件的节点加入到剩余节点
		candidates = append(candidates, node)
	}
	//去锁
	n.RUnlock()
	glog.V(2).Infoln(n.Id(), "picking", numberOfNodes-1, "from rest", len(candidates), "node candidates")
	//判断剩余节点数的容器长度是否为0
	ret := len(restNodes) == 0
	for k, node := range candidates {
		if k < len(restNodes) {
			restNodes[k] = node
			if k == len(restNodes)-1 {
				ret = true
			}
		} else {
			r := rand.Intn(k + 1)
			if r < len(restNodes) {
				restNodes[r] = node
			}
		}
	}
	//如果错误记录日志
	if !ret {
		glog.V(2).Infoln(n.Id(), "failed to pick", numberOfNodes-1, "from rest", len(candidates), "node candidates")
		err = errors.New("Not enough data node found!")
	}
	return
}

//判断是否是数据节点
func (n *NodeImpl) IsDataNode() bool {
	return n.nodeType == "DataNode"
}

//判断是否是机架节点
func (n *NodeImpl) IsRack() bool {
	return n.nodeType == "Rack"
}

//是否是数据中心
func (n *NodeImpl) IsDataCenter() bool {
	return n.nodeType == "DataCenter"
}

//实现string方法，打印自己
func (n *NodeImpl) String() string {
	//如果有父节点，从父节点的string到自己的string
	if n.parent != nil {
		return n.parent.String() + ":" + string(n.id)
	}
	return string(n.id)
}

//获取节点的id
func (n *NodeImpl) Id() NodeId {
	return n.id
}

//获取剩余可设置的空间
func (n *NodeImpl) FreeSpace() int {
	return n.maxVolumeCount - n.volumeCount
}

//设置自己的父节点
func (n *NodeImpl) SetParent(node Node) {
	n.parent = node
}

//获取孩子节点，加锁放并发改
func (n *NodeImpl) Children() (ret []Node) {
	n.RLock()
	defer n.RUnlock()
	for _, c := range n.children {
		ret = append(ret, c)
	}
	return ret
}

//返回当前节点的父节点
func (n *NodeImpl) Parent() Node {
	return n.parent
}

//获取当前节点的value值
func (n *NodeImpl) GetValue() interface{} {
	return n.value
}
func (n *NodeImpl) ReserveOneVolume(r int) (assignedNode *DataNode, err error) {
	n.RLock()
	defer n.RUnlock()
	for _, node := range n.children {
		freeSpace := node.FreeSpace()
		// fmt.Println("r =", r, ", node =", node, ", freeSpace =", freeSpace)
		if freeSpace <= 0 {
			continue
		}
		if r >= freeSpace {
			r -= freeSpace
		} else {
			if node.IsDataNode() && node.FreeSpace() > 0 {
				// fmt.Println("vid =", vid, " assigned to node =", node, ", freeSpace =", node.FreeSpace())
				return node.(*DataNode), nil
			}
			assignedNode, err = node.ReserveOneVolume(r)
			if err != nil {
				return
			}
		}
	}
	return
}

//调整当前节点的最大卷数量,如果有父节点，递归调整父节点
func (n *NodeImpl) UpAdjustMaxVolumeCountDelta(maxVolumeCountDelta int) { //can be negative
	n.maxVolumeCount += maxVolumeCountDelta
	if n.parent != nil {
		n.parent.UpAdjustMaxVolumeCountDelta(maxVolumeCountDelta)
	}
}

//调整当前节点的卷数量，如果有父节点，递归调整父节点
func (n *NodeImpl) UpAdjustVolumeCountDelta(volumeCountDelta int) { //can be negative
	n.volumeCount += volumeCountDelta
	if n.parent != nil {
		n.parent.UpAdjustVolumeCountDelta(volumeCountDelta)
	}
}

//调整当前节点的生效卷数量，如果有父节点，递归调整父节点
func (n *NodeImpl) UpAdjustActiveVolumeCountDelta(activeVolumeCountDelta int) { //can be negative
	n.activeVolumeCount += activeVolumeCountDelta
	if n.parent != nil {
		n.parent.UpAdjustActiveVolumeCountDelta(activeVolumeCountDelta)
	}
}

//更新节点的最大卷号,如果有父节点，递归更新相关节点的最大卷号
func (n *NodeImpl) UpAdjustMaxVolumeId(vid storage.VolumeId) { //can be negative
	if n.maxVolumeId < vid {
		n.maxVolumeId = vid
		if n.parent != nil {
			n.parent.UpAdjustMaxVolumeId(vid)
		}
	}
}

//获取最大的卷id
func (n *NodeImpl) GetMaxVolumeId() storage.VolumeId {
	return n.maxVolumeId
}

//获取节点的卷数量
func (n *NodeImpl) GetVolumeCount() int {
	return n.volumeCount
}

//获取生效的卷数量
func (n *NodeImpl) GetActiveVolumeCount() int {
	return n.activeVolumeCount
}

//获取最大的卷数量
func (n *NodeImpl) GetMaxVolumeCount() int {
	return n.maxVolumeCount
}

//添加子节点
func (n *NodeImpl) LinkChildNode(node Node) {
	//加锁
	n.Lock()
	defer n.Unlock()
	//如果节点没有在子节点列表中
	if n.children[node.Id()] == nil {
		//往当前节点的子节点列表，添加节点id
		n.children[node.Id()] = node
		//递归调整当前相关节点的最大卷数量
		n.UpAdjustMaxVolumeCountDelta(node.GetMaxVolumeCount())
		//更新最大卷号
		n.UpAdjustMaxVolumeId(node.GetMaxVolumeId())
		//更新卷数量
		n.UpAdjustVolumeCountDelta(node.GetVolumeCount())
		//更新生效中的卷数量
		n.UpAdjustActiveVolumeCountDelta(node.GetActiveVolumeCount())
		//设置节点的父节点
		node.SetParent(n)
		//打info级别log
		glog.V(0).Infoln(n, "adds child", node.Id())
	}
}

//删除子节点
func (n *NodeImpl) UnlinkChildNode(nodeId NodeId) {
	//加锁
	n.Lock()
	defer n.Unlock()
	//根据节点ID获取指定的子节点
	node := n.children[nodeId]
	//如果子节点存在
	if node != nil {
		//将子节点的父节点指针，指向空
		node.SetParent(nil)
		//将子节点的id从子节点容器中删除
		delete(n.children, node.Id())
		//递归调整节点的卷数量
		n.UpAdjustVolumeCountDelta(-node.GetVolumeCount())
		//递归调整生效的卷数量
		n.UpAdjustActiveVolumeCountDelta(-node.GetActiveVolumeCount())
		//递归调整最大的卷数量
		n.UpAdjustMaxVolumeCountDelta(-node.GetMaxVolumeCount())
		//记录日志
		glog.V(0).Infoln(n, "removes", node, "volumeCount =", n.activeVolumeCount)
	}
}

func (n *NodeImpl) CollectDeadNodeAndFullVolumes(freshThreshHold int64, volumeSizeLimit uint64) {
	if n.IsRack() {
		for _, c := range n.Children() {
			dn := c.(*DataNode) //can not cast n to DataNode
			if dn.LastSeen < freshThreshHold {
				if !dn.Dead {
					dn.Dead = true
					n.GetTopology().chanDeadDataNodes <- dn
				}
			}
			for _, v := range dn.GetVolumes() {
				if uint64(v.Size) >= volumeSizeLimit {
					//fmt.Println("volume",v.Id,"size",v.Size,">",volumeSizeLimit)
					n.GetTopology().chanFullVolumes <- v
				}
			}
		}
	} else {
		for _, c := range n.Children() {
			c.CollectDeadNodeAndFullVolumes(freshThreshHold, volumeSizeLimit)
		}
	}
}

//获取最上层的父拓扑节点
func (n *NodeImpl) GetTopology() *Topology {
	//定义一个节点
	var p Node
	//将当前节点赋给定义的节点变量
	p = n
	//循环查找自己的父节点，直到找到最起点节点
	for p.Parent() != nil {
		p = p.Parent()
	}
	//找到value并进行类型转换
	return p.GetValue().(*Topology)
}
