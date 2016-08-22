package topology

import (
	"errors"
	"io/ioutil"
	"math/rand"

	"github.com/chrislusf/raft"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/sequence"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
)

//拓扑结构体
type Topology struct {
	//继承NodeImpl
	NodeImpl

	collectionMap *util.ConcurrentReadMap

	pulse int64
	//卷大小
	volumeSizeLimit uint64
	//发号器
	Sequence sequence.Sequencer
	//死的数据节点
	chanDeadDataNodes      chan *DataNode
	chanRecoveredDataNodes chan *DataNode
	//满了的数据节点
	chanFullVolumes chan storage.VolumeInfo

	configuration *Configuration

	RaftServer raft.Server
}

func NewTopology(id string, confFile string, seq sequence.Sequencer, volumeSizeLimit uint64, pulse int) (*Topology, error) {
	//初始化拓扑变量
	t := &Topology{}
	//设计节点id
	t.id = NodeId(id)
	//设置节点类型
	t.nodeType = "Topology"
	t.NodeImpl.value = t
	t.children = make(map[NodeId]Node)
	t.collectionMap = util.NewConcurrentReadMap()
	t.pulse = int64(pulse)
	//设置卷的大小
	t.volumeSizeLimit = volumeSizeLimit
	//设置发号器
	t.Sequence = seq

	t.chanDeadDataNodes = make(chan *DataNode)
	t.chanRecoveredDataNodes = make(chan *DataNode)
	t.chanFullVolumes = make(chan storage.VolumeInfo)
	//加载配置文件
	err := t.loadConfiguration(confFile)

	return t, err
}

//拓扑的是否是leade人的判断方法
func (t *Topology) IsLeader() bool {
	//如果是leader节点的话，在进行比较
	if leader, e := t.Leader(); e == nil {
		return leader == t.RaftServer.Name()
	}
	return false
}

//拓扑的leader方法
func (t *Topology) Leader() (string, error) {
	l := ""
	//如果RaftServer不为nil，则取RaftServer的leader方法,否则换回错误
	if t.RaftServer != nil {
		l = t.RaftServer.Leader()
	} else {
		return "", errors.New("Raft Server not ready yet!")
	}
	//如果取完RaftServer的leader方法还为空，也报错
	if l == "" {
		// We are a single node cluster, we are the leader
		return t.RaftServer.Name(), errors.New("Raft Server not initialized!")
	}

	return l, nil
}

//加载config文件
func (t *Topology) loadConfiguration(configurationFile string) error {
	//读取文件
	b, e := ioutil.ReadFile(configurationFile)
	if e == nil {
		//根据配置文件设置
		t.configuration, e = NewConfiguration(b)
		return e
	}
	glog.V(0).Infoln("Using default configurations.")
	return nil
}

func (t *Topology) Lookup(collection string, vid storage.VolumeId) []*DataNode {
	//maybe an issue if lots of collections?
	if collection == "" {
		for _, c := range t.collectionMap.Items() {
			if list := c.(*Collection).Lookup(vid); list != nil {
				return list
			}
		}
	} else {
		if c, ok := t.collectionMap.Find(collection); ok {
			return c.(*Collection).Lookup(vid)
		}
	}
	return nil
}

func (t *Topology) NextVolumeId() storage.VolumeId {
	vid := t.GetMaxVolumeId()
	next := vid.Next()
	go t.RaftServer.Do(NewMaxVolumeIdCommand(next))
	return next
}

func (t *Topology) HasWritableVolume(option *VolumeGrowOption) bool {
	vl := t.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl)
	return vl.GetActiveVolumeCount(option) > 0
}

func (t *Topology) PickForWrite(count uint64, option *VolumeGrowOption) (string, uint64, *DataNode, error) {
	vid, count, datanodes, err := t.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl).PickForWrite(count, option)
	if err != nil || datanodes.Length() == 0 {
		return "", 0, nil, errors.New("No writable volumes available!")
	}
	fileId, count := t.Sequence.NextFileId(count)
	return storage.NewFileId(*vid, fileId, rand.Uint32()).String(), count, datanodes.Head(), nil
}

func (t *Topology) GetVolumeLayout(collectionName string, rp *storage.ReplicaPlacement, ttl *storage.TTL) *VolumeLayout {
	return t.collectionMap.Get(collectionName, func() interface{} {
		return NewCollection(collectionName, t.volumeSizeLimit)
	}).(*Collection).GetOrCreateVolumeLayout(rp, ttl)
}

func (t *Topology) FindCollection(collectionName string) (*Collection, bool) {
	c, hasCollection := t.collectionMap.Find(collectionName)
	return c.(*Collection), hasCollection
}

func (t *Topology) DeleteCollection(collectionName string) {
	t.collectionMap.Delete(collectionName)
}

func (t *Topology) RegisterVolumeLayout(v storage.VolumeInfo, dn *DataNode) {
	t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl).RegisterVolume(&v, dn)
}
func (t *Topology) UnRegisterVolumeLayout(v storage.VolumeInfo, dn *DataNode) {
	glog.Infof("removing volume info:%+v", v)
	t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl).UnRegisterVolume(&v, dn)
}

func (t *Topology) ProcessJoinMessage(joinMessage *operation.JoinMessage) {
	t.Sequence.SetMax(*joinMessage.MaxFileKey)
	dcName, rackName := t.configuration.Locate(*joinMessage.Ip, *joinMessage.DataCenter, *joinMessage.Rack)
	dc := t.GetOrCreateDataCenter(dcName)
	rack := dc.GetOrCreateRack(rackName)
	dn := rack.FindDataNode(*joinMessage.Ip, int(*joinMessage.Port))
	if *joinMessage.IsInit && dn != nil {
		t.UnRegisterDataNode(dn)
	}
	dn = rack.GetOrCreateDataNode(*joinMessage.Ip,
		int(*joinMessage.Port), *joinMessage.PublicUrl,
		int(*joinMessage.MaxVolumeCount))
	var volumeInfos []storage.VolumeInfo
	for _, v := range joinMessage.Volumes {
		if vi, err := storage.NewVolumeInfo(v); err == nil {
			volumeInfos = append(volumeInfos, vi)
		} else {
			glog.V(0).Infoln("Fail to convert joined volume information:", err.Error())
		}
	}
	deletedVolumes := dn.UpdateVolumes(volumeInfos)
	for _, v := range volumeInfos {
		t.RegisterVolumeLayout(v, dn)
	}
	for _, v := range deletedVolumes {
		t.UnRegisterVolumeLayout(v, dn)
	}
}

//根据数据节点名称,获取或者创建数据中心,类似于mysql的replace
func (t *Topology) GetOrCreateDataCenter(dcName string) *DataCenter {
	//循环拓扑的子节点
	for _, c := range t.Children() {
		//将子节点转换成数据中心类型
		dc := c.(*DataCenter)
		//判断id是否一致，如果一致返回此节点
		if string(dc.Id()) == dcName {
			return dc
		}
	}
	//不存在，示例话数据中心
	dc := NewDataCenter(dcName)
	//将数据中心添加到拓扑的子节点列表
	t.LinkChildNode(dc)
	return dc
}
