package topology

//数据中心直接继承nodeImpl
type DataCenter struct {
	NodeImpl
}

//数据中心构造函数
func NewDataCenter(id string) *DataCenter {
	//分配内存
	dc := &DataCenter{}
	//id赋值
	dc.id = NodeId(id)
	//类型设置
	dc.nodeType = "DataCenter"
	//孩子节点分配内存
	dc.children = make(map[NodeId]Node)
	//value赋值
	dc.NodeImpl.value = dc
	return dc
}

//获取或者创建机架
func (dc *DataCenter) GetOrCreateRack(rackName string) *Rack {
	//找到数据中心的孩子节点
	for _, c := range dc.Children() {
		//转换成机架节点
		rack := c.(*Rack)
		//如果存在此节点，直接返回
		if string(rack.Id()) == rackName {
			return rack
		}
	}
	//创建机架
	rack := NewRack(rackName)
	//添加到数据中心的孩子节点
	dc.LinkChildNode(rack)
	return rack
}

//创建映射
func (dc *DataCenter) ToMap() interface{} {
	m := make(map[string]interface{})
	//id赋值
	m["Id"] = dc.Id()
	//最大卷数
	m["Max"] = dc.GetMaxVolumeCount()
	//空闲空间设置
	m["Free"] = dc.FreeSpace()
	var racks []interface{}
	//处理数据中心的孩子节点
	for _, c := range dc.Children() {
		//转成rack类型
		rack := c.(*Rack)
		//调用rack的ToMap方法，先转成map，然后添加到slice列表中
		racks = append(racks, rack.ToMap())
	}
	//增加新key racks
	m["Racks"] = racks
	return m
}
