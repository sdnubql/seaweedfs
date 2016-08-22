package topology

import (
	"encoding/xml"
)

//机架和数据中心的位置
type loc struct {
	dcName   string
	rackName string
}

//机架，包括名称和ip列表
type rack struct {
	Name string   `xml:"name,attr"`
	Ips  []string `xml:"Ip"`
}

//数据中心，包括名称和机架列表
type dataCenter struct {
	Name  string `xml:"name,attr"`
	Racks []rack `xml:"Rack"`
}

//顶级拓扑，包括数据中心列表
type topology struct {
	DataCenters []dataCenter `xml:"DataCenter"`
}

//config配置结构
type Configuration struct {
	//配置名称
	XMLName xml.Name `xml:"Configuration"`
	//拓扑
	Topo topology `xml:"Topology"`
	//根据ip反查位置的映射表
	ip2location map[string]loc
}

//config文件的构造函数
func NewConfiguration(b []byte) (*Configuration, error) {
	c := &Configuration{}
	err := xml.Unmarshal(b, c)
	c.ip2location = make(map[string]loc)
	//直接循环数据中心和机架，用ip做key，来映射ip和数据中心，机架之间的映射关系
	for _, dc := range c.Topo.DataCenters {
		for _, rack := range dc.Racks {
			for _, ip := range rack.Ips {
				c.ip2location[ip] = loc{dcName: dc.Name, rackName: rack.Name}
			}
		}
	}
	return c, err
}

//实现字符串化方法
func (c *Configuration) String() string {
	if b, e := xml.MarshalIndent(c, "  ", "  "); e == nil {
		return string(b)
	}
	return ""
}

//根据ip去找到映射关系
func (c *Configuration) Locate(ip string, dcName string, rackName string) (dc string, rack string) {
	//如果能用ip找到映射关系，直接返回ip映射中的机架名称和数据中心的名称
	if c != nil && c.ip2location != nil {
		if loc, ok := c.ip2location[ip]; ok {
			return loc.dcName, loc.rackName
		}
	}

	//如果没找到返回默认的数据中心值
	if dcName == "" {
		dcName = "DefaultDataCenter"
	}

	//如果没找到返回默认的机架值
	if rackName == "" {
		rackName = "DefaultRack"
	}

	return dcName, rackName
}
