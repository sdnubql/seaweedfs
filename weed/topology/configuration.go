package topology

import (
	"encoding/xml"
)

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
	XMLName     xml.Name `xml:"Configuration"`
	Topo        topology `xml:"Topology"`
	ip2location map[string]loc
}

//config文件的构造函数
func NewConfiguration(b []byte) (*Configuration, error) {
	c := &Configuration{}
	err := xml.Unmarshal(b, c)
	c.ip2location = make(map[string]loc)
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

func (c *Configuration) Locate(ip string, dcName string, rackName string) (dc string, rack string) {
	if c != nil && c.ip2location != nil {
		if loc, ok := c.ip2location[ip]; ok {
			return loc.dcName, loc.rackName
		}
	}

	if dcName == "" {
		dcName = "DefaultDataCenter"
	}

	if rackName == "" {
		rackName = "DefaultRack"
	}

	return dcName, rackName
}
