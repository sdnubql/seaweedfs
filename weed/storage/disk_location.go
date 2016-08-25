package storage

import (
	"io/ioutil"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

//DiskLocation结构
type DiskLocation struct {
	//目录位置
	Directory string
	//最多挂在的卷数量
	MaxVolumeCount int
	//卷列表
	volumes map[VolumeId]*Volume
}

//DiskLocation构造函数
func NewDiskLocation(dir string, maxVolumeCount int) *DiskLocation {
	//分配内存
	location := &DiskLocation{Directory: dir, MaxVolumeCount: maxVolumeCount}
	//初始化卷列表
	location.volumes = make(map[VolumeId]*Volume)
	return location
}

//加载存在的卷
func (l *DiskLocation) loadExistingVolumes(needleMapKind NeedleMapType) {

	//读取目录
	if dirs, err := ioutil.ReadDir(l.Directory); err == nil {
		//循环读取
		for _, dir := range dirs {
			//文件名称
			name := dir.Name()
			//如果不是目录，并且有后缀.bat
			if !dir.IsDir() && strings.HasSuffix(name, ".dat") {
				//集合
				collection := ""
				//获取去掉后缀的文件名
				base := name[:len(name)-len(".dat")]
				//在找collection的分隔位
				i := strings.LastIndex(base, "_")
				//如果存在，分别获取赋值
				if i > 0 {
					collection, base = base[0:i], base[i+1:]
				}
				//创建卷id
				if vid, err := NewVolumeId(base); err == nil {
					if l.volumes[vid] == nil {
						//构建卷信息
						if v, e := NewVolume(l.Directory, collection, vid, needleMapKind, nil, nil); e == nil {
							l.volumes[vid] = v
							glog.V(0).Infof("data file %s, replicaPlacement=%s v=%d size=%d ttl=%s", l.Directory+"/"+name, v.ReplicaPlacement, v.Version(), v.Size(), v.Ttl.String())
						} else {
							glog.V(0).Infof("new volume %s error %s", name, e)
						}
					}
				}
			}
		}
	}
	glog.V(0).Infoln("Store started on dir:", l.Directory, "with", len(l.volumes), "volumes", "max", l.MaxVolumeCount)
}

//删除指定集合的卷
func (l *DiskLocation) DeleteCollectionFromDiskLocation(collection string) (e error) {
	//循环卷列表
	for k, v := range l.volumes {
		//判断卷的集合是否跟指定的集合相同
		if v.Collection == collection {
			//根据id删除卷
			e = l.deleteVolumeById(k)
			if e != nil {
				return
			}
		}
	}
	return
}

//根据卷id删除卷
func (l *DiskLocation) deleteVolumeById(vid VolumeId) (e error) {
	//找到卷
	v, ok := l.volumes[vid]
	if !ok {
		return
	}
	//销毁卷数据
	e = v.Destroy()
	if e != nil {
		return
	}
	//从列表里面删除
	delete(l.volumes, vid)
	return
}
