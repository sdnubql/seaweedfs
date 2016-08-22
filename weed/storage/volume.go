package storage

import (
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

//卷结构
type Volume struct {
	Id            VolumeId      //卷id
	dir           string        //目录
	Collection    string        //集合
	dataFile      *os.File      //数据文件
	nm            NeedleMapper  //针map
	needleMapKind NeedleMapType //针map类型
	readOnly      bool          //是否只读

	SuperBlock //继承超级块

	dataFileAccessLock sync.Mutex // 锁
	lastModifiedTime   uint64     //unix time in seconds //上一次被修改的时间
}

//卷的构造函数
func NewVolume(dirname string, collection string, id VolumeId, needleMapKind NeedleMapType, replicaPlacement *ReplicaPlacement, ttl *TTL) (v *Volume, e error) {
	v = &Volume{dir: dirname, Collection: collection, Id: id}               //初始化卷
	v.SuperBlock = SuperBlock{ReplicaPlacement: replicaPlacement, Ttl: ttl} //初始化超级块
	v.needleMapKind = needleMapKind                                         //设置针map类型
	e = v.load(true, true, needleMapKind)
	return
}

//卷的打印函数
func (v *Volume) String() string {
	return fmt.Sprintf("Id:%v, dir:%s, Collection:%s, dataFile:%v, nm:%v, readOnly:%v", v.Id, v.dir, v.Collection, v.dataFile, v.nm, v.readOnly)
}

//获取卷的文件名
func (v *Volume) FileName() (fileName string) {
	//如果没有有集合
	if v.Collection == "" {
		//文件名由目录和id拼接的到
		fileName = path.Join(v.dir, v.Id.String())
	} else {
		//有集合时文件名由目录和集合，id拼接而成
		fileName = path.Join(v.dir, v.Collection+"_"+v.Id.String())
	}
	return
}

//卷的数据文件
func (v *Volume) DataFile() *os.File {
	return v.dataFile
}

//卷的版本
func (v *Volume) Version() Version {
	return v.SuperBlock.Version()
}

//卷的大小,通过获取数据文件的大小
func (v *Volume) Size() int64 {
	stat, e := v.dataFile.Stat()
	if e == nil {
		return stat.Size()
	}
	glog.V(0).Infof("Failed to read file size %s %v", v.dataFile.Name(), e)
	return -1
}

// Close cleanly shuts down this volume
//干净的关闭此卷，加锁，然后关闭此卷，然后打开锁
func (v *Volume) Close() {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	v.nm.Close()
	_ = v.dataFile.Close()
}

//判断是否需要复制
func (v *Volume) NeedToReplicate() bool {
	return v.ReplicaPlacement.GetCopyCount() > 1
}

//获取内容的大小
func (v *Volume) ContentSize() uint64 {
	return v.nm.ContentSize()
}

// volume is expired if modified time + volume ttl < now
// except when volume is empty
// or when the volume does not have a ttl
// or when volumeSizeLimit is 0 when server just starts
//判断是否过期
func (v *Volume) expired(volumeSizeLimit uint64) bool {
	//如果volumeSizeLimit == 0不做处理
	if volumeSizeLimit == 0 {
		//skip if we don't know size limit
		return false
	}
	//如果内容大小为0，不做处理
	if v.ContentSize() == 0 {
		return false
	}
	//如果没有设置过期时间,
	if v.Ttl == nil || v.Ttl.Minutes() == 0 {
		return false
	}
	glog.V(0).Infof("now:%v lastModified:%v", time.Now().Unix(), v.lastModifiedTime)
	livedMinutes := (time.Now().Unix() - int64(v.lastModifiedTime)) / 60
	glog.V(0).Infof("ttl:%v lived:%v", v.Ttl, livedMinutes)
	//如果时间间隔小存活时间
	if int64(v.Ttl.Minutes()) < livedMinutes {
		return true
	}
	return false
}

// wait either maxDelayMinutes or 10% of ttl minutes
func (v *Volume) exiredLongEnough(maxDelayMinutes uint32) bool {
	if v.Ttl == nil || v.Ttl.Minutes() == 0 {
		return false
	}
	//先设置为过期时间的10%
	removalDelay := v.Ttl.Minutes() / 10
	//如果过期时间的10% > maxDelayMinutes
	//设置为maxDelayMinutes
	if removalDelay > maxDelayMinutes {
		removalDelay = maxDelayMinutes
	}
	//当前时间比过去时间＋上最后修改的时间还大，返回true
	if uint64(v.Ttl.Minutes()+removalDelay)*60+v.lastModifiedTime < uint64(time.Now().Unix()) {
		return true
	}
	return false
}
