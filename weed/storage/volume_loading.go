package storage

import (
	"fmt"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

func loadVolumeWithoutIndex(dirname string, collection string, id VolumeId, needleMapKind NeedleMapType) (v *Volume, e error) {
	//初始化卷信息
	v = &Volume{dir: dirname, Collection: collection, Id: id}
	//设置卷的超级块
	v.SuperBlock = SuperBlock{}
	//设置文件的映射类别
	v.needleMapKind = needleMapKind
	e = v.load(false, false, needleMapKind)
	return
}

func (v *Volume) load(alsoLoadIndex bool, createDatIfMissing bool, needleMapKind NeedleMapType) error {
	var e error
	//拼接文件名
	fileName := v.FileName()

	//检查文件
	if exists, canRead, canWrite, modifiedTime := checkFile(fileName + ".dat"); exists {
		//不可读报错
		if !canRead {
			return fmt.Errorf("cannot read Volume Data file %s.dat", fileName)
		}
		//如果可写
		if canWrite {
			//打开文件
			v.dataFile, e = os.OpenFile(fileName+".dat", os.O_RDWR|os.O_CREATE, 0644)
			//修改最后修改的时间
			v.lastModifiedTime = uint64(modifiedTime.Unix())
		} else {
			//不可写，打开文件，设置卷为只读
			glog.V(0).Infoln("opening " + fileName + ".dat in READONLY mode")
			v.dataFile, e = os.Open(fileName + ".dat")
			v.readOnly = true
		}
	} else {
		//文件不存在
		if createDatIfMissing { //如果设置了在没找到时创建
			//创建文件
			v.dataFile, e = os.OpenFile(fileName+".dat", os.O_RDWR|os.O_CREATE, 0644)
		} else {
			return fmt.Errorf("Volume Data file %s.dat does not exist.", fileName)
		}
	}

	if e != nil {
		if !os.IsPermission(e) { //没有权限时报错
			return fmt.Errorf("cannot load Volume Data %s.dat: %v", fileName, e)
		}
	}
	//如果没有设置复制策略
	if v.ReplicaPlacement == nil {
		e = v.readSuperBlock()
	} else {
		e = v.maybeWriteSuperBlock()
	}
	if e == nil && alsoLoadIndex {
		var indexFile *os.File
		if v.readOnly {
			glog.V(1).Infoln("open to read file", fileName+".idx")
			if indexFile, e = os.OpenFile(fileName+".idx", os.O_RDONLY, 0644); e != nil {
				return fmt.Errorf("cannot read Volume Index %s.idx: %v", fileName, e)
			}
		} else {
			glog.V(1).Infoln("open to write file", fileName+".idx")
			if indexFile, e = os.OpenFile(fileName+".idx", os.O_RDWR|os.O_CREATE, 0644); e != nil {
				return fmt.Errorf("cannot write Volume Index %s.idx: %v", fileName, e)
			}
		}
		if e = CheckVolumeDataIntegrity(v, indexFile); e != nil {
			v.readOnly = true
			glog.V(0).Infof("volumeDataIntegrityChecking failed %v", e)
		}
		//根据文件类型，获取不同的nm
		switch needleMapKind {
		case NeedleMapInMemory:
			glog.V(0).Infoln("loading index file", fileName+".idx", "readonly", v.readOnly)
			if v.nm, e = LoadNeedleMap(indexFile); e != nil {
				glog.V(0).Infof("loading index %s error: %v", fileName+".idx", e)
			}
		case NeedleMapLevelDb:
			glog.V(0).Infoln("loading leveldb file", fileName+".ldb")
			if v.nm, e = NewLevelDbNeedleMap(fileName+".ldb", indexFile); e != nil {
				glog.V(0).Infof("loading leveldb %s error: %v", fileName+".ldb", e)
			}
		case NeedleMapBoltDb:
			glog.V(0).Infoln("loading boltdb file", fileName+".bdb")
			if v.nm, e = NewBoltDbNeedleMap(fileName+".bdb", indexFile); e != nil {
				glog.V(0).Infof("loading boltdb %s error: %v", fileName+".bdb", e)
			}
		}
	}
	return e
}

//检查文件
func checkFile(filename string) (exists, canRead, canWrite bool, modTime time.Time) {
	exists = true
	fi, err := os.Stat(filename)
	//文件是否存在
	if os.IsNotExist(err) {
		exists = false
		return
	}
	//判断文件是否可读
	if fi.Mode()&0400 != 0 {
		canRead = true
	}
	//判断文件是否可写
	if fi.Mode()&0200 != 0 {
		canWrite = true
	}
	//文件的修改时间
	modTime = fi.ModTime()
	return
}
