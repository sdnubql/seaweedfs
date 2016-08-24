package storage

import (
	"fmt"
	"os"

	"github.com/boltdb/bolt"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
)

//BoltDbNeedleMap的结构
type BoltDbNeedleMap struct {
	dbFileName string //db文件名
	db         *bolt.DB
	baseNeedleMapper
}

var boltdbBucket = []byte("weed")

//BoltDbNeedleMap的构造函数
func NewBoltDbNeedleMap(dbFileName string, indexFile *os.File) (m *BoltDbNeedleMap, err error) {
	//内存申请
	m = &BoltDbNeedleMap{dbFileName: dbFileName}
	//索引文件设置
	m.indexFile = indexFile
	//如果dbFileName文件不是最新的
	if !isBoltDbFresh(dbFileName, indexFile) {
		glog.V(1).Infof("Start to Generate %s from %s", dbFileName, indexFile.Name())
		//重新生成db文件
		generateBoltDbFile(dbFileName, indexFile)
		glog.V(1).Infof("Finished Generating %s from %s", dbFileName, indexFile.Name())
	}
	glog.V(1).Infof("Opening %s...", dbFileName)
	//打开文件
	if m.db, err = bolt.Open(dbFileName, 0644, nil); err != nil {
		return
	}
	glog.V(1).Infof("Loading %s...", indexFile.Name())
	nm, indexLoadError := LoadNeedleMap(indexFile)
	if indexLoadError != nil {
		return nil, indexLoadError
	}
	m.mapMetric = nm.mapMetric
	return
}

func isBoltDbFresh(dbFileName string, indexFile *os.File) bool {
	// normally we always write to index file first
	//打开文件
	dbLogFile, err := os.Open(dbFileName)
	if err != nil {
		return false
	}
	defer dbLogFile.Close()
	//获取db日志文件的状态
	dbStat, dbStatErr := dbLogFile.Stat()
	//获取索引文件的状态
	indexStat, indexStatErr := indexFile.Stat()
	if dbStatErr != nil || indexStatErr != nil {
		glog.V(0).Infof("Can not stat file: %v and %v", dbStatErr, indexStatErr)
		return false
	}

	//比较最后的修改时间
	return dbStat.ModTime().After(indexStat.ModTime())
}

//生成BoltDbFile
func generateBoltDbFile(dbFileName string, indexFile *os.File) error {
	//打开文件
	db, err := bolt.Open(dbFileName, 0644, nil)
	if err != nil { //报错返回
		return err
	}
	defer db.Close()
	return WalkIndexFile(indexFile, func(key uint64, offset, size uint32) error {
		//在索引文件中有这个值时
		if offset > 0 {
			boltDbWrite(db, key, offset, size)
		} else {
			boltDbDelete(db, key)
		}
		return nil
	})
}

func (m *BoltDbNeedleMap) Get(key uint64) (element *NeedleValue, ok bool) {
	//分配内存
	bytes := make([]byte, 8)
	var data []byte
	//转换key到bytes
	util.Uint64toBytes(bytes, key)
	//根据key去查询
	err := m.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(boltdbBucket)
		if bucket == nil {
			return fmt.Errorf("Bucket %q not found!", boltdbBucket)
		}

		data = bucket.Get(bytes)
		return nil
	})
	//异常报错
	if err != nil || len(data) != 8 {
		return nil, false
	}
	//解析返回值,偏移量，大小
	offset := util.BytesToUint32(data[0:4])
	//大小
	size := util.BytesToUint32(data[4:8])
	//解析文件的值
	return &NeedleValue{Key: Key(key), Offset: offset, Size: size}, true
}

//修改
func (m *BoltDbNeedleMap) Put(key uint64, offset uint32, size uint32) error {
	var oldSize uint32
	//查找
	if oldNeedle, ok := m.Get(key); ok {
		oldSize = oldNeedle.Size
	}
	//变更
	m.logPut(key, oldSize, size)
	// write to index file first
	//忘index文件中写数据
	if err := m.appendToIndexFile(key, offset, size); err != nil {
		return fmt.Errorf("cannot write to indexfile %s: %v", m.indexFile.Name(), err)
	}
	//写数据
	return boltDbWrite(m.db, key, offset, size)
}

func boltDbWrite(db *bolt.DB,
	key uint64, offset uint32, size uint32) error {
	//分配内存
	bytes := make([]byte, 16)
	//将key做到前8个字节
	util.Uint64toBytes(bytes[0:8], key)
	//起点做到8-11，四个字节中
	util.Uint32toBytes(bytes[8:12], offset)
	//大小做到最后的4个字节中
	util.Uint32toBytes(bytes[12:16], size)
	//更新db
	return db.Update(func(tx *bolt.Tx) error {
		//判断boltdbBucket是否存在
		bucket, err := tx.CreateBucketIfNotExists(boltdbBucket)
		if err != nil {
			return err
		}
		//如果存在，写db
		err = bucket.Put(bytes[0:8], bytes[8:16])
		if err != nil {
			return err
		}
		return nil
	})
}
func boltDbDelete(db *bolt.DB, key uint64) error {
	//分配内存
	bytes := make([]byte, 8)
	//转换
	util.Uint64toBytes(bytes, key)
	//开始删除
	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(boltdbBucket)
		if err != nil {
			return err
		}

		err = bucket.Delete(bytes)
		if err != nil {
			return err
		}
		return nil
	})
}

func (m *BoltDbNeedleMap) Delete(key uint64) error {
	//获取key的值
	if oldNeedle, ok := m.Get(key); ok {
		//记录旧的针文件的size，然后增加被删除的文件内容的大小，和被删除的次数
		m.logDelete(oldNeedle.Size)
	}
	// write to index file first
	//把这个key写到所有文件中偏移起点为0，大小为0
	if err := m.appendToIndexFile(key, 0, 0); err != nil {
		return err
	}
	//从db中删除
	return boltDbDelete(m.db, key)
}

func (m *BoltDbNeedleMap) Close() {
	//关闭db文件句柄
	m.db.Close()
}

//BoltDbNeedleMap销毁方法
func (m *BoltDbNeedleMap) Destroy() error {
	m.Close()
	//删除索引文件
	os.Remove(m.indexFile.Name())
	//删除db文件
	return os.Remove(m.dbFileName)
}
