package storage

import (
	"fmt"
	"os"

	"github.com/chrislusf/seaweedfs/weed/util"
)

//检查卷数据的完整性
func CheckVolumeDataIntegrity(v *Volume, indexFile *os.File) error {
	var indexSize int64
	var e error
	//校验索引文件的完整性
	if indexSize, e = verifyIndexFileIntegrity(indexFile); e != nil {
		return fmt.Errorf("verifyIndexFileIntegrity %s failed: %v", indexFile.Name(), e)
	}
	//如果索引文件的大小为空
	if indexSize == 0 {
		return nil
	}
	var lastIdxEntry []byte
	//获取最后一个实体的数据
	if lastIdxEntry, e = readIndexEntryAtOffset(indexFile, indexSize-NeedleIndexSize); e != nil {
		return fmt.Errorf("readLastIndexEntry %s failed: %v", indexFile.Name(), e)
	}
	key, offset, size := idxFileEntry(lastIdxEntry)
	//deleted index entry could not point to deleted needle
	//被删除的查找不到
	if offset == 0 {
		return nil
	}
	//校验针文件的一致性
	if e = verifyNeedleIntegrity(v.dataFile, v.Version(), int64(offset)*NeedlePaddingSize, key, size); e != nil {
		return fmt.Errorf("verifyNeedleIntegrity %s failed: %v", indexFile.Name(), e)
	}
	return nil
}

//校验索引文件的一致性
func verifyIndexFileIntegrity(indexFile *os.File) (indexSize int64, err error) {
	//获取索引文件的大小
	if indexSize, err = util.GetFileSize(indexFile); err == nil {
		if indexSize%NeedleIndexSize != 0 { //索引文件的大小必须是针文件索引大小的整数倍
			err = fmt.Errorf("index file's size is %d bytes, maybe corrupted", indexSize)
		}
	}
	return
}

//在指定offset，读取文件中的数据
func readIndexEntryAtOffset(indexFile *os.File, offset int64) (bytes []byte, err error) {
	if offset < 0 { //如果offset小于0，报错
		err = fmt.Errorf("offset %d for index file is invalid", offset)
		return
	}
	//16字节的针大小
	bytes = make([]byte, NeedleIndexSize)
	//在指定文件的指定位置，读取数据
	_, err = indexFile.ReadAt(bytes, offset)
	return
}

//校验文件一致性
func verifyNeedleIntegrity(datFile *os.File, v Version, offset int64, key uint64, size uint32) error {
	n := new(Needle) //申请针文件的内存

	err := n.ReadData(datFile, offset, size, v) //读取文件
	if err != nil {
		return err
	}
	if n.Id != key { //如果id和key不一致，报错
		return fmt.Errorf("index key %#x does not match needle's Id %#x", key, n.Id)
	}
	return nil
}
