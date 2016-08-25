package stats

//磁盘状态
type DiskStatus struct {
	//目录
	Dir string
	//总量
	All uint64
	//使用量
	Used uint64
	//空闲的量
	Free uint64
}

//DiskStatus的构造函数
func NewDiskStatus(path string) (disk *DiskStatus) {
	disk = &DiskStatus{Dir: path}
	disk.fillInStatus()
	return
}
