package storage

//定义version 一个字节
type Version uint8

const (
	//常量version1
	Version1 = Version(1)
	//常量version2
	Version2 = Version(2)
	//当前version
	CurrentVersion = Version2
)
