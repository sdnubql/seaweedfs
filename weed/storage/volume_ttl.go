package storage

import (
	"strconv"
)

const (
	//定义常量
	//stored unit types
	//空为0
	Empty byte = iota
	//分钟
	Minute
	//小时
	Hour
	//日期
	Day
	//星期
	Week
	//月
	Month
	//年
	Year
)

//tts的结构
type TTL struct {
	count byte //数量
	unit  byte //单位
}

var EMPTY_TTL = &TTL{}

// translate a readable ttl to internal ttl
// Supports format example:
// 3m: 3 minutes
// 4h: 4 hours
// 5d: 5 days
// 6w: 6 weeks
// 7M: 7 months
// 8y: 8 years
//将可读的ttl转换成内部结构
func ReadTTL(ttlString string) (*TTL, error) {
	if ttlString == "" { //如果可读的ttl字符串为空，直接返回空
		return EMPTY_TTL, nil
	}
	//专成byte
	ttlBytes := []byte(ttlString)
	//最后一位作为单位
	unitByte := ttlBytes[len(ttlBytes)-1]
	//前n－1位作为数值
	countBytes := ttlBytes[0 : len(ttlBytes)-1]
	//判断单位大小，强转
	if '0' <= unitByte && unitByte <= '9' {
		countBytes = ttlBytes
		unitByte = 'm'
	}
	//强转大小
	count, err := strconv.Atoi(string(countBytes))
	//单位
	unit := toStoredByte(unitByte)
	return &TTL{count: byte(count), unit: unit}, err
}

// read stored bytes to a ttl
//设置ttl内部存储结构
func LoadTTLFromBytes(input []byte) (t *TTL) {
	return &TTL{count: input[0], unit: input[1]}
}

// read stored bytes to a ttl
//通过一个32位数获取ttl
func LoadTTLFromUint32(ttl uint32) (t *TTL) {
	input := make([]byte, 2)
	input[1] = byte(ttl)
	input[0] = byte(ttl >> 8)
	return LoadTTLFromBytes(input)
}

// save stored bytes to an output with 2 bytes
func (t *TTL) ToBytes(output []byte) {
	output[0] = t.count
	output[1] = t.unit
}

//过期时间转32位存储
func (t *TTL) ToUint32() (output uint32) {
	//count 挪8位，留给单位
	output = uint32(t.count) << 8
	output += uint32(t.unit)
	return output
}

//过期时间的打印函数,将内部存储的数据转换为可视的数据,为空时返回空字符串
func (t *TTL) String() string {
	if t == nil || t.count == 0 {
		return ""
	}
	if t.unit == Empty {
		return ""
	}
	countString := strconv.Itoa(int(t.count))
	switch t.unit {
	case Minute:
		return countString + "m"
	case Hour:
		return countString + "h"
	case Day:
		return countString + "d"
	case Week:
		return countString + "w"
	case Month:
		return countString + "M"
	case Year:
		return countString + "y"
	}
	return ""
}

//将可读的byte转换成实际的存储内容
func toStoredByte(readableUnitByte byte) byte {
	switch readableUnitByte {
	case 'm':
		return Minute
	case 'h':
		return Hour
	case 'd':
		return Day
	case 'w':
		return Week
	case 'M':
		return Month
	case 'y': //y代表year
		return Year
	}
	return 0
}

//获取过期时间的分钟表示
func (t TTL) Minutes() uint32 {
	//根据单位进行处理
	switch t.unit {
	case Empty: //如果为空
		return 0 //返回0
	case Minute: //如果是分钟
		return uint32(t.count) //返回分钟count值
	case Hour: //如果是小时
		return uint32(t.count) * 60 //小时x60
	case Day:
		return uint32(t.count) * 60 * 24 //如果是天, 转换成分钟
	case Week:
		return uint32(t.count) * 60 * 24 * 7 //周转分钟
	case Month:
		return uint32(t.count) * 60 * 24 * 31 //月转分钟(固定31天)
	case Year:
		return uint32(t.count) * 60 * 24 * 365 //年转分钟(固定365天)
	}
	return 0
}
