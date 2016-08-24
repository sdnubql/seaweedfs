package storage

import (
	"strconv"
	"sync"
)

//针值的结构
type NeedleValue struct {
	Key    Key    //key
	Offset uint32 `comment:"Volume offset"`            //since aligned to 8 bytes, range is 4G*8=32G //起点
	Size   uint32 `comment:"Size of the data portion"` //大小
}

const (
	batch = 100000
)

//定义key类型
type Key uint64

//key的打印函数,按照10进制转换成字符串
func (k Key) String() string {
	return strconv.FormatUint(uint64(k), 10)
}

//CompactSection的结构
type CompactSection struct {
	sync.RWMutex                     //锁
	values       []NeedleValue       //value
	overflow     map[Key]NeedleValue //溢出
	start        Key                 //开始
	end          Key                 //结束
	counter      int                 //计数器
}

//CompactSection的构造函数
func NewCompactSection(start Key) *CompactSection {
	return &CompactSection{
		values:   make([]NeedleValue, batch),
		overflow: make(map[Key]NeedleValue),
		start:    start,
	}
}

//return old entry size
func (cs *CompactSection) Set(key Key, offset uint32, size uint32) uint32 {
	ret := uint32(0)
	//key跟end进行比较，看是否更新end
	if key > cs.end {
		cs.end = key
	}
	//加锁
	cs.Lock()
	//查找key
	if i := cs.binarySearchValues(key); i >= 0 {
		ret = cs.values[i].Size
		//println("key", key, "old size", ret)
		cs.values[i].Offset, cs.values[i].Size = offset, size
	} else { //key不存在
		//判断数量是否比预定义的还大
		needOverflow := cs.counter >= batch
		//加的这个,用两种结构，为啥不统一用hash呢
		needOverflow = needOverflow || cs.counter > 0 && cs.values[cs.counter-1].Key > key
		if needOverflow { //需要溢出时
			//println("start", cs.start, "counter", cs.counter, "key", key)
			//查找溢出表
			if oldValue, found := cs.overflow[key]; found {
				ret = oldValue.Size
			}
			//设置
			cs.overflow[key] = NeedleValue{Key: key, Offset: offset, Size: size}
		} else {
			//设置values的值
			p := &cs.values[cs.counter]
			p.Key, p.Offset, p.Size = key, offset, size
			//println("added index", cs.counter, "key", key, cs.values[cs.counter].Key)
			//把数量加1
			cs.counter++
		}
	}
	//解锁
	cs.Unlock()
	return ret
}

//return old entry size
//删除函数
func (cs *CompactSection) Delete(key Key) uint32 {
	//加锁
	cs.Lock()
	ret := uint32(0)
	//查找是否存在
	if i := cs.binarySearchValues(key); i >= 0 {
		//如果值存在
		if cs.values[i].Size > 0 {
			ret = cs.values[i].Size
			//把他的值设置为0
			cs.values[i].Size = 0
		}
	}
	//如果在溢出表里面，从溢出表中删除
	if v, found := cs.overflow[key]; found {
		delete(cs.overflow, key)
		ret = v.Size
	}
	//解锁
	cs.Unlock()
	return ret
}

//通过key获取
func (cs *CompactSection) Get(key Key) (*NeedleValue, bool) {
	//加锁
	cs.RLock()
	//如果在溢出表里面,直接返回
	if v, ok := cs.overflow[key]; ok {
		cs.RUnlock()
		return &v, true
	}
	//如果在valuesslice中，查找返回
	if i := cs.binarySearchValues(key); i >= 0 {
		cs.RUnlock()
		return &cs.values[i], true
	}
	cs.RUnlock()
	return nil, false
}

//通过key查找
func (cs *CompactSection) binarySearchValues(key Key) int {
	l, h := 0, cs.counter-1
	//给定key，比最后一个values的key还大，报错
	if h >= 0 && cs.values[h].Key < key {
		return -2
	}
	//println("looking for key", key)
	//通过循环2分查找
	for l <= h {
		m := (l + h) / 2
		//println("mid", m, "key", cs.values[m].Key, cs.values[m].Offset, cs.values[m].Size)
		if cs.values[m].Key < key {
			l = m + 1
		} else if key < cs.values[m].Key {
			h = m - 1
		} else {
			//println("found", m)
			return m
		}
	}
	return -1
}

//This map assumes mostly inserting increasing keys
//CompactMap 结构
type CompactMap struct {
	list []*CompactSection
}

//CompactMap构造函数
func NewCompactMap() CompactMap {
	return CompactMap{}
}

func (cm *CompactMap) Set(key Key, offset uint32, size uint32) uint32 {
	//查找key
	x := cm.binarySearchCompactSection(key)
	if x < 0 { //如果没有设置过
		//println(x, "creating", len(cm.list), "section, starting", key)
		//初始化并赋值
		cm.list = append(cm.list, NewCompactSection(key))
		x = len(cm.list) - 1
		//keep compact section sorted by start
		for x > 0 { //强制排序,需要，对上面返回值为-3的处理，就是挪顺序
			if cm.list[x-1].start > cm.list[x].start {
				cm.list[x-1], cm.list[x] = cm.list[x], cm.list[x-1]
				x = x - 1
			} else {
				break
			}
		}
	}
	//已经被设置过
	return cm.list[x].Set(key, offset, size)
}

//先查找，然后在调用删除方法删除
func (cm *CompactMap) Delete(key Key) uint32 {
	x := cm.binarySearchCompactSection(key)
	if x < 0 {
		return uint32(0)
	}
	return cm.list[x].Delete(key)
}

//按照key，去查找
func (cm *CompactMap) Get(key Key) (*NeedleValue, bool) {
	//先按照key找slice中的键值
	x := cm.binarySearchCompactSection(key)
	if x < 0 {
		return nil, false
	}
	//在按照cs中的方法去查找
	return cm.list[x].Get(key)
}

//根据key查找
func (cm *CompactMap) binarySearchCompactSection(key Key) int {
	l, h := 0, len(cm.list)-1
	//如果长度为空,报错
	if h < 0 {
		return -5
	}
	//如果最后一个的start，小于等于key
	if cm.list[h].start <= key {
		//如果最后一个的counter < batch 并且 key 小于等于最后一个的end,返回h
		if cm.list[h].counter < batch || key <= cm.list[h].end {
			return h
		}
		return -4
	}
	//遍历查找,二分法查找
	for l <= h {
		m := (l + h) / 2
		if key < cm.list[m].start {
			h = m - 1
		} else { // cm.list[m].start <= key
			if cm.list[m+1].start <= key {
				l = m + 1
			} else {
				return m
			}
		}
	}
	return -3
}

// Visit visits all entries or stop if any error when visiting
//Visit，将所有的NeedleValue，都调用一次func，如果期间遇到错误，终止
func (cm *CompactMap) Visit(visit func(NeedleValue) error) error {
	//循环list
	for _, cs := range cm.list {
		cs.RLock()
		//每一个CompactSection的overflow
		for _, v := range cs.overflow {
			//调用visit，看看是否报错
			if err := visit(v); err != nil {
				cs.RUnlock() //为啥没有defer呢，这一坨一坨的cs.RUnlock 不恶心吗？？？？？
				return err
			}
		}
		//循环values，如果没有在cs.overflow中出现过，在调用visit，为啥会一个key，在两个容器中都有呢？？
		for _, v := range cs.values {
			//判断是否在cs.overflow这个容器中
			if _, found := cs.overflow[v.Key]; !found {
				if err := visit(v); err != nil {
					cs.RUnlock()
					return err
				}
			}
		}
		cs.RUnlock()
	}
	return nil
}
