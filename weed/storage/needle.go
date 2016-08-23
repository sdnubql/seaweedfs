package storage

import (
	"fmt"
	"io/ioutil"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/images"
	"github.com/chrislusf/seaweedfs/weed/operation"
)

const (
	NeedleHeaderSize      = 16                         //should never change this//针的头文件
	NeedlePaddingSize     = 8                          //针的宽大小
	NeedleChecksumSize    = 4                          //针的checksum的大小
	MaxPossibleVolumeSize = 4 * 1024 * 1024 * 1024 * 8 //最大可能的卷大小
)

/*
* A Needle means a uploaded and stored file.
* Needle file size is limited to 4GB for now.
* 针结构是代表上传并存储的文件
* 针文件的大小必须小于4g
 */
//针文件的结构
type Needle struct {
	Cookie uint32 `comment:"random number to mitigate brute force lookups"`    //cookie值，随机值
	Id     uint64 `comment:"needle id"`                                        //针文件的id
	Size   uint32 `comment:"sum of DataSize,Data,NameSize,Name,MimeSize,Mime"` //文件的大小，包括(DataSize,Data,NameSize,Name,MimeSize,Mime)

	DataSize     uint32 `comment:"Data size"`            //version2 //数据的大小
	Data         []byte `comment:"The actual file data"` //数据
	Flags        byte   `comment:"boolean flags"`        //version2
	NameSize     uint8  //version2 	//名字的大小
	Name         []byte `comment:"maximum 256 characters"` //version2 //名字
	MimeSize     uint8  //version2 //mime的大小
	Mime         []byte `comment:"maximum 256 characters"` //version2 //mime
	LastModified uint64 //only store LastModifiedBytesLength bytes, which is 5 bytes to disk //最后被修改的时间
	Ttl          *TTL   //过期时间

	Checksum CRC    `comment:"CRC32 to check integrity"` //一致性校验码
	Padding  []byte `comment:"Aligned to 8 bytes"`       //补充

	rawBlock *Block // underlying supporing []byte, fetched and released into a pool //原始块
}

//Needle实现String方法,便于打印
func (n *Needle) String() (str string) {
	str = fmt.Sprintf("Cookie:%d, Id:%d, Size:%d, DataSize:%d, Name: %s, Mime: %s", n.Cookie, n.Id, n.Size, n.DataSize, n.Name, n.Mime)
	return
}

//解析上传的文件
func ParseUpload(r *http.Request) (
	fileName string, data []byte, mimeType string, isGzipped bool,
	modifiedTime uint64, ttl *TTL, isChunkedFile bool, e error) {
	//解析MultipartReader头
	form, fe := r.MultipartReader()
	//如果报错,打log返回
	if fe != nil {
		glog.V(0).Infoln("MultipartReader [ERROR]", fe)
		e = fe
		return
	}

	//first multi-part item
	//解析
	part, fe := form.NextPart()

	if fe != nil {
		glog.V(0).Infoln("Reading Multi part [ERROR]", fe)
		e = fe
		return
	}

	//解析文件名
	fileName = part.FileName()
	//如果文件名不为空,解析目录
	if fileName != "" {
		fileName = path.Base(fileName)
	}

	//获取数据
	data, e = ioutil.ReadAll(part)
	if e != nil {
		glog.V(0).Infoln("Reading Content [ERROR]", e)
		return
	}

	//if the filename is empty string, do a search on the other multi-part items
	//循环解析，如果文件名为空,接着解析,直到不为空
	for fileName == "" {
		part2, fe := form.NextPart()
		//如果报错，直接跳出
		if fe != nil {
			break // no more or on error, just safely break
		}
		//解析名称
		fName := part2.FileName()

		//found the first <file type> multi-part has filename
		//如果文件名不为空
		if fName != "" {
			//读取内容
			data2, fe2 := ioutil.ReadAll(part2)
			if fe2 != nil {
				glog.V(0).Infoln("Reading Content [ERROR]", fe2)
				e = fe2
				return
			}

			//update
			//修改内容，和解析名称
			data = data2
			fileName = path.Base(fName)
			break
		}
	}
	//对文件名进行解析
	dotIndex := strings.LastIndex(fileName, ".")
	ext, mtype := "", ""
	if dotIndex > 0 {
		//截取文件后缀
		ext = strings.ToLower(fileName[dotIndex:])
		mtype = mime.TypeByExtension(ext)
	}
	//解析header头里面的content-type
	contentType := part.Header.Get("Content-Type")
	if contentType != "" && mtype != contentType {
		mimeType = contentType //only return mime type if not deductable
		mtype = contentType
	}
	//解析header头，Content-Encoding，看是否被压缩
	if part.Header.Get("Content-Encoding") == "gzip" {
		isGzipped = true
	} else if operation.IsGzippable(ext, mtype) {
		if data, e = operation.GzipData(data); e != nil {
			return
		}
		isGzipped = true
	}
	//如果文件后缀位.gz认为是压缩
	if ext == ".gz" {
		isGzipped = true
	}
	//解析文件名
	if strings.HasSuffix(fileName, ".gz") &&
		!strings.HasSuffix(fileName, ".tar.gz") {
		fileName = fileName[:len(fileName)-3]
	}
	//解析ts
	modifiedTime, _ = strconv.ParseUint(r.FormValue("ts"), 10, 64)
	//解析过期时间
	ttl, _ = ReadTTL(r.FormValue("ttl"))
	//解析cm
	isChunkedFile, _ = strconv.ParseBool(r.FormValue("cm"))
	return
}

//针文件的构造函数
func NewNeedle(r *http.Request, fixJpgOrientation bool) (n *Needle, e error) {
	//声明变量
	fname, mimeType, isGzipped, isChunkedFile := "", "", false, false
	//申请内存
	n = new(Needle)
	//解析上传的文件
	fname, n.Data, mimeType, isGzipped, n.LastModified, n.Ttl, isChunkedFile, e = ParseUpload(r)
	if e != nil {
		return
	}
	//如果文件名称小于256
	if len(fname) < 256 {
		n.Name = []byte(fname)
		n.SetHasName()
	}
	//如果mimeType小于256
	if len(mimeType) < 256 {
		n.Mime = []byte(mimeType)
		n.SetHasMime()
	}
	//是否压缩
	if isGzipped {
		n.SetGzipped()
	}
	//最后修改时间
	if n.LastModified == 0 {
		n.LastModified = uint64(time.Now().Unix())
	}
	n.SetHasLastModifiedDate()
	//设置过期时间
	if n.Ttl != EMPTY_TTL {
		n.SetHasTtl()
	}

	if isChunkedFile {
		n.SetIsChunkManifest()
	}

	if fixJpgOrientation {
		loweredName := strings.ToLower(fname)
		if mimeType == "image/jpeg" || strings.HasSuffix(loweredName, ".jpg") || strings.HasSuffix(loweredName, ".jpeg") {
			n.Data = images.FixJpgOrientation(n.Data)
		}
	}

	//校验码
	n.Checksum = NewCRC(n.Data)
	//文件名的解析
	commaSep := strings.LastIndex(r.URL.Path, ",")
	dotSep := strings.LastIndex(r.URL.Path, ".")
	fid := r.URL.Path[commaSep+1:]
	if dotSep > 0 {
		fid = r.URL.Path[commaSep+1 : dotSep]
	}

	e = n.ParsePath(fid)

	return
}

//解析路径
func (n *Needle) ParsePath(fid string) (err error) {
	//先获取fid的长度
	length := len(fid)
	//如果fid长度小于8，报错
	if length <= 8 {
		return fmt.Errorf("Invalid fid: %s", fid)
	}
	delta := ""
	//查找是否有_
	deltaIndex := strings.LastIndex(fid, "_")
	//如果存在_,进行截取,前面的部分作为fid,后面的部分作为delta
	if deltaIndex > 0 {
		fid, delta = fid[0:deltaIndex], fid[deltaIndex+1:]
	}
	//解析id和cookie
	n.Id, n.Cookie, err = ParseKeyHash(fid)
	if err != nil {
		return err
	}
	//如果delta，不为0，将针的id在加上delta,但是直接加不会造成id重复吗？？？？？？？？？？？？？？？
	if delta != "" {
		if d, e := strconv.ParseUint(delta, 10, 64); e == nil {
			n.Id += d
		} else {
			return e
		}
	}
	return err
}

//文件key和hash的解析函数
func ParseKeyHash(key_hash_string string) (uint64, uint32, error) {
	//长度小于8，报错
	if len(key_hash_string) <= 8 {
		return 0, 0, fmt.Errorf("KeyHash is too short.")
	}
	//长度大于24报错
	if len(key_hash_string) > 24 {
		return 0, 0, fmt.Errorf("KeyHash is too long.")
	}
	//获取分割点
	split := len(key_hash_string) - 8
	//解析key，长度减去固定的8
	key, err := strconv.ParseUint(key_hash_string[:split], 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("Parse key error: %v", err)
	}
	//hash占固定的末尾8位
	hash, err := strconv.ParseUint(key_hash_string[split:], 16, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("Parse hash error: %v", err)
	}
	return key, uint32(hash), nil
}
