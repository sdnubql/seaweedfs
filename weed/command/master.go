package command

import (
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/server"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/gorilla/mux"
)

//init函数初始化run方法,启动时调用的方法
func init() {
	cmdMaster.Run = runMaster // break init cycle
}

//master命令的定义
var cmdMaster = &Command{
	//具体的usageline,第一个world是实际的命令
	UsageLine: "master -port=9333",
	//weed help是的说明
	Short: "start a master server",
	//weed help master的说明
	Long: `start a master server to provide volume=>location mapping service
  and sequence number of file ids

  `,
}

//master 可以跟的命令行参数解析
var (
	//端口号解析
	mport = cmdMaster.Flag.Int("port", 9333, "http listen port")
	//主ip
	masterIp     = cmdMaster.Flag.String("ip", "localhost", "master <ip>|<server> address")
	masterBindIp = cmdMaster.Flag.String("ip.bind", "0.0.0.0", "ip address to bind to")
	//存储元数据的目录
	metaFolder  = cmdMaster.Flag.String("mdir", os.TempDir(), "data directory to store meta data")
	masterPeers = cmdMaster.Flag.String("peers", "", "other master nodes in comma separated ip:port list, example: 127.0.0.1:9093,127.0.0.1:9094")
	//存储卷的大小 30G
	volumeSizeLimitMB = cmdMaster.Flag.Uint("volumeSizeLimitMB", 30*1000, "Master stops directing writes to oversized volumes.")
	//心跳间隔
	mpulse = cmdMaster.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	//配置文件
	confFile                = cmdMaster.Flag.String("conf", "/etc/weedfs/weedfs.conf", "Deprecating! xml configuration file")
	defaultReplicaPlacement = cmdMaster.Flag.String("defaultReplication", "000", "Default replication type if not specified.")
	//连接idle的时间
	mTimeout = cmdMaster.Flag.Int("idleTimeout", 10, "connection idle seconds")
	//最大cpu数
	mMaxCpu               = cmdMaster.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")
	garbageThreshold      = cmdMaster.Flag.String("garbageThreshold", "0.3", "threshold to vacuum and reclaim spaces")
	masterWhiteListOption = cmdMaster.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
	masterSecureKey       = cmdMaster.Flag.String("secure.secret", "", "secret to encrypt Json Web Token(JWT)")
	masterCpuProfile      = cmdMaster.Flag.String("cpuprofile", "", "cpu profile output file")

	masterWhiteList []string
)

//master 命令的run函数
func runMaster(cmd *Command, args []string) bool {
	//如果设置的最大cpu数小于1，直接取cpu数量
	if *mMaxCpu < 1 {
		*mMaxCpu = runtime.NumCPU()
	}
	//设置环境变量runtime.GOMAXPROCS
	runtime.GOMAXPROCS(*mMaxCpu)
	//如果命令行参数cpuprofile不为空时，创建它
	if *masterCpuProfile != "" {
		//创建cpuprofile文件
		f, err := os.Create(*masterCpuProfile)
		//创建不成功，报fatal
		if err != nil {
			glog.Fatal(err)
		}
		//性能监控当前进程的cpu，写到cpuprofile
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		OnInterrupt(func() {
			pprof.StopCPUProfile()
		})
	}
	//如果写元数据的目录不可写，报错
	if err := util.TestFolderWritable(*metaFolder); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir) Writable %s : %s", *metaFolder, err)
	}
	//如果命令行参数配置了白名单，切割并把他们赋值给masterWhiteList
	if *masterWhiteListOption != "" {
		//切割并赋值
		masterWhiteList = strings.Split(*masterWhiteListOption, ",")
	}

	r := mux.NewRouter()
	ms := weed_server.NewMasterServer(r, *mport, *metaFolder,
		*volumeSizeLimitMB, *mpulse, *confFile, *defaultReplicaPlacement, *garbageThreshold,
		masterWhiteList, *masterSecureKey,
	)

	listeningAddress := *masterBindIp + ":" + strconv.Itoa(*mport)

	glog.V(0).Infoln("Start Seaweed Master", util.VERSION, "at", listeningAddress)

	listener, e := util.NewListener(listeningAddress, time.Duration(*mTimeout)*time.Second)
	if e != nil {
		glog.Fatalf("Master startup error: %v", e)
	}

	go func() {
		time.Sleep(100 * time.Millisecond)
		myMasterAddress := *masterIp + ":" + strconv.Itoa(*mport)
		var peers []string
		if *masterPeers != "" {
			peers = strings.Split(*masterPeers, ",")
		}
		raftServer := weed_server.NewRaftServer(r, peers, myMasterAddress, *metaFolder, ms.Topo, *mpulse)
		ms.SetRaftServer(raftServer)
	}()

	if e := http.Serve(listener, r); e != nil {
		glog.Fatalf("Fail to serve: %v", e)
	}
	return true
}
