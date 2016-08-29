package main

/**
*读取索引文件的工具
 */

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/chrislusf/seaweedfs/weed/storage"
)

var (
	//解析命令行参数
	indexFileName = flag.String("file", "", ".idx file to analyze")
)

func main() {
	//进行解析
	flag.Parse()
	//打开文件名
	indexFile, err := os.OpenFile(*indexFileName, os.O_RDONLY, 0644)
	if err != nil {
		log.Fatalf("Create Volume Index [ERROR] %s\n", err)
	}
	defer indexFile.Close()

	//一行行的打印索引文件的信息
	storage.WalkIndexFile(indexFile, func(key uint64, offset, size uint32) error {
		fmt.Printf("key %d, offset %d, size %d, nextOffset %d\n", key, offset*8, size, offset*8+size)
		return nil
	})
}
