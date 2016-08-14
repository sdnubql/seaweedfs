package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"text/template"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/chrislusf/seaweedfs/weed/command"
	"github.com/chrislusf/seaweedfs/weed/glog"
)

var IsDebug *bool
var server *string

//命令列表
var commands = command.Commands

//成功时的退出状态
var exitStatus = 0

//设置退出状态时的锁
var exitMu sync.Mutex

//设置退出状态
func setExitStatus(n int) {
	//加锁
	exitMu.Lock()
	//判断状态并设置
	if exitStatus < n {
		exitStatus = n
	}
	//去掉锁
	exitMu.Unlock()
}

func main() {
	//定义日志的大小 32M
	glog.MaxSize = 1024 * 1024 * 32
	//设置随机数种子
	rand.Seed(time.Now().UnixNano())
	//设置参数解析错误时，往标准错误打印的使用说明
	flag.Usage = usage
	//对参数进行解析
	flag.Parse()

	//获取所以的 non-flag参数
	args := flag.Args()
	//如果参数长度小于1，显示使用说明,其他什么也不做
	if len(args) < 1 {
		usage()
	}

	//具体的help
	if args[0] == "help" {
		help(args[1:])
		//遍历命令列表,如果能找到匹配的命令，打印命令的默认值
		for _, cmd := range commands {
			//len(args) >= 2实际上只能取到==2 ，因为在help方法里面已经做了参数长度的判断
			if len(args) >= 2 && cmd.Name() == args[1] && cmd.Run != nil {
				fmt.Fprintf(os.Stderr, "Default Parameters:\n")
				cmd.Flag.PrintDefaults()
			}
		}
		return
	}

	//遍历命令列表
	for _, cmd := range commands {
		//如果命令名称和参数一致，并且命令的Run不为nil，则进行具体的设置，运行
		if cmd.Name() == args[0] && cmd.Run != nil {
			//设置具体命令的usage
			cmd.Flag.Usage = func() { cmd.Usage() }
			//解析参数
			cmd.Flag.Parse(args[1:])
			args = cmd.Flag.Args()
			IsDebug = cmd.IsDebug
			//具体的执行命令，如果命令执行失败报错
			if !cmd.Run(cmd, args) {
				fmt.Fprintf(os.Stderr, "\n")
				//打印此命令的使用说明
				cmd.Flag.Usage()
				//往标准错误打印说明
				fmt.Fprintf(os.Stderr, "Default Parameters:\n")
				//打印此命令的默认值
				cmd.Flag.PrintDefaults()
			}
			//调用退出前需要执行的函数和设置退出的状态码
			exit()
			return
		}
	}

	fmt.Fprintf(os.Stderr, "weed: unknown subcommand %q\nRun 'weed help' for usage.\n", args[0])
	setExitStatus(2)
	exit()
}

var usageTemplate = `
SeaweedFS: store billions of files and serve them fast!

Usage:

	weed command [arguments]

The commands are:
{{range .}}{{if .Runnable}}
    {{.Name | printf "%-11s"}} {{.Short}}{{end}}{{end}}

Use "weed help [command]" for more information about a command.

`

var helpTemplate = `{{if .Runnable}}Usage: weed {{.UsageLine}}
{{end}}
  {{.Long}}
`

// tmpl executes the given template text on data, writing the result to w.
func tmpl(w io.Writer, text string, data interface{}) {
	t := template.New("top")
	t.Funcs(template.FuncMap{"trim": strings.TrimSpace, "capitalize": capitalize})
	template.Must(t.Parse(text))
	if err := t.Execute(w, data); err != nil {
		panic(err)
	}
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToTitle(r)) + s[n:]
}

func printUsage(w io.Writer) {
	tmpl(w, usageTemplate, commands)
}

//使用说明具体的函数
func usage() {
	printUsage(os.Stderr)
	//往标准错误打印说明内容
	fmt.Fprintf(os.Stderr, "For Logging, use \"weed [logging_options] [command]\". The logging options are:\n")
	//打印已定义参数的默认值
	flag.PrintDefaults()
	//退出,设置状态为2
	os.Exit(2)
}

// help implements the 'help' command.
//具体的weed help方法
func help(args []string) {
	//如果help 后的参数为0，直接打印usage,然后退出
	if len(args) == 0 {
		printUsage(os.Stdout)
		// not exit 2: succeeded at 'weed help'.
		return
	}
	//如果help后面的参数多于一个，显示错误
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "usage: weed help command\n\nToo many arguments given.\n")
		os.Exit(2) // failed at 'weed help'
	}

	arg := args[0]

	//遍历命令列表，如果参数和列表中的项匹配，显示具体的命令help
	for _, cmd := range commands {
		if cmd.Name() == arg {
			tmpl(os.Stdout, helpTemplate, cmd)
			// not exit 2: succeeded at 'weed help cmd'.
			return
		}
	}

	//help的命令 在命令列表里面找不到具体的命令时报错
	fmt.Fprintf(os.Stderr, "Unknown help topic %#q.  Run 'weed help'.\n", arg)
	os.Exit(2) // failed at 'weed help cmd'
}

//退出前需要执行的函数的slice
var atexitFuncs []func()

//添加需要退出前执行的函数
func atexit(f func()) {
	atexitFuncs = append(atexitFuncs, f)
}

//设置退出状态,如果定义了执行结束前回调的函数，执行函数
func exit() {
	//遍历退出前需要执行的函数的slice，并执行
	for _, f := range atexitFuncs {
		f()
	}
	//设置退出状态
	os.Exit(exitStatus)
}

func debug(params ...interface{}) {
	glog.V(4).Infoln(params)
}
