package command

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

//支持的命令列表
var Commands = []*Command{
	cmdBenchmark,
	cmdBackup,
	cmdCompact,
	cmdCopy,
	cmdFix,
	cmdServer,
	cmdMaster,
	cmdFiler,
	cmdUpload,
	cmdDownload,
	cmdShell,
	cmdVersion,
	cmdVolume,
	cmdExport,
	cmdMount,
}

//命令的主结构
type Command struct {
	// Run runs the command.
	// The args are the arguments after the command name.
	//执行的具体方法
	Run func(cmd *Command, args []string) bool

	// UsageLine is the one-line usage message.
	// The first word in the line is taken to be the command name.
	//一行的使用说明第一个word是命令的名称，跟后面的用空格隔开
	UsageLine string

	// Short is the short description shown in the 'go help' output.
	//在go help中的简介
	Short string

	// Long is the long message shown in the 'go help <this-command>' output.
	//在go help 具体命令 时的显示说明
	Long string

	// Flag is a set of flags specific to this command.
	//这个命令需要的参数
	Flag flag.FlagSet

	//是否是调试模式
	IsDebug *bool
}

// Name returns the command's name: the first word in the usage line.
//获取命令的名称,因为命令的名称在usageline中
func (c *Command) Name() string {
	//获取usageline
	name := c.UsageLine
	//找到第一个空格的索引
	i := strings.Index(name, " ")
	//如果索引值> 0 ，直接截取字符串,得到名称
	if i >= 0 {
		name = name[:i]
	}
	return name
}

//具体命令的使用说明
func (c *Command) Usage() {
	fmt.Fprintf(os.Stderr, "Example: weed %s\n", c.UsageLine)
	fmt.Fprintf(os.Stderr, "Default Usage:\n")
	c.Flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "Description:\n")
	fmt.Fprintf(os.Stderr, "  %s\n", strings.TrimSpace(c.Long))
	os.Exit(2)
}

// Runnable reports whether the command can be run; otherwise
// it is a documentation pseudo-command such as importpath.
//判断这个命令是否可以执行,其实就是判断结构体里面的Run方法是否设置
func (c *Command) Runnable() bool {
	return c.Run != nil
}
