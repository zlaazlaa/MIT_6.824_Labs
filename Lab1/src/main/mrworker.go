package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import "6.824/mr"
import "plugin"
import "os"
import "fmt"
import "log"

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		//os.Exit(1)
	}
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("plugin had been opened")
		}
	}()

	//mapf, reducef := loadPlugin(os.Args[1])
	mapf, reducef := loadPlugin("wc.so")
	mr.Worker(mapf, reducef)
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	//filename = "./" + filename
	filename = "/home/zlaa123456/6.824/src/main/wc.so"
	fmt.Printf("filename = %s\n", filename)
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("加载插件失败：", err)
		}
	}()
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v :%v", filename, err)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		//panic(err)
		log.Fatalf("cannot find Map in %v :%v", filename, err)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
