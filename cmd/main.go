package main

import (
	"github.com/zach030/OctopusDB/cluster"
	"github.com/zach030/OctopusDB/server"
)

func main() {
	cluster.Start()
	http := server.NewHttpServer()
	go func() {
		http.Start()
	}()
}
