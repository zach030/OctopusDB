package conf

import (
	"flag"
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Store   Store   `yaml:"store"`
	Server  Server  `yaml:"server"`
	Cluster Cluster `yaml:"cluster"`
}

type Store struct {
	Engine string `yaml:"engine"`
	Path   string `yaml:"path"`
}

type Server struct {
	GRPCPort           int   `yaml:"grpc_port"`
	HttpPort           int   `yaml:"http_port"`
	Rate               int   `yaml:"rate"`
	SlowQueryThreshold int64 `yaml:"slow_query_threshold"`
}

type Cluster struct {
	NodeId  string `yaml:"node_id"`
	Path    string `yaml:"path"`
	Port    int    `yaml:"port"`
	Master  string `yaml:"master"`
	Timeout int    `yaml:"timeout"`
}

var Conf Config

func init() {
	file := flag.String("config", "configs/master.yml", "config")

	flag.Parse()

	bs, err := ioutil.ReadFile(*file)
	if err != nil {
		log.Fatalf("read file %s %+v ", *file, err)
	}
	err = yaml.Unmarshal(bs, &Conf)
	if err != nil {
		log.Fatalf("unmarshal: %+v", err)
	}

	log.Printf("conf: %+v", Conf)
}
