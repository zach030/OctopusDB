package cluster

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/zach030/OctopusDB/cluster/fsm"
	"github.com/zach030/OctopusDB/internal/conf"
	"google.golang.org/protobuf/proto"
)

var node *raft.Raft

func Start() {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(conf.Conf.Cluster.NodeId)
	raftConfig.LogLevel = "Trace"
	log.Printf("raft config: %+v", raftConfig)

	path := filepath.Join(conf.Conf.Cluster.Path, string(raftConfig.LocalID))

	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		log.Fatalln("mkdir error", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(path, "raft-log.bolt"))
	if err != nil {
		log.Fatalf("new log store error, %+v", err)
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(path, "raft-stable.bolt"))
	if err != nil {
		log.Fatalf("new stable store error, %+v", err)
	}
	snapshotStore, err := raft.NewFileSnapshotStore(path, 20, os.Stderr)
	if err != nil {
		log.Fatalln("new snapshot store error", err)
	}

	address, err := net.ResolveTCPAddr("tcp", "127.0.0.1:"+strconv.Itoa(conf.Conf.Cluster.Port))
	if err != nil {
		log.Fatalln("resolve tcp addr error", err)
	}
	transport, err := raft.NewTCPTransport(address.String(), address, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Fatalln("new tcp transport addr error", err)
	}

	node, err = raft.NewRaft(raftConfig, fsm.NewFSM(), logStore, stableStore, snapshotStore, transport)
	if err != nil {
		log.Fatalln("new raft error", err)
	}

	if len(conf.Conf.Cluster.Master) == 0 {
		node.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		})
	} else {
		url := fmt.Sprintf("http://%s/join?nodeId=%s&joinAddress=%s", conf.Conf.Cluster.Master, raftConfig.LocalID, transport.LocalAddr())
		resp, err := http.Get(url)
		if err != nil {
			log.Fatalf("join cluster error: %+v", err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatalf("read body error: %+v", err)
		}
		log.Printf("join response: %s", body)
		if string(body) != "ok" {
			log.Fatalf("join cluster error: %s", body)
		}
	}
}

func Apply(methodName string, request proto.Message) error {
	if node.State() != raft.Leader {
		return errors.New("only can apply from leader")
	}
	requestBytes, err := proto.Marshal(request)
	if err != nil {
		return err
	}
	entry := &pb.LogEntry{MethodName: methodName, RequestBytes: requestBytes}
	data, err := proto.Marshal(entry)
	if err != nil {
		return err
	}

	future := node.Apply(data, time.Duration(conf.Conf.Cluster.Timeout)*time.Millisecond)
	if err := future.Error(); err != nil {
		log.Printf("apply error: %+v", err)
		return err
	}
	resp := future.Response()
	if resp == nil {
		return nil
	}
	err = resp.(error)
	log.Printf("response error: %+v", err)
	return err
}

func Join(nodeId, addr string) error {
	log.Printf("received join request for remote node %s at %s", nodeId, addr)

	configFuture := node.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.Println("failed to get raft configuration", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeId) || srv.Address == raft.ServerAddress(addr) {
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeId) {
				log.Printf("node %s at %s already member of cluster, ignoring join request", nodeId, addr)
				return nil
			}

			future := node.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeId, addr, err)
			}
		}
	}
	log.Println("Coming to add voter")
	f := node.AddVoter(raft.ServerID(nodeId), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		log.Println(f.Error())
		return f.Error()
	}
	log.Printf("node %s at %s joined successfully", nodeId, addr)
	return nil
}
