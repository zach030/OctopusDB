package server

import "github.com/zach030/OctopusDB/storage"

type OctopusServer struct {
	storage storage.Storage
}

func NewServer(storage storage.Storage) *OctopusServer {
	return &OctopusServer{storage: storage}
}
