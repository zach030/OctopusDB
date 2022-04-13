package server

import (
	"github.com/gin-gonic/gin"
)

type HttpServer struct {
	Engine *gin.Engine
}

func NewHttpServer() *HttpServer {
	engine := gin.Default()
	return &HttpServer{Engine: engine}
}

func (s *HttpServer) Start() {
	s.Engine.Run(":8000")
}
