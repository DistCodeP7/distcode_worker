package api

import (
	"github.com/DistCodeP7/distcode_worker/worker"

	"github.com/gin-gonic/gin"
)

type Server struct {
	router *gin.Engine;
	jobDispatcher *worker.JobDispatcher
}

func (s *Server) getAllWorkers(c *gin.Context) {
	// Placeholder implementation
	c.JSON(200, gin.H{
		"workers": s.jobDispatcher.WorkerManager().ListWorkers(),
	})
}

func (s *Server)getIdleWorkers(c *gin.Context) {
	// Placeholder implementation
	c.JSON(200, gin.H{
		"idle_workers": []string{"worker1"},
	})
}

func StartHttpServer(jd *worker.JobDispatcher) *Server {
	r := gin.Default()
	s := &Server{
		router:       r,
		jobDispatcher: jd,
	}
	s.registerRoutes()
	
	go func() {
		if err := s.router.Run(":8080"); err != nil {
			panic(err)
		}
	}()
	return s
}

func (s *Server) registerRoutes() {
	// Grouping routes by resource
	workerGroup := s.router.Group("/workers")
	{
		workerGroup.GET("/", s.getAllWorkers)
		workerGroup.GET("/idle", s.getIdleWorkers)
	}
}