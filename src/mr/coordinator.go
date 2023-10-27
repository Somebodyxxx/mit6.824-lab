package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	inputfiles_             []string
	sum_of_map_routines_    int
	map_routines_state_     []int //0,未分配；1，正在进行；2：已完成
	reduce_routines_state_  []int
	NReduce                 int
	finish_map_routines_    int
	intermediate_files_     []string
	cur_map_id              int
	cur_reduce_id           int
	finish_reduce_routines_ int
	mu                      sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Distribute(args *Job, reply *Job) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.inputfiles_) > 0 {
		reply.JobType = 1 // Map
		reply.MapInputFiles = append(reply.MapInputFiles, c.inputfiles_[0])
		reply.MapId = c.cur_map_id
		c.map_routines_state_[c.cur_map_id] = 1
		c.cur_map_id = 1
		for (c.cur_map_id <= c.sum_of_map_routines_) && (c.map_routines_state_[c.cur_map_id] != 0) {
			c.cur_map_id++
		}
		reply.NReduce = c.NReduce
		c.inputfiles_ = c.inputfiles_[1:]
		go c.CheckJob(reply)
		return nil
	}
	if c.finish_map_routines_ == c.sum_of_map_routines_ {
		if c.cur_reduce_id > c.NReduce {
			reply.JobType = 3 //finish
			return nil
		}
		reply.JobType = 2 // Reduce
		reply.MapInputFiles = append(reply.MapInputFiles, c.intermediate_files_...)
		reply.SumMapId = c.sum_of_map_routines_
		reply.ReduceId = c.cur_reduce_id
		c.reduce_routines_state_[c.cur_reduce_id] = 1
		for (c.cur_reduce_id <= c.NReduce) && (c.reduce_routines_state_[c.cur_reduce_id] != 0) {
			c.cur_reduce_id++
		}
		go c.CheckJob(reply)
		return nil
	}
	reply.JobType = 0 //wait Map finish
	return nil
}

func (c *Coordinator) FinishJob(args *Job, reply *Job) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.JobType == 1 {
		c.finish_map_routines_++
		c.map_routines_state_[args.MapId] = 2
	}
	if args.JobType == 2 {
		c.finish_reduce_routines_++
		c.reduce_routines_state_[args.ReduceId] = 2
	}
	return nil
}

func (c *Coordinator) CheckJob(job *Job) {
	time.Sleep(time.Second * 10)
	c.mu.Lock()
	defer c.mu.Unlock()
	if job.JobType == 1 && c.map_routines_state_[job.MapId] != 2 {
		//map
		c.inputfiles_ = append(job.MapInputFiles[:1], c.inputfiles_...)
		c.cur_map_id = job.MapId
	}
	if job.JobType == 2 && c.reduce_routines_state_[job.ReduceId] != 2 {
		//reduce
		c.cur_reduce_id = job.ReduceId
	}
	// ReDistribute
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finish_reduce_routines_ == c.NReduce {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.inputfiles_ = append(c.inputfiles_, files...)
	c.sum_of_map_routines_ = len(c.inputfiles_)
	c.NReduce = nReduce
	c.finish_map_routines_ = 0
	c.cur_map_id = 1
	c.cur_reduce_id = 1
	c.finish_reduce_routines_ = 0

	c.map_routines_state_ = make([]int, c.sum_of_map_routines_+1) //1-n
	c.reduce_routines_state_ = make([]int, nReduce+1)

	c.server()
	return &c
}
