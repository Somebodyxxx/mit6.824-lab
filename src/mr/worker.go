package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMapJob(job *Job, mapf func(string, string) []KeyValue) {
	//Read from source file
	filename := job.MapInputFiles[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	defer file.Close()
	intermediate := mapf(filename, string(content))

	// write to memory
	hash_intermediate := make([][]KeyValue, job.NReduce)
	for _, v := range intermediate {
		hash := ihash(v.Key)
		num_reduce := hash % job.NReduce
		hash_intermediate[num_reduce] = append(hash_intermediate[num_reduce], v)
	}

	//write to file
	for i := 0; i < job.NReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", job.MapId-1, i) //start from 0
		ofile, err := os.Create(oname)                   // default: if exits, override..
		if err != nil {
			log.Fatalf("cannot create %v", ofile)
		}
		// Based on lab description:
		// write by JSON
		enc := json.NewEncoder(ofile)
		for _, v := range hash_intermediate[i] {
			if err := enc.Encode(&v); err != nil {
				log.Fatalf("cannot Encode %v", ofile)
			}
		}
		defer ofile.Close()
	}
	args := Job{}
	args.JobType = 1
	args.MapId = job.MapId
	reply := Job{}
	call("Coordinator.FinishJob", &args, &reply)
}

func doReduceJob(job *Job, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for i := 1; i <= job.SumMapId; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i-1, job.ReduceId-1) // start from 0
		// fmt.Printf("ReduceId = %d , open  %v\n", job.ReduceId, filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		// data, err := ioutil.ReadAll(file)
		// if err != nil {
		// 	log.Fatalf("read file failed..")
		// }
		// defer file.Close()
		// // function to detect word separators.
		// ff := func(r rune) bool { return !unicode.IsLetter(r) }
		// // split contents into an array of words.
		// words := strings.FieldsFunc(string(data), ff)
		// // Only detect word!! no number!!!
		// for j := 0; j < len(words); j++ {
		// 	kva := KeyValue{words[j], "1"}
		// 	intermediate = append(intermediate, kva)
		// }
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", job.ReduceId-1) //start from 0
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	defer ofile.Close()

	args := Job{}
	args.JobType = 2
	args.ReduceId = job.ReduceId
	reply := Job{}
	call("Coordinator.FinishJob", &args, &reply)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := Job{}
		reply := Job{}
		ok := call("Coordinator.Distribute", &args, &reply)
		if ok {
			// fmt.Printf("reply.type %v\n", reply.JobType)
		} else {
			// fmt.Printf("call failed!\n")
		}
		if reply.JobType == 3 {
			//finish MapReduce
			break
		}
		if reply.JobType == 1 {
			// fmt.Printf("reply.MapId %v\n", reply.MapId)
			go doMapJob(&reply, mapf)
		}
		if reply.JobType == 2 {
			// fmt.Printf("reply.ReduceId %v\n", reply.ReduceId)
			go doReduceJob(&reply, reducef)
		}
		// fmt.Println("no job to do , wait...")
		time.Sleep(time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
