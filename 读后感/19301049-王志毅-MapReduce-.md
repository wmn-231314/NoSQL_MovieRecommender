# MapReduce
## For abstract 
Mapreduce just map the task into many pieces to calculte the result.And then use reduce to get it then into  a final result.
## for rpc implementation
``` go
const (
	Map int = iota
	Reduce
)
const (
	Unassigned int = iota
	Assigned
	Completed
)

type ExampleArgs struct {
	X int
}

type AskForJobArgs struct {
}
type AskForJobReply struct {
	NReduce int
}

type RequestJobArgs struct {
	WorkerId int
}
type RequestJobReply struct {
	JobType TaskType
	File    string
	TaskId  int
}

type JobDoneArgs struct {
	Workid  int
	TaskId  int
	JobType TaskType
}
type JobDoneReply struct {
	Exit bool
}
```
## For Coordinator implementation

``` go
type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	mapTasks    []Job
	reduceTasks []Job
	nReduce     int
	files       []string
	nMap        int
}
```
## For job Implementation
``` go
type Job struct {
	Type     TaskType
	File     string
	index    int
	Status   TaskStatus
	workedID int
}

```
## For master map Job implementation
``` go
func mapJob(mapf func(string, string) []KeyValue, file string, taskID int) {
	fmt.Println("Mao job Start")

	mapFile, err := os.Open(file)
	if err != nil {
		log.Fatalf("cannot open %v", file)
	}
	content, err := ioutil.ReadAll(mapFile)
	if err != nil {
		log.Fatalf("cannot read %v", file)
	}
	err = mapFile.Close()
	if err != nil {
		return
	}
	fmt.Println("Mao job Mid")

	kva := mapf(file, string(content))

	//until here is the write file work
	fmt.Println("Mao job mmm")

	prefix := fmt.Sprintf("%vmr-%v", "", taskID)
	files := make([]*os.File, 0, nReduce)
	buffers := make([]*bufio.Writer, 0, nReduce)
	encoders := make([]*json.Encoder, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		path := fmt.Sprintf("%v-%v-%v", prefix, i, os.Getpid())
		file, err := os.Create(path)
		if err != nil {
			checkError(err, "Cannot create file %v\n", path)
		}
		buf := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buf)
		encoders = append(encoders, json.NewEncoder(buf))
	}
	fmt.Println("Mao job eeee")

	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		err := encoders[idx].Encode(&kv)
		checkError(err, "Cannot encode %v to file\n", kv)
	}
	for _, buf := range buffers {
		err := buf.Flush()
		if err != nil {
			fmt.Println(err, "Cannot encode kv to buffer\n")
		}
	}
	for i, file := range files {
		file.Close()
		newPath := fmt.Sprintf("%v-%v", prefix, i)
		err := os.Rename(file.Name(), newPath)
		if err != nil {
			fmt.Println(err, "Cannot rename file %v\n", file.Name())
		}
	}
}
```
## For Reply job done
``` go
func (c *Coordinator) ReplyJobDone(args *JobDoneArgs, reply *JobDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var task *Job
	if args.JobType == MapTask {
		task = &c.mapTasks[args.TaskId]
	} else if args.JobType == ReduceTask {
		task = &c.reduceTasks[args.TaskId]
	} else {
		fmt.Println("error don,t have this task type")
	}
	if args.Workid == task.workedID && task.Status == Executing {
		task.Status = Finished
		if args.JobType == MapTask && c.nMap > 0 {
			c.nMap--
		} else if args.JobType == ReduceTask && c.nReduce > 0 {
			c.nReduce--
		}
	}
	reply.Exit = c.nMap == 0 && c.nReduce == 0
	return nil
}
```