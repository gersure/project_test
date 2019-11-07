package main

import (
	"errors"
	"fmt"
	"github.com/prometheus/procfs"
	"time"
)

const userHZ = 100
const procfsPath = "/proc"

type (
	// Thread contains per-thread data.
	ThreadProc struct {
		Pid        int
		ParentPid  int
		Name       string
		Cmdline    []string
		NumThreads int
		StartTime  time.Time
		State      string
		Count      Counts
		IODev      IODev
		Filedesc   Filedesc
		Memory     Memory
	}

	Counts struct {
		CPUUserTime           float64
		CPUSystemTime         float64
		MajorPageFaults       uint64
		MinorPageFaults       uint64
		CtxSwitchVoluntary    uint64
		CtxSwitchNonvoluntary uint64
	}

	IODev struct {
		ReadBytes     uint64
		WriteBytes    uint64
		NetReadBytes  uint64
		NewWriteBytes uint64
	}

	// Memory describes a proc's memory usage.
	Memory struct {
		ResidentBytes uint64
		VirtualBytes  uint64
		VmSwapBytes   uint64
	}

	// Filedesc describes a proc's file descriptor usage and soft limit.
	Filedesc struct {
		// Open is the count of open file descriptors, -1 if unknown.
		Open int64
		// Limit is the fd soft limit for the process.
		Limit uint64
	}
)

func ProcMetrics(proc procfs.Proc) (*ThreadProc, error) {
	var res_err error = nil
	stat, err := proc.Stat()
	if err != nil {
		res_err = errors.New("proc.Stat err:" + err.Error())
	}
	cmdline, err := proc.CmdLine()
	if err != nil {
		if res_err != nil {
			res_err = errors.New(res_err.Error() + "; proc.CmdLine err:" + err.Error())
		} else {
			res_err = errors.New("proc.CmdLine err:" + err.Error())
		}
	}

	status, err := proc.NewStatus()
	if err != nil {
		if res_err != nil {
			res_err = errors.New(res_err.Error() + "; proc.NewStatus err:" + err.Error())
		} else {
			res_err = errors.New("proc.NewStatus err:" + err.Error())
		}
	}

	io, err := proc.IO()
	if err != nil {
		if res_err != nil {
			res_err = errors.New(res_err.Error() + "; proc.IO err:" + err.Error())
		} else {
			res_err = errors.New("proc.IO err:" + err.Error())
		}
	}

	limit, err := proc.Limits()
	if err != nil {
		if res_err != nil {
			res_err = errors.New(res_err.Error() + "; proc.Limits err:" + err.Error())
		} else {
			res_err = errors.New("proc.Limits err:" + err.Error())
		}
	}

	netdev, err := proc.NetDev()
	if err != nil {
		if res_err != nil {
			res_err = errors.New(res_err.Error() + "; proc.NetDev err:" + err.Error())
		} else {
			res_err = errors.New("proc.NetDev err:" + err.Error())
		}
	}

	var read_byte, write_bytes uint64
	for _, dev := range netdev {
		read_byte += dev.RxBytes
		write_bytes += dev.TxBytes
	}

	iodev := IODev{
		ReadBytes:     io.ReadBytes,
		WriteBytes:    io.WriteBytes,
		NetReadBytes:  read_byte,
		NewWriteBytes: write_bytes,
	}
	//schedstat, err := proc.Schedstat()
	//if err != nil {
	//	if res_err != nil {
	//		res_err = errors.New(res_err.Error() + "; proc.Schedstat err:" + err.Error())
	//	} else {
	//		res_err = errors.New("proc.Schedstat err:" + err.Error())
	//	}
	//}

	count := Counts{
		CPUUserTime:           float64(stat.CUTime / userHZ),
		CPUSystemTime:         float64(stat.STime / userHZ),
		MajorPageFaults:       uint64(stat.MajFlt),
		MinorPageFaults:       uint64(stat.MinFlt),
		CtxSwitchVoluntary:    status.VoluntaryCtxtSwitches,
		CtxSwitchNonvoluntary: status.NonVoluntaryCtxtSwitches,
	}

	numfds, err := proc.FileDescriptorsLen()
	if err != nil {
		if res_err != nil {
			res_err = errors.New(res_err.Error() + "; proc.FileDescriptorsLen err:" + err.Error())
		} else {
			res_err = errors.New("proc.FileDescriptorsLen err:" + err.Error())
		}
	}

	filedesc := Filedesc{
		Open:  int64(numfds),
		Limit: uint64(limit.OpenFiles),
	}

	memory := Memory{
		ResidentBytes: uint64(stat.ResidentMemory()),
		VirtualBytes:  uint64(stat.VirtualMemory()),
		VmSwapBytes:   status.VmSwap * 1024,
	}

	//fs, err := procfs.NewFS(procfsPath)
	//if err != nil {
	//	if res_err != nil {
	//		res_err = errors.New(res_err.Error() +  "; proc.NewFS err:" + err.Error())
	//	}else {
	//		res_err = errors.New("proc.NewFS err:" + err.Error())
	//	}
	//}
	//fsstat, err := fs.Stat()
	//if err != nil {
	//	if res_err != nil {
	//		res_err = errors.New(res_err.Error() +  "; fs.Stat err:" + err.Error())
	//	}else {
	//		res_err = errors.New("fs.Stat err:" + err.Error())
	//	}
	//}

	//startTime := time.Unix(int64(fsstat.BootTime), 0).UTC()
	//startTime = startTime.Add(time.Second / userHZ * time.Duration(stat.Starttime))

	return &ThreadProc{
		Pid:        proc.PID,
		ParentPid:  stat.PPID,
		Name:       stat.Comm,
		Cmdline:    cmdline,
		NumThreads: stat.NumThreads,
		//StartTime:  startTime,
		State:    stat.State,
		IODev:    iodev,
		Count:    count,
		Filedesc: filedesc,
		Memory:   memory,
	}, res_err
}

func GetThreadsProc(pids ...int) ([]*ThreadProc, error) {
	var res_err error = nil
	var threads_proc []*ThreadProc
	for _, pid := range pids {
		proc, err := procfs.NewProc(pid)
		if err != nil {
			if res_err != nil {
				str := fmt.Sprintf("%s; proc.NewProc [%d] err: %s",res_err.Error(), pid, err.Error())
				res_err = errors.New(str)
			} else {
				str := fmt.Sprintf("proc.NewProc [%d] err: %s", pid, err.Error())
				res_err = errors.New(str)
			}
			continue
		}
		thread, err := ProcMetrics(proc)
		if err != nil {
			if res_err != nil {
				str := fmt.Sprintf("%s; ProcMetrics [%d] err: %s",res_err.Error(), pid, err.Error())
				res_err = errors.New(str)
			} else {
				str := fmt.Sprintf("ProcMetrics [%d] err: %s", pid, err.Error())
				res_err = errors.New(str)
			}
			continue
		}
		threads_proc = append(threads_proc, thread)
		fmt.Println("\n************\n",*thread)
	}
	return threads_proc, res_err
}

func main() {
	for {
		_, err := GetThreadsProc(9844, 18214, 18484)
		if err != nil {
			fmt.Println(err.Error())
		}

		time.Sleep(time.Millisecond*500)
	}
}

