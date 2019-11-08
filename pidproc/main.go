package main

import (
	"errors"
	"fmt"
	"github.com/prometheus/procfs"
	"log"
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
		CpuUage    CpuUage
	}

	CpuUage struct {
		ProcCpu  float64
		TotalCpu float64
	}

	Counts struct {
		CPUUserTime           float64
		CPUSystemTime         float64
		CPUCUserTime          float64
		CPUCSystemTime        float64
		MajorPageFaults       uint64
		MinorPageFaults       uint64
		CtxSwitchVoluntary    uint64
		CtxSwitchNonvoluntary uint64
	}

	IODev struct {
		ReadBytes     uint64
		WriteBytes    uint64
		NetReadBytes  uint64
		NetWriteBytes uint64
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

	CpuInfo struct {
		Processors    int
		ProcessorInfo []procfs.CPUInfo
	}
)

func GetCpuInfo() (CpuInfo, error) {
	fs, err := procfs.NewFS(procfsPath)
	if err != nil {
		return CpuInfo{}, errors.New("GetCpuInfo proc.NewFS err:" + err.Error())
	}
	cpuinfo, err := fs.CPUInfo()
	if err != nil {
		return CpuInfo{}, errors.New("GetCpuInfo fs.CPUInfo err:" + err.Error())
	}
	fs.Stat()

	return CpuInfo{
		Processors:    len(cpuinfo),
		ProcessorInfo: cpuinfo,
	}, nil

}

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
		NetWriteBytes: write_bytes,
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
		CPUUserTime:           float64(stat.UTime) / userHZ,
		CPUSystemTime:         float64(stat.STime) / userHZ,
		CPUCUserTime:          float64(stat.CUTime) / userHZ,
		CPUCSystemTime:        float64(stat.CSTime) / userHZ,
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

	fs, err := procfs.NewFS(procfsPath)
	if err != nil {
		if res_err != nil {
			res_err = errors.New(res_err.Error() + "; proc.NewFS err:" + err.Error())
		} else {
			res_err = errors.New("proc.NewFS err:" + err.Error())
		}
	}
	fsstat, err := fs.Stat()
	if err != nil {
		if res_err != nil {
			res_err = errors.New(res_err.Error() + "; fs.Stat err:" + err.Error())
		} else {
			res_err = errors.New("fs.Stat err:" + err.Error())
		}
	}
	cputotal := fsstat.CPUTotal.User +
		fsstat.CPUTotal.Nice +
		fsstat.CPUTotal.System +
		fsstat.CPUTotal.Idle +
		fsstat.CPUTotal.Iowait +
		fsstat.CPUTotal.IRQ +
		fsstat.CPUTotal.SoftIRQ +
		//fsstat.CPUTotal.Steal +
		fsstat.CPUTotal.Guest
	//fsstat.CPUTotal.GuestNice

	cpuusage := CpuUage{
		ProcCpu: float64(count.CPUUserTime +
			count.CPUSystemTime +
			count.CPUCUserTime +
			count.CPUCSystemTime),
		TotalCpu: cputotal,
	}

	startTime := time.Unix(int64(fsstat.BootTime), 0).Local()
	startTime = startTime.Add(time.Second / userHZ * time.Duration(stat.Starttime))

	return &ThreadProc{
		Pid:        proc.PID,
		ParentPid:  stat.PPID,
		Name:       stat.Comm,
		Cmdline:    cmdline,
		NumThreads: stat.NumThreads,
		StartTime:  startTime,
		State:      stat.State,
		IODev:      iodev,
		Count:      count,
		Filedesc:   filedesc,
		Memory:     memory,
		CpuUage:    cpuusage,
	}, res_err
}

func GetThreadsProc(pids ...int) ([]*ThreadProc, error) {
	var res_err error = nil
	var threads_proc []*ThreadProc
	for _, pid := range pids {
		proc, err := procfs.NewProc(pid)
		if err != nil {
			if res_err != nil {
				str := fmt.Sprintf("%s; proc.NewProc [%d] err: %s", res_err.Error(), pid, err.Error())
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
				str := fmt.Sprintf("%s; ProcMetrics [%d] err: %s", res_err.Error(), pid, err.Error())
				res_err = errors.New(str)
			} else {
				str := fmt.Sprintf("ProcMetrics [%d] err: %s", pid, err.Error())
				res_err = errors.New(str)
			}
			continue
		}
		threads_proc = append(threads_proc, thread)
	}
	return threads_proc, res_err
}

func Debug(proc ThreadProc, proc_prev ThreadProc) {

	const MB = 1024 * 1024
	fmt.Println("\n************\n")
	fmt.Println("thrid [", proc.Pid, "] ", proc.Name)
	fmt.Println("\tcmdline:", proc.Cmdline)
	fmt.Println("\tstarttime:", proc.StartTime.Format("2006/1/2 15:04:05"))
	fmt.Println("\trun state:", proc.State)
	fmt.Println("\tnum threads:", proc.NumThreads)
	fmt.Println("\tnum use fds:", proc.Filedesc.Open)
	fmt.Println("\tnum limit fds:", proc.Filedesc.Limit)
	fmt.Println("\tcpu systime:", proc.Count.CPUSystemTime)
	fmt.Println("\tcpu usertime:", proc.Count.CPUUserTime)
	fmt.Println("\tcpu csystime:", proc.Count.CPUCSystemTime)
	fmt.Println("\tcpu cusertime:", proc.Count.CPUCUserTime)
	fmt.Println("\tmem ResidentM:", float32(proc.Memory.ResidentBytes/MB))
	fmt.Println("\tmem VirtualM:", float32(proc.Memory.VirtualBytes/MB))
	fmt.Println("\tmem VmSwapM:", float32(proc.Memory.VmSwapBytes/MB))
	fmt.Println("\tmem ReadM:", float32(proc.IODev.ReadBytes/MB))
	fmt.Println("\tmem WriteM:", float32(proc.IODev.WriteBytes/MB))

	usage := (proc.CpuUage.ProcCpu - proc_prev.CpuUage.ProcCpu) / (proc.CpuUage.TotalCpu - proc_prev.CpuUage.TotalCpu)
	fmt.Println("\tcpu usage **********:", usage* float64(cpuinfo.Processors))

}

var cpuinfo CpuInfo

func main() {
	var before *ThreadProc
	cpuinfo, err := GetCpuInfo()
	if err != nil {
		log.Fatalf("get cpuinfo err:%s", err.Error())
	}else{
		log.Println("%v", cpuinfo)
	}
	for {
		threads, err := GetThreadsProc(3397)
		if err != nil {
			fmt.Println(err.Error())
		}

		time.Sleep(time.Millisecond * 2000)
		if before == nil {
			before = threads[0]
			continue
		}
		Debug(*threads[0], *before)
		before = threads[0]
	}
}

