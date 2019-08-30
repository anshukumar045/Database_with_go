package collector

import (
	"time"
	"fmt"
	"sync"
	"reflect"
	"github.com/patrickmn/go-cache"
)

type groundlevel map[string][]statsinput
type finalprocsum map[string][]Statsexp
type categoryAndCmdline map[string]string
const css, sts, sms, ios, cmdline = "CpuSummary", "StatusSummary","SmapsSummary","IocntSummary" , "cmdline"

func NewUpdateProcessStat() {
	for {
		arr := make([]workloadpersec, 0)
		start := time.Now()
		var wg sync.WaitGroup	
		
		for z := 0 ; z <= totlaCount  ; z++ { 
			go func(){
				defer wg.Done()
				wrk := getWorkLoadsfromConfig()
				arr = append(arr , wrk)
			}()
			wg.Add(1)
			if z < totlaCount - 1 {
				time.Sleep(time.Duration(interval) * time.Second)
			}
			wg.Wait()
		}
		// fmt.Println("catwithcmd= ", catwithcmd)
		mycppumap := make(map[string][]CPUstat)
		myiocntmap := make(map[string][]IOCounters)
		mysmapsmap := make(map[string][]smapsCounter)
		mystatusinfomap := make(map[string][]StatusInfo)
		
		for _, ar := range arr {
			for k, v := range ar {
				// fmt.Println("**************************")
				e := reflect.ValueOf(&v).Elem()	
				for i := 0; i < e.NumField(); i++ {
					// fmt.Println(e.Type().Field(i).Name)
					midleveltmp := e.Type().Field(i).Name
					switch midleveltmp {
						case "mycppu":
								vl ,ok := mycppumap[k]
								if !ok {
								vl = append(vl,v.mycppu)
								mycppumap[k] = vl
								} else {
								vl = append(vl,v.mycppu)
								mycppumap[k] = vl
								}
						case "myiocnt":
							vl ,ok := myiocntmap[k]
								if !ok {
								vl = append(vl,v.myiocnt)
								myiocntmap[k] = vl
								} else {
								vl = append(vl,v.myiocnt)
								myiocntmap[k] = vl
								}
						case "mysmaps":
							vl ,ok := mysmapsmap[k]
								if !ok {
								vl = append(vl,v.mysmaps)
								mysmapsmap[k] = vl
								} else {
								vl = append(vl,v.mysmaps)
								mysmapsmap[k] = vl
								}
						case "mystatusinfo":
							vl ,ok := mystatusinfomap[k]
								if !ok {
								vl = append(vl,v.mystatusinfo)
								mystatusinfomap[k] = vl
								} else {
								vl = append(vl,v.mystatusinfo)
								mystatusinfomap[k] = vl
								}
						}
				}					
			}
		}

		fp := make(chan groundlevel)
		go createGroundLevel4(myiocntmap, fp)
		go createGroundLevel3(mysmapsmap, fp)
		go createGroundLevel2(mystatusinfomap, fp)
		go createGroundLevel1(mycppumap, fp)

		myiocnt := <- fp
		mysmap  := <- fp
		mystatu := <- fp
		cpustats := <- fp

		// fmt.Println("*****cpustats******=", cpustats)

		ccc1 := make(chan finalprocsum)
		ccc2 := make(chan finalprocsum)
		ccc3 := make(chan finalprocsum)
		ccc4 := make(chan finalprocsum)
		go getProcSummary(cpustats, ccc1)
		go getProcSummary(mystatu, ccc2)
		go getProcSummary(mysmap, ccc3)
		go getProcSummary(myiocnt, ccc4)


		cpustatsumaary := <- ccc1
		mystatuusummary := <- ccc2
		mysmapsummary := <- ccc3
		myiocntsummary := <- ccc4

		go func() {
			defer wg.Done()
			GetfinalprocsumIntoCache(css, &cpustatsumaary)
		}()
		wg.Add(1)

		go func() {
			defer wg.Done()
			GetfinalprocsumIntoCache(sts, &mystatuusummary)
		}()
		wg.Add(1)

		go func() {
			defer wg.Done()
			GetfinalprocsumIntoCache(sms, &mysmapsummary)
		}()
		wg.Add(1)
		
		go func() {
			defer wg.Done()
			GetfinalprocsumIntoCache(ios, &myiocntsummary)
		}()
		wg.Add(1)
		wg.Wait()

		timesince := time.Since(start)
		fmt.Println("time since= ", timesince)
	}
}
type workloadpersec map[string]workloaddata
func getWorkLoadsfromConfig() workloadpersec{
	workloads := GetPidWorkloads()
		// fmt.Println(workloads)
	// var wg sync.WaitGroup

	pidlst := []string{}
	workloaddatamap := make(map[string]workloaddata)
	for k , v := range workloads {
		cmdArr := make([]string, 0)
		mycppuArr := make([]CPUstat, 0)
		myiocntArr := []IOCounters{}
		mysmapsArr := 	[]smapsCounter{}
		mystatusinfoArr := []StatusInfo{}

		pidlst = append(pidlst, v...)
		for _ , pid := range v {
			c1 := make(chan string, 1)
			c2 := make(chan CPUstat, 1)
			c3 := make(chan IOCounters, 1)
			c4 := make(chan smapsCounter, 1)
			c5 := make(chan StatusInfo, 1)
			
			go func() {
				// defer wg.Done()
				cmdlinetmp , _ := getCommandline(pid)             // command line for each PID
				c1 <- cmdlinetmp
			}()
			// wg.Add(1)
		
			
			go func() {
				// defer wg.Done()
				mycpputmp, _ := CPUinfo(pid)                       // cpu-total, user, sys, nice, iowait, guest
				c2 <- *mycpputmp
			}()
			// wg.Add(1)

			go func() {
				// defer wg.Done()
				myiocnttmp, _ := getIOCounter(pid)                // readcount, write count, read_byte, write_byte
				c3 <- *myiocnttmp
			}()
			// wg.Add(1)

			go func() {
				// defer wg.Done()
				mysmapstmp, _ := getSmaps(pid)                    // rss, pss, shared dirty, private dirty
				c4 <- *mysmapstmp
			}()
			// wg.Add(1)

			go func() {
				// defer wg.Done()
				mystatusinfotmp, _ := getStatusInfo(pid)    	 // get StatusInfo
				c5 <- *mystatusinfotmp
			}()
			// wg.Add(1)
			// wg.Wait()

			cmdlinetmp := <- c1
			mycpputmp := <- c2
			myiocnttmp := <- c3
			mysmapstmp := <- c4
			mystatusinfotmp := <- c5

			cmdArr = append(cmdArr, cmdlinetmp)
			mycppuArr = append(mycppuArr, mycpputmp)
			myiocntArr = append(myiocntArr, myiocnttmp)
			mysmapsArr = append(mysmapsArr, mysmapstmp)
			mystatusinfoArr = append(mystatusinfoArr, mystatusinfotmp)
		}

		// var cmdlines string 
		// for _ , v := range cmdArr {
		// 	cmdlines = cmdlines + ", " + v
		// }

		cmdlines := strings.Join(distinct(cmdArr), ",")
		sumCPU_total, sumUser, sumSystem, sumNice, sumIowait, sumGuest, sumMinpgflt,sumMajpgflt,sumThreadCnt := 0,0,0,0,0,0,0,0,0
		
		for _, v := range mycppuArr {
			sumCPU_total = sumCPU_total + v.CPU_total
			sumUser = sumUser + v.User
			sumSystem = sumSystem + v.System
			sumNice = sumNice + v.Nice
			sumIowait = sumIowait + v.Iowait
			sumGuest = sumGuest + v.Guest
			sumMinpgflt = sumMinpgflt + v.Minpgflt
			sumMajpgflt = sumMajpgflt + v.Majpgflt
			sumThreadCnt = sumThreadCnt + v.ThreadCnt
		}
		
		sumrdcnt, sumwrtcnt, sumrdbytes, sumwrtbytes := 0,0,0,0
		for _, v := range myiocntArr {
			sumrdcnt = sumrdcnt + v.rdcnt
			sumwrtcnt = sumwrtcnt + v.wrtcnt
			sumrdbytes = sumrdbytes + v.rdbytes
			sumwrtbytes = sumwrtbytes + v.wrtbytes
		}
		
		sumrss, sumpss, sumsdirty,sumpdirty := 0,0,0,0
		for _, v := range mysmapsArr {
			sumrss = sumrss + v.rss
			sumpss = sumpss + v.pss
			sumsdirty = sumsdirty + v.sdirty
			sumpdirty = sumpdirty + v.pdirty
		}
		
		sumVmData,sumVmStk,sumVmLck, sumVmLib,sumVmExe  := 0,0,0,0,0
		for _, v := range mystatusinfoArr {
			sumVmData = sumVmData + v.VmData
			sumVmStk  = sumVmStk + v.VmStk
			sumVmLck = sumVmLck + v.VmLck
			sumVmLib = sumVmLib + v.VmLib
			sumVmExe = sumVmExe + v.VmExe
		}
		_ ,ok := workloaddatamap[k]
			if !ok {
				var tmp workloaddata
				var tmpCpu CPUstat
				var tmpio IOCounters
				var tmpsmaps smapsCounter
				var tmpstatusinfo StatusInfo

				tmpCpu = CPUstat{sumCPU_total,sumUser,sumSystem,sumNice,sumIowait,sumGuest,sumMinpgflt,sumMajpgflt,sumThreadCnt}
				tmpio = IOCounters{sumrdcnt,sumwrtcnt,sumrdbytes,sumwrtbytes}
				tmpsmaps = smapsCounter{sumrss, sumpss, sumsdirty, sumpdirty}
				tmpstatusinfo = StatusInfo{sumVmData,sumVmStk,sumVmLck,sumVmLib,sumVmExe}
				
				tmp = workloaddata{cmdlines,tmpCpu,tmpio,tmpsmaps,tmpstatusinfo}
				workloaddatamap[k] = tmp
			}	
	}
return workloaddatamap
}
//READ FROM CACHE
func getProcessSummaryMetricsC(key string, c chan finalprocsum )  {
	tmp := make(finalprocsum)
	if x, found := C.Get(key); found {
		metricconf := x.(**finalprocsum)
		for k, v := range **metricconf {
			// fmt.Println(k , "-->", v)
			tmp[k] = v
		} 
	}
	c <- tmp
}

func getProcesscatwithKey(key string, c chan categoryAndCmdline)  {
	tmp := make(categoryAndCmdline)
	if x, found := C.Get(key); found {
		metricconf := x.(**categoryAndCmdline)
		for k, v := range **metricconf {
			// fmt.Println(k , "-->", v)
			tmp[k] = v
		} 
	}
	c <- tmp
}


// LOAD THE finalprocsum DS INTO CACHE
func GetfinalprocsumIntoCache(key string ,fps *finalprocsum){
	C.Set(key, &fps, cache.NoExpiration)
}

func GetCmdlnIntoCache(cmdd *categoryAndCmdline){
	C.Set(cmdline, &cmdd, cache.NoExpiration)
}

func getProcSummary(maparr groundlevel , c chan finalprocsum) {
	fpsum := make(finalprocsum)
	for k , arr := range maparr {
		for _ , v := range arr {
				vl , ok := fpsum[k]
				if !ok {
					final := SumStatsCalculate(v)
					vl := append(vl,final...)
					fpsum[k] = vl
				} else {
					final := SumStatsCalculate(v)
					vl := append(vl,final...)
					fpsum[k] = vl
				}
		}
	}
c <- fpsum
}

// func  createGroundLevel4(arr map[string][]IOCounters , wg *sync.WaitGroup) groundlevel {
func  createGroundLevel4(arr map[string][]IOCounters, c chan groundlevel)  {
	// defer wg.Done()
	gndlvlmap := make(groundlevel)
	for k , v := range arr {
		value, ok := gndlvlmap[k]
		// value, ok := myiocnt[k]
		rdcntmap := make(map[string][]float64)
		wrtcntmap := make(map[string][]float64)
		rdbytesmap := make(map[string][]float64)
		wrtbytesmap := make(map[string][]float64)

		if !ok {
			for _, vl := range v {
				e := reflect.ValueOf(&vl).Elem()
				for i := 0; i < e.NumField(); i++ { 
					structname := e.Type().Field(i).Name
					switch structname {
					case "rdcnt":
						rd ,ok := rdcntmap["rdcnt"]
						if !ok {
							rd = append(rd,float64(vl.rdcnt))
							rdcntmap["rdcnt"] = rd
						} else {
							rd = append(rd,float64(vl.rdcnt))
							rdcntmap["rdcnt"] = rd
						}
					case "wrtcnt":
						wrt ,ok := wrtcntmap["wrtcnt"]
						if !ok {
							wrt = append(wrt,float64(vl.wrtcnt))
							wrtcntmap["wrtcnt"] = wrt
						} else {
							wrt = append(wrt,float64(vl.wrtcnt))
							wrtcntmap["wrtcnt"] = wrt
						}
					case "rdbytes":
						rdb ,ok := rdbytesmap["rdbytes"]
						if !ok {
							rdb = append(rdb,float64(vl.rdbytes))
							rdbytesmap["rdbytes"] = rdb
						} else {
							rdb = append(rdb,float64(vl.rdbytes))
							rdbytesmap["rdbytes"] = rdb
						}
					case "wrtbytes":
						wtb ,ok := wrtbytesmap["wrtbytes"]
						if !ok {
							wtb = append(wtb,float64(vl.wrtbytes))
							wrtbytesmap["wrtbytes"] = wtb
						} else {
							wtb = append(wtb,float64(vl.wrtbytes))
							wrtbytesmap["wrtbytes"] = wtb
						}
					}
				}
			}
			value = append(value, rdcntmap)
			value = append(value, wrtcntmap)
			value = append(value, rdbytesmap)
			value = append(value, wrtbytesmap)
			gndlvlmap[k] = value 
		}
	}
	c <- gndlvlmap
}
// func createGroundLevel3(arr map[string][]smapsCounter, wg *sync.WaitGroup) groundlevel { 
// func createGroundLevel3(arr map[string][]smapsCounter, mysmap groundlevel, wg *sync.WaitGroup) {
func createGroundLevel3(arr map[string][]smapsCounter, c chan groundlevel) {
	// defer wg.Done()
	gndlvlmap := make(groundlevel)
	for k , v := range arr { 
		value, ok := gndlvlmap[k]
		// value, ok := mysmap[k]
		rssmap := make(map[string][]float64)
		pssmap := make(map[string][]float64)
		sdirtymap := make(map[string][]float64)
		pdirtymap := make(map[string][]float64)

		if !ok {
			for _, vl := range v {
				e := reflect.ValueOf(&vl).Elem()
				for i := 0; i < e.NumField(); i++ {
					structname := e.Type().Field(i).Name
					switch structname {
					case "rss":
						rs , ok := rssmap["rss"]
						if !ok {
							rs = append(rs,float64(vl.rss))
							rssmap["rss"] = rs
						} else {
							rs = append(rs,float64(vl.rss))
							rssmap["rss"] = rs
						}
					case "pss":
						ps ,ok := pssmap["pss"]
						if !ok {
							ps = append(ps,float64(vl.pss))
							pssmap["pss"] = ps
						} else {
							ps = append(ps,float64(vl.pss))
							pssmap["pss"] = ps
						}
					case "sdirty":
						sd ,ok := sdirtymap["sdirty"]
						if !ok {
							sd = append(sd,float64(vl.sdirty))
							sdirtymap["sdirty"] = sd
						} else {
							sd = append(sd,float64(vl.sdirty))
							sdirtymap["sdirty"] = sd
						}
					case "pdirty":
						pd ,ok := pssmap["pdirty"]
						if !ok {
							pd = append(pd,float64(vl.pdirty))
							pssmap["pdirty"] = pd
						} else {
							pd = append(pd,float64(vl.pdirty))
							pssmap["pdirty"] = pd
						}
					}
				}
			}
			value = append(value, rssmap)
			value = append(value, pssmap)
			value = append(value, sdirtymap)
			value = append(value, pdirtymap)
			gndlvlmap[k] = value 

		}
	}

	c <- gndlvlmap
}
// func  createGroundLevel2(arr map[string][]StatusInfo) groundlevel {
// func  createGroundLevel2(arr map[string][]StatusInfo, mystatu groundlevel, wg *sync.WaitGroup)  { c chan groundlevel
func  createGroundLevel2(arr map[string][]StatusInfo, c chan groundlevel)  { 
	// defer wg.Done()
	gndlvlmap := make(groundlevel)
	for k,v := range arr {
		value, ok := gndlvlmap[k]
		// value, ok := mystatu[k]
		VmDatamap := make(map[string][]float64)
		VmStkmap := make(map[string][]float64)
		VmLckmap := make(map[string][]float64)
		VmLibmap := make(map[string][]float64)
		VmExemap := make(map[string][]float64)

		if !ok {
			for _, vl := range v {
				e := reflect.ValueOf(&vl).Elem()
				for i := 0; i < e.NumField(); i++ {
					structname := e.Type().Field(i).Name
					switch structname {
						case "VmData":
							vdt, ok := VmDatamap["VmData"]
							if !ok {
								vdt = append(vdt, float64(vl.VmData))
								VmDatamap["VmData"] = vdt
							} else {
								vdt = append(vdt, float64(vl.VmData))
								VmDatamap["VmData"] = vdt
							}
						case "VmStk":
							vst, ok := VmStkmap["VmStk"]
							if !ok {
								vst = append(vst, float64(vl.VmStk))
								VmStkmap["VmStk"] = vst
							} else {
								vst = append(vst, float64(vl.VmStk))
								VmStkmap["VmStk"] = vst
							}
						case "VmLck":
							vlk, ok := VmLckmap["VmLck"]
							if !ok {
								vlk = append(vlk, float64(vl.VmLck))
								VmLckmap["VmLck"] = vlk
							} else {
								vlk = append(vlk, float64(vl.VmLck))
								VmLckmap["VmLck"] = vlk
							}
						case "VmLib":
							vlb, ok := VmLibmap["VmLib"]
							if !ok {
								vlb = append(vlb, float64(vl.VmLib))
								VmLibmap["VmLib"] = vlb
							} else {
								vlb = append(vlb, float64(vl.VmLib))
								VmLibmap["VmLib"] = vlb
							}
						case "VmExe":
							vme, ok := VmExemap["VmExe"]
							if !ok {
								vme = append(vme, float64(vl.VmExe))
								VmExemap["VmExe"] = vme
							} else {
								vme = append(vme, float64(vl.VmExe))
								VmExemap["VmExe"] = vme
							}
					}
				}
			}
			value = append(value,VmDatamap)
			value = append(value, VmStkmap)
			value = append(value, VmLckmap)
			value = append(value, VmLibmap)
			value = append(value, VmExemap)
			gndlvlmap[k] = value 
		}
	}

	c <- gndlvlmap
}
// func  createGroundLevel1(arr map[string][]CPUstat) groundlevel {
// func  createGroundLevel1(arr map[string][]CPUstat, cpustats groundlevel, wg *sync.WaitGroup)  {
func  createGroundLevel1(arr map[string][]CPUstat, c chan groundlevel)  {
	// defer wg.Done()
	gndlvlmap := make(groundlevel)
	for k , v := range arr {
		value, ok := gndlvlmap[k]
		// value, ok := cpustats[k]
		CPU_totalmap := make(map[string][]float64)
		Usermap := make(map[string][]float64)
		Systemmap := make(map[string][]float64)
		Nicemap := make(map[string][]float64)
		Iowaitmap := make(map[string][]float64)
		Guestmap := make(map[string][]float64)
		Minpgfltmap := make(map[string][]float64)
		Majpgfltmap := make(map[string][]float64)
		ThreadCntmap := make(map[string][]float64)
		if !ok {
			for _, vl := range v {
				e := reflect.ValueOf(&vl).Elem()
				for i := 0; i < e.NumField(); i++ {
					structname := e.Type().Field(i).Name
					switch structname {
						case "CPU_total":
							cp, ok := CPU_totalmap["CPU_total"]
							if !ok {
								cp = append(cp,float64(vl.CPU_total))
								CPU_totalmap["CPU_total"] = cp
							} else {
								cp = append(cp,float64(vl.CPU_total))
								CPU_totalmap["CPU_total"] = cp
							}
						case "User":
							usr, ok := Usermap["User"]
							if !ok {
								usr = append(usr,float64(vl.User))
								Usermap["User"] = usr
							} else {
								usr = append(usr,float64(vl.User))
								Usermap["User"] = usr
							}
						case "System":
							sys, ok := Systemmap["System"]
							if !ok {
								sys = append(sys,float64(vl.System))
								Systemmap["System"] = sys
							} else {
								sys = append(sys,float64(vl.System))
								Systemmap["System"] = sys
							}
						case "Nice":
							nic, ok := Nicemap["Nice"]
							if !ok {
								nic = append(nic,float64(vl.Nice))
								Nicemap["Nice"] = nic
							} else {
								nic = append(nic,float64(vl.Nice))
								Nicemap["Nice"] = nic
							}
						case "Iowait":
							iow, ok := Iowaitmap["Iowait"]
							if !ok {
								iow = append(iow,float64(vl.Iowait))
								Iowaitmap["Iowait"] = iow
							} else {
								iow = append(iow,float64(vl.Iowait))
								Iowaitmap["Iowait"] = iow
							}
						case "Guest":
							gst, ok := Guestmap["Guest"]
							if !ok {
								gst = append(gst,float64(vl.Guest))
								Guestmap["Guest"] = gst
							} else {
								gst = append(gst,float64(vl.Guest))
								Guestmap["Guest"] = gst
							}
						case "Minpgflt":
							minf, ok := Minpgfltmap["Minpgflt"]
							if !ok {
								minf = append(minf,float64(vl.Minpgflt))
								Minpgfltmap["Minpgflt"] = minf
							} else {
								minf = append(minf,float64(vl.Minpgflt))
								Minpgfltmap["Minpgflt"] = minf
							}
						case "Majpgflt":
							majf, ok := Majpgfltmap["Majpgflt"]
							if !ok {
								majf = append(majf,float64(vl.Majpgflt))
								Majpgfltmap["Majpgflt"] = majf
							} else {
								majf = append(majf,float64(vl.Majpgflt))
								Majpgfltmap["Majpgflt"] = majf
							}
						case "ThreadCnt":
							tcnt, ok := ThreadCntmap["ThreadCnt"]
							if !ok {
								tcnt = append(tcnt,float64(vl.ThreadCnt))
								ThreadCntmap["ThreadCnt"] = tcnt
							} else {
								tcnt = append(tcnt,float64(vl.ThreadCnt))
								ThreadCntmap["ThreadCnt"] = tcnt
							}	
					}
				}
			}

			value = append(value,CPU_totalmap)
			value = append(value,Usermap)
			value = append(value,Systemmap)
			value = append(value, Nicemap)
			value = append(value, Iowaitmap)
			value = append(value, Guestmap)
			value = append(value, Minpgfltmap)
			value = append(value, Majpgfltmap)
			value = append(value, ThreadCntmap)

			// cpustats[k] = value 
			gndlvlmap[k] = value
		}	
	}
	// return gndlvlmap
	c <- gndlvlmap
}
