package collector

import (
	"time"
	"fmt"
	"sync"
	// "reflect"
	"github.com/patrickmn/go-cache"
)
type fsm []statsinput
type restproc []Statsexp
const fsmi = "filestatmeminfo"

func NewUpdateProcessStatrest() {
	for {
		mutex := new(sync.Mutex)
		arrfstat := make([]FileStat, 0)
		arrfmem := make([]MemStat, 0)
		start := time.Now()
		for z := 0 ; z <= totlaCount  ; z++ {
			myfilestat, _ := getFileStat()
			mymeminfo, _ := getMemInfo()

			mutex.Lock()
			go func(myfilestat FileStat, mymeminfo MemStat) {
				arrfstat = append(arrfstat, myfilestat)
				arrfmem = append(arrfmem, mymeminfo)
				mutex.Unlock()
			}(*myfilestat, *mymeminfo)

			if z < totlaCount - 1 {
				time.Sleep(time.Duration(interval) * time.Second)
			}
		}
		fsmc := make(chan fsm)
		go getFileStatSum(arrfstat, fsmc)
		go getMemInfoStats(arrfmem, fsmc)
		
		finalstat := <- fsmc
		finalmem := <- fsmc

		semifinal := append(finalstat, finalmem...)

		fnlc := make(chan restproc)
		go getRestProcSummay(semifinal, fnlc)

		finalscore := <- fnlc
		
		var wg sync.WaitGroup	
		go func () {
			defer wg.Done()
			GetRestprocsumIntoCache(fsmi,&finalscore)
		}()
		wg.Add(1)
		wg.Wait()
		
		timesince := time.Since(start)
		fmt.Println("time since1= ", timesince)
	}
}

// READ FROM CACHE
func getProcessRestSummaryMetricsC(key string, c chan restproc )  {
	// tmp := make(restproc,0)
	if x, found := C.Get(key); found {
		cnf := x.(**restproc)
	c <- **cnf
	}
}


// LOAD THE finalprocsum DS INTO CACHE
func GetRestprocsumIntoCache(key string ,fps *restproc) {
	C.Set(key, &fps, cache.NoExpiration)
}



func getRestProcSummay(sfnl fsm, c chan restproc ) {
	tmp := make(restproc, 0)
	for _, v := range sfnl {
		final := SumStatsCalculate(v)
		tmp = append(tmp, final...)
	}

	c <- tmp
}

func getFileStatSum(fs []FileStat, c chan fsm) {
	fsmarr := make(fsm, 0)
	opf, ttf := make([]float64, 0), make([]float64, 0)
	for _, v := range fs {
		opf = append(opf, float64(v.OpenFiles))
		ttf = append(ttf, float64(v.TotalFiles))
	}
	tmp := make(map[string][]float64)
	tmp["OpenFiles"] = opf
	fsmarr = append(fsmarr, tmp)

	tmp = make(map[string][]float64)
	tmp["TotalFiles"] = ttf
	fsmarr = append(fsmarr, tmp)
	c <- fsmarr
}


func getMemInfoStats(mem []MemStat, c chan fsm) {
	fsmarr := make(fsm, 0)

	memttl, memfr, memavl, memusd := make([]float64, 0), make([]float64, 0), make([]float64, 0), make([]float64, 0)

	for _ , v := range mem {
		memttl = append(memttl, float64(v.MemTotal))
		memfr = append(memfr, float64(v.MemFree))
		memavl = append(memavl, float64(v.MemAvailable))
		memusd = append(memusd, float64(v.MemUsed))
	}
	tmp := make(map[string][]float64)
	tmp["MemTotal"] = memttl
	fsmarr = append(fsmarr, tmp)

	tmp = make(map[string][]float64)
	tmp["MemFree"] = memfr
	fsmarr = append(fsmarr, tmp)

	tmp = make(map[string][]float64)
	tmp["MemAvailable"] = memavl
	fsmarr = append(fsmarr, tmp)

	tmp = make(map[string][]float64)
	tmp["MemUsed"] = memusd
	fsmarr = append(fsmarr, tmp)

	c <- fsmarr
}

