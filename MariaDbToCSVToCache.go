package collector

/***************************************************************************************************
SAMPLE DB STRUCTURE
+----------+-------------+-----------+-------------+-------------+------------+---------------------+
| serverid | servername  | metriccat | metrictype  | metricvalue | metricinfo | changedatetime      |
+----------+-------------+-----------+-------------+-------------+------------+---------------------+
|        1 | localdomain | collector | arp         | arp         | 1          | 2019-08-06 06:58:34 |
|        2 | localdomain | collector | bcache      | bcache      | 1          | 2019-08-06 06:58:34 |
|        3 | localdomain | collector | conntrack   | conntrack   | 1          | 2019-08-06 06:58:34 |
|        4 | localdomain | collector | cpu         | cpu         | 1          | 2019-08-06 06:58:34 |
|        5 | localdomain | summary   | cpu_summary | cpu_summary | 1          | 2019-08-06 06:58:34 |
+----------+-------------+-----------+-------------+-------------+------------+---------------------+
THE SCRIPT WILL LOAD DATA FROM DB TO CSV TO CACHE
IN CASE DB IS NOT AVAILABLE, THE DATA WILL BE LOADED FROM CSV.
IN CASE DB AND CSV BOTH NOT AVAILABLE THE updateExporterMetric WILL RETURN TRUE 
AND getProcessMetrics WILL RETURN EMPTY Conf
****************************************************************************************************/

import (
	"database/sql"
	"log"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os/exec"
	"strings"
	"encoding/csv"
	"os"
	"strconv"
	"github.com/patrickmn/go-cache"
)

type Tag struct {
	serverid         int      `json:"id"`
	servername      string   `json:"text"`
	metriccat       string   `json:"text"`
	metrictype      string   `json:"text"`
	metricvalue     string   `json:"text"`
	metricinfo      string   `json:"text"`
	changedatetime  int      `json:"number"`
}
type finalTag struct {
	servername     string
	metriccat      string
	metrictype     string
	metricvalue    string
	metricinfo     string
	changedatetime int
}

type configdata struct {
//      MetricCat       string
        MetricType      string
        Metricvalue     string
        Metricinfo      int
        changedatetime  int
}
var C = cache.New(cache.NoExpiration, 0)
type Conf map[string][]configdata
const cdt = "changeDateTimeSum"
const minfo = "metricconf"

func ReadDBToCache() {
	cmd, _ := exec.Command("hostname").Output()
	hostname := strings.Split(strings.TrimSpace(string(cmd)),".")
	
	_ , found := C.Get(cdt)                                             // CHECK FOR THE cdt IN THE CACHE 
	if found {
		cnt := getSumFromCache(cdt)                                     // GET THE SUM FROM CACHE
		sum := GetChangeDateTime(hostname[len(hostname)-1])             // GET THE SUM FROM DB
		if cnt != sum && sum != 0 {   									// IF THERE IS DIFF IN CHANGE DATE TIME & DB CONNECTION IS ESTABLISHED
			sum := GetChangeDateTime(hostname[len(hostname)-1])         // GET THE SUM OF CHANGE DATE TIME FROM DB
			GetSumIntoCache(&sum)                                       // LOAD THE SUM ON CHANGE DATE TIME INTO CACHE
			ReadDB(hostname[len(hostname)-1])                           // READ DB AND LOAD ENTIRE DATA
			configuration := make(Conf)                                 // MAKE A VARIABLE OF Conf
			conf := configuration.AddToConfig()                         // GET THE DATA INTO Conf 
			GetIntoCache(&conf)                                         // LOAD THE Conf INTO CACHE 
		}
	} else {
		sum := GetChangeDateTime(hostname[len(hostname)-1])        	    // GET THE SUM OF CHANGE DATE TIME FROM DB
		if sum != 0 {                                                   // IF DB CONNECTION IS ESTABLISHED
			GetSumIntoCache(&sum)                                       // LOAD THE SUM ON CHANGE DATE TIME INTO CACHE
			ReadDB(hostname[len(hostname)-1])                           // READ DB AND LOAD ENTIRE DATA
			configuration := make(Conf)                                 // MAKE A VARIABLE OF Conf
			conf := configuration.AddToConfig()                         // GET THE CSV DATA INTO Conf DS
			GetIntoCache(&conf)                                         // LOAD THE Conf DS INTO CACHE 
		}
		
	}

	cnt := getSumFromCache(cdt)
	if cnt == 0 {
		configuration := make(Conf)                                     // MAKE A VARIABLE OF Conf
		conf := configuration.AddToConfig()                             // GET THE CSV DATA INTO Conf DS
		sumFromConf := conf.getCDTfromConf()					        // GET THE SUM CHANGE DATE TIME FROM Conf	
		GetSumIntoCache(&sumFromConf)                                   // LOAD THE SUM OF CHANGE DATE TIME TO CACHE
		GetIntoCache(&conf)                                             // LOAD THE Conf DS INTO CACHE 
	}
		          
}


// READ DB AND GET THE SUM OF CHANGE DATE TIME
func GetChangeDateTime(hname string) int {
	db, err := sql.Open("mysql", "prometheus:prometheus@/exporterdb?charset=utf8")
	defer db.Close()
	var sumoftime int
	if err == nil {
		cnt , err1 := db.Query("SELECT sum( UNIX_TIMESTAMP(changedatetime)) FROM nodeexporter where servername = ?",  hname)
		if err1 == nil {
			for cnt.Next() {
				err1 = cnt.Scan(&sumoftime)
				if err1 != nil {
					log.Println(err1)
				}
			}
		}	
	}
	
	// fmt.Println("cnt= ", sumoftime)
return sumoftime
}

//READ DB AND LOAD ENTIRE DATA
func ReadDB(hname string) {
	db, err := sql.Open("mysql", "prometheus:prometheus1@/exporterdb?charset=utf8")
	defer db.Close()
	if err == nil {
		lst := make([]finalTag,0)
		stmt, err1 := db.Query("SELECT serverid,servername,metriccat,metrictype,metricvalue,metricinfo,UNIX_TIMESTAMP(changedatetime) from nodeexporter where servername = ?", hname)
		if err1 == nil {
			sum := 0
			for stmt.Next(){
				var tag Tag
				err = stmt.Scan(&tag.serverid, &tag.servername, &tag.metriccat, &tag.metrictype, &tag.metricvalue, &tag.metricinfo, &tag.changedatetime)
				if err == nil {
					tmp := finalTag{tag.servername,tag.metriccat,tag.metrictype,tag.metricvalue,tag.metricinfo,tag.changedatetime}
					lst = append(lst,tmp)
					sum = sum + tag.changedatetime
			//      fmt.Println(tag.servername, tag.metricname, tag.metricinfo)
				}
			}
		// fmt.Println("final= ", lst)
		// fmt.Println("sum= ", sum)
		log.Println("writing to csv")
		createCsv(lst)
		}	
	}
}

// CREATE CSV FROM DATA STRUCTURE
func createCsv(final []finalTag) {
	cmd, _ := exec.Command("whoami").Output()
	path := "/home/"+strings.TrimSpace(string(cmd))+"/csv/exporter_config.csv"
	csvfile, err := os.Create(path)
//	csvfile, err := os.Create("/home/kanshu/Projects/src/test/mariadbtest/exporter_config.csv")
	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}
	csvwriter := csv.NewWriter(csvfile)
	defer csvfile.Close()
	for _, tag := range final{
		_ = csvwriter.Write([]string{tag.metriccat,tag.metrictype,tag.metricvalue,tag.metricinfo, strconv.Itoa(tag.changedatetime)})
	}
	csvwriter.Flush()
	
}

// LOAD THE CSV INTO A DATA STRUCTURE 
func (conf Conf)AddToConfig() Conf{
	cmd, _ := exec.Command("whoami").Output()
	path := "/home/"+strings.TrimSpace(string(cmd))+"/csv/exporter_config.csv"
	// path := "/home/kanshu/Projects/src/test/mariadbtest1/exporter_config.csv"
	file, err := os.Open(path)
	defer file.Close()
	if err == nil {
		lines,_ := csv.NewReader(file).ReadAll()
		var tmp configdata
		for _ , line := range lines {
			v ,ok := conf[line[0]]
			if !ok {
				tmp.MetricType     = line[1]
				tmp.Metricvalue    = line[2]
					mi, _ := strconv.Atoi(line[3])
				tmp.Metricinfo     = mi
				cdt, _ := strconv.Atoi(line[4])
				tmp.changedatetime = cdt
				v = append(v , tmp)
				conf[line[0]] = v
			} else {
				tmp.MetricType     = line[1]
							tmp.Metricvalue    = line[2]
							mi,_ := strconv.Atoi(line[3])
							tmp.Metricinfo     = mi
							cdt,_ := strconv.Atoi(line[4])
							tmp.changedatetime = cdt
				v = append(v , tmp)
				conf[line[0]] = v
			}
		}
		// fmt.Println("conf= ",conf)
	}
return conf
}

// GET SUM OF CHANGE DATE TIME FROM Conf
func (conf Conf)getCDTfromConf() int {
	var sum int
		for _, v := range conf {
			for _ , vl := range v {
				sum = sum + vl.changedatetime
			} 
			// fmt.Println("**********************************sum***= ", sum)
		}
	return sum
}

// SET THE DATA INTO CACHE
func GetSumIntoCache(cnf *int){
	C.Set("changeDateTimeSum", &cnf, cache.NoExpiration)
}

// LOAD THE Conf DS INTO CACHE
func GetIntoCache(cnf *Conf){
	C.Set(minfo, &cnf, cache.NoExpiration)
}

// DELETE THE CACHE WITH THE KEY
func DelkeyCache(key string) {
	C.Delete(key)   // clean the cache
}

// READ THE SUM FROM CACHE
func getSumFromCache(cdt string) int {
	cnt := 0
	if x, found := C.Get(cdt); found {
		metricconf := x.(**int)
		cnt = **metricconf 
	}
return cnt
}

// GET THE STATUS OF THE METRIC FROM CACHE
func updateExporterMetric(typ string,  metric string) bool {
	if x, found := C.Get(minfo); found {
		metricconf := x.(**Conf)
		// fmt.Printf("\n %#v \n", *metricconf)
		for k, v := range **metricconf {
			if k == typ {
				for _, value := range v {
					// fmt.Printf("\n%+v\n",value)
					if strings.TrimSpace(value.MetricType) == metric  {
						if value.Metricinfo == 1 { 
							return true
						} else {
							return false
						}
					}
				}				
			} 
		}
	}
	return true
}

// GET THE WORK LOAD RELATED METRICS
func getProcessMetrics() Conf {
	tmp := make(Conf)
	if x, found := C.Get(minfo); found {
		metricconf := x.(**Conf)
		for k, v := range **metricconf {
			if k != "collector" && k != "summary"{
				tmp[k]=v
			} 
		}
	}
	fmt.Printf("****tmp***** = \n%+v\n",tmp)
	return tmp
}
