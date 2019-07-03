package main

import (
        "database/sql"
        "log"
        "fmt"
        _ "github.com/go-sql-driver/mysql"
)

type Tag struct {
        serverid int `json:"id"`
        servername string `json:"text"`
        metricname string `json:"text"`
        flag int `json:"number"`
}

func main() {
        db, err := sql.Open("mysql", "prometheus:prometheus@/exporterdb?charset=utf8")
        if err != nil {
                log.Println(err)
        }
        defer db.Close()
        lst := make([]Tag,0)
        stmt, err := db.Query("SELECT * from serverinfo")
        if err != nil {
                log.Println(err)
        }
        for stmt.Next(){
                var tag Tag
                err = stmt.Scan(&tag.serverid, &tag.servername, &tag.metricname, &tag.flag)
                if err != nil {
                        fmt.Println(err)
                }
                lst = append(lst,tag)
                fmt.Println(tag.serverid, tag.servername, tag.metricname, tag.flag)
        }

        fmt.Println("final= ", lst)
}
