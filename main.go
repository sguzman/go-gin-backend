package main

import (
    "database/sql"
    "encoding/json"
    "fmt"
    "github.com/gin-gonic/gin"
    _ "github.com/lib/pq"
    "os"
    "strconv"
)

type DataType struct {
    Serial string `json:"serial"`
    Time string `json:"time"`
    Subs   uint64 `json:"subs"`
}

type JsonType struct {
    Serials []DataType `json:"serials"`
}

func (that DataType) String() string {
    return fmt.Sprintf("{%s, %s, %d}",
        that.Serial, that.Time, that.Subs)
}

const (
    defaultHost = "192.168.1.63"
    defaultPort = "30000"
)

func connStr() string {
    host := os.Getenv("DB_HOST")
    port := os.Getenv("DB_PORT")

    if len(host) == 0 || len(port) == 0 {
        return fmt.Sprintf("user=postgres dbname=youtube host=%s port=%s sslmode=disable", defaultHost, defaultPort)
    } else {
        return fmt.Sprintf("user=postgres dbname=youtube host=%s port=%s sslmode=disable", host, port)
    }
}

func connection() *sql.DB {
    db, err := sql.Open("postgres", connStr())
    {
        if err != nil {
            panic(err)
        }
    }

    return db
}

func channels(serial string, limit uint64) []DataType {
    sqlStr := `select 
                     serial, time, subs 
              from youtube.entities.chan_stats 
              where serial = $1
              ORDER BY time desc
              LIMIT $2`
    db := connection()
    {
        _, err := db.Exec("SET TIME ZONE 'US/Pacific'")
        if err != nil {
            panic(err)
        }
    }

    defer func() {
        err := db.Close()
        {
            if err != nil {
                panic(err)
            }
        }
    }()

    row, err := db.Query(sqlStr, serial, limit)
    {
        if err != nil {
            panic(err)
        }
    }

    serials := make([]DataType, 0)
    for row.Next() {
        var (
            serial string
            time string
            subs uint64
        )

        err := row.Scan(&serial, &time, &subs)
        {
            if err != nil {
                panic(err)
            }
        }

        data := DataType{
            Serial: serial,
            Time: time,
            Subs: subs,
        }

        serials = append(serials, data)
    }

    return serials
}

func jsonFromSerial(serial string, limit uint64) []byte {
    chans := channels(serial, limit)
    jsonData := JsonType{
        Serials: chans,
    }

    jsonStr, err := json.Marshal(jsonData)
    {
        if err != nil {
            panic(err)
        }
    }

    return jsonStr
}

func main() {
    {
        r := gin.Default()
        r.GET("/:serial/:limit", func(c *gin.Context) {
            serial := c.Param("serial")
            count, err := strconv.ParseUint(c.Param("limit"), 10, 64)
            {
                if err != nil {
                    panic(err)
                }
            }

            jsonData := jsonFromSerial(serial, count)
            {
                writeCount, err := c.Writer.Write(jsonData)
                if err != nil {
                    panic(err)
                }

                fmt.Println("Wrote", writeCount, "bytes")
            }

        })

        {
            err := r.Run()
            if err != nil {
                panic(err)
            }
        }
    }
}
