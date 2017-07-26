package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

type LogEntry struct {
	IP           string
	Time         time.Time
	ResponseTime int
	StatusCode   int
	Request      string
	UserAgent    string
}

func PrepareDB() error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return errors.Wrap(err, "failed to connect to DB")
	}
	defer db.Close()
	q := "create table entries (ip varchar(40), time datetime, responsetime int, statuscode int, request text, useragent text)"
	if _, err := db.Exec(q); err != nil {
		return errors.Wrap(err, "failed to create tables")
	}

	return nil
}

func parseLogLine(line string, r *regexp.Regexp) (*LogEntry, error) {
	match := r.FindAllStringSubmatch(line, -1)
	if match == nil {
		fmt.Println(line)
		panic(nil)
	}
	m := match[0]
	t, _ := time.Parse("02/Jan/2006:15:04:05 -0700", m[2])
	status, _ := strconv.Atoi(m[4])
	entry := &LogEntry{
		IP:           m[1],
		Time:         t,
		Request:      m[3],
		StatusCode:   status,
		ResponseTime: 0,
		UserAgent:    m[5],
	}
	return entry, nil
}

var dsn = "root@tcp(127.0.0.1:3306)/logcol?parseTime=true"

func main() {
	err := PrepareDB()
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	scanner := bufio.NewScanner(os.Stdin)
	var wg sync.WaitGroup
	inputs := make(chan string, 10)
	entries := make(chan *LogEntry, 1000)
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			var r = regexp.MustCompile(`^(.+?) - - \[(.+?)\] "(.*?)" (\d+) \d+ ".+?" "(.+?)".*$`)
			for {
				select {
				case t := <-inputs:
					e, _ := parseLogLine(t, r)
					entries <- e
				case <-ctx.Done():
					wg.Done()
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		i := 0
		qsize := 10000
		queue := make([]string, qsize)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			panic(err)
		}
		defer db.Close()
		for {
			select {
			case e := <-entries:
				q := fmt.Sprintf(`("%s", "%s", %d, %d, "%s", "%s")`, e.IP, e.Time.UTC().Format("2006-01-02 15:04:05"),
					e.ResponseTime, e.StatusCode, e.Request, e.UserAgent)
				queue[i] = q
				i++
				if i == qsize {
					query := "insert into entries values" + strings.Join(queue, ",")
					_, err := db.Exec(query)
					if err != nil {
						panic(err)
					}
					i = 0
				}
			case <-ctx.Done():
				queue := queue[0:i]
				query := "insert into entries values" + strings.Join(queue, ",")
				_, err := db.Exec(query)
				if err != nil {
					panic(err)
				}
				wg.Done()
				return
			}
		}
	}()
	for scanner.Scan() {
		inputs <- scanner.Text()
		// res, _ := json.Marshal(e)
		// fmt.Println(string(res))
	}
	time.Sleep(1 * time.Second)
	cancel()
	wg.Wait()
}
