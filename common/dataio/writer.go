package dataio

import (
    "bufio"
    "os"
    "strconv"
    "sync"
)

func (d *DataIOps) booterWriting() {
    var wg sync.WaitGroup

    for i := 0; i < d.writeGoroutineNum; i++ {
        wg.Add(1)
        go d.writeFile(i, &wg)
    }

    wg.Wait()
}

func (d *DataIOps) writeFile(goIndex int, wg *sync.WaitGroup) {
    defer wg.Done()

    fname := d.outDir + "/" + strconv.Itoa(goIndex) + ".jsonl"
    f, err := os.Create(fname)
    if err != nil {
        mLog.Fatal("Create output file <%s> failed, err: %s", fname, err)
    }
    defer f.Close()

    w := bufio.NewWriter(f)

    for line := range d.writeAsyncChan {
        _, err := w.WriteString(line+"\n")
        if err != nil {
            mLog.Warnf("Write line to <%s> failed, line: %s, err: %s", fname, line, err)
        }
    }
}
