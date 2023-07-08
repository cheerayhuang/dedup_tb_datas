package dataio

import (
    "bufio"
    "os"
    "strconv"
    "sync/atomic"
)

func (d *DataIOps) booterWriting() {
    for i := 0; i < d.writeGoroutineNum; i++ {
        d.writeGoroutineWg.Add(1)
        go d.writeFile(i)
    }
}

func (d *DataIOps) writeFile(goIndex int) {
    defer d.writeGoroutineWg.Done()

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
            mLog.Warnf("try to write line to <%s> failed, line: %s, err: %s", fname, line, err)
        }
        atomic.AddInt32(&d.toWriteLineNums, -1)
        //mLog.Infof("Finish trying to write line on goroutine %d", goIndex)
    }
    w.Flush()
    mLog.Infof("flush file on goroutine %d", goIndex)
}
