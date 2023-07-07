package dataio

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"dedup6.8T/common/simhash"
	log "github.com/sirupsen/logrus"
)

var (
   globalIndices *simhash.SimHashIndex
)

const (
    readBytesPerOp = 96 * 1024 * 1024
    //readBytesPerOp = 256
)

type lineContents struct {
    Text string `json: "text"`
}

type DataIOps struct {
    fname string
    f *os.File

    bytesPool sync.Pool
    stringPool sync.Pool

}

func NewDataIOps(fname string) (*DataIOps, error) {
    r := new(DataIOps)
    r.fname = fname

    f, err := os.Open(fname)
    if err != nil {
        mLog.Errorf("Open file <%s> failed, err: %s", fname, err)
        return nil, err
    }
    r.f = f

    r.bytesPool = sync.Pool{New: func() interface{} {
	    block := make([]byte, readBytesPerOp + 2*1024)
		return block
	}}

	r.stringPool = sync.Pool{New: func() interface{} {
		lines := ""
		return lines
	}}

    return r, nil
}

func (d* DataIOps) Close() error {
    d.f.Close()

    return nil
}

func (d *DataIOps) ReadAndIndex(op string) error {
    b := bufio.NewReader(d.f)

    var wg sync.WaitGroup

    var lineCount uint32 = 0
    var encounterEOF bool = false

    for {
        buf := d.bytesPool.Get().([]byte)

        n, err := io.ReadFull(b, buf[0:readBytesPerOp])
        buf = buf[:n]

		if n == 0 {
			if err != nil && err != io.EOF {
                mLog.Errorf("Reading file <%s> failed, err: %s", d.fname, err)
			    return err
			}
            if err == io.EOF {
                mLog.Infof("Read file <%s> finished.", d.fname)
                break
            }
		}

        bytesUntilNewLine, err := b.ReadBytes('\n')
        if encounterEOF {
            mLog.Warn("EOF had been encountered during the last reading.")
            break;
        }
        if err != nil {
            mLog.Infof("Reading an extra line from file <%s> failed, err: %s", d.fname, err)
            if err == io.EOF  {
                encounterEOF = true
                mLog.Infof("Ah! Encountered EOF when read extra line.")
                if n == 0 {
                    break
                } else {
                    if len(bytesUntilNewLine) != 0 {
                        mLog.Infof("append the rest of bytes though it encountered EOF, rest len: %d, buf len: %d, n: %d", len(bytesUntilNewLine), len(buf), n)
                        buf = append(buf, bytesUntilNewLine...)
                    }
                }
            }
        } else {
            buf = append(buf, bytesUntilNewLine...)
        }

        strBuf := d.stringPool.Get().(string)
        strBuf = string(buf)
        lines := strings.Split(strBuf, "\n")
        mLog.Infof("len of the last line: %d", len(lines[len(lines)-1]))
        if len(lines[len(lines)-1]) < 2 {
            lines = lines[0:len(lines)-1]
        }

        /*
        mLog.Info("Split lines from buf:")
        for i, l := range lines {
            mLog.Infof("line: %d, <%s>", lineCount+uint32(i)+1, l)
        }*/

        d.bytesPool.Put(buf)
        d.stringPool.Put(strBuf)

        wg.Add(1)
        go func() {
            d.process(lines, int(lineCount), op)
            atomic.AddUint32(&lineCount, uint32(len(lines)))
            mLog.Infof("process total lines: %d", lineCount)
            wg.Done()
        }()
    }

    wg.Wait()

    return nil
}

func (d *DataIOps) process(lines []string, lineBegIndex int, op string) {
    mLog.Infof("process lines: %d", len(lines))

    for i, l := range lines {
        //mLog.Infof("process <%s>, line: %d, ctn: %s", d.fname, lineBegIndex+i+1, l)
        ctn := new(lineContents)
        err := json.Unmarshal([]byte(l), ctn)
        if err != nil {
            mLog.Warnf("json Unmarshal failed, contents: %s, line: %d, fname: %s, err: %s",
                l, lineBegIndex+i+1, d.fname, err)
            continue
        }

        lMeta := new(simhash.LineMeta)
        lMeta.FileName = d.fname
        lMeta.LineNum = lineBegIndex+i+1
        hash, keys := simhash.SimHashValue(&ctn.Text)
        if hash == 0 {
            mLog.Warn("valid line, drop it.")
            continue
        }
        lMeta.SimHash = hash
        //mLog.Infof("contents hash: %d", hash)

        if op == "index" {
            globalIndices.Insert(lMeta, keys)
        }

        if op == "find" {
            similarRes, _ := globalIndices.NearBy(lMeta, keys)
            mLog.Infof("after looking up, res: %v", similarRes)
            // TODO: write a new file
            for _, r := range similarRes {
                mLog.WithFields(log.Fields{
                    "DUP": true,
                }).Infof("[curfile: <%s>, sim hash: %d] is DUP of [<%s>, sim hash: %d]",
                    d.fname, lMeta.SimHash, r.FileName, r.SimHash)
            }

        }
    }
}
