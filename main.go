package main


import (
    //"bytes"
    //"encoding/binary"
    //"crypto/md5"
    //"fmt"

    //"regexp"
    "flag"
    "os"

    //"dedup6.8T/common/simhash"
    "dedup6.8T/common/dataio"

    log "github.com/sirupsen/logrus"
)

var (
    mainLog = log.WithFields(log.Fields{
        "module": "main",
    })
)

func init() {
    mainLog.Logger.SetLevel(log.DebugLevel)
    mainLog.Logger.SetFormatter(&log.TextFormatter{
        DisableColors: false,
        FullTimestamp: true,
    })
    log.SetOutput(os.Stdout)
}

func main() {
    /*
    array := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xab, 0x01}
    //array := []byte{0x00, 0x01, 0x08}
    //array := []byte("a")
    var num uint64
    err := binary.Read(bytes.NewBuffer(array[:]), binary.BigEndian, &num)
    fmt.Println(err)
    fmt.Printf("%v, %x\n", array, num)


    re := regexp.MustCompile(`\w+`)
    TrimRes := re.FindAll([]byte("How are you? Fine, Thanks"), -1)

    fmt.Println(string(bytes.Join(TrimRes, []byte{})))
    */


    // test
    //str := "How are you? Fine, Thank you."
    //mainLog.Debugln(simhash.SimHashValue(&str))

    /*tokens := simhash.Tokenize(&str)

    mainLog.Debugf("tokens list: ")
    for _, v := range tokens {
        mainLog.Debugf("%s ", string(v))
    }*/
    var (
        srcFileName string
        targetFileName string
    )

    flag.StringVar(&srcFileName, "src", "test.jsonl", "source data file.")
    flag.StringVar(&targetFileName, "target", "test2.jsonl", "target data file which needs removing dulications.")
    flag.Parse()

    d, err := dataio.NewDataIOps(srcFileName)
    if err != nil {
        mainLog.Error("new dataio failed")
        return
    }
    defer d.Close()
    d.ReadAndIndex("index")

    d2, err := dataio.NewDataIOps(targetFileName)
    if err != nil {
        mainLog.Error("new dataio failed")
        return
    }
    defer d2.Close()
    d2.ReadAndIndex("find")
}
