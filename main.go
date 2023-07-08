package main


import (
    //"bytes"
    //"encoding/binary"
    //"crypto/md5"
    //"fmt"

    //"regexp"
    "flag"
    "io/fs"
    "os"
    "strings"

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
    mainLog.Logger.SetLevel(log.DebugLevel)
    log.SetOutput(os.Stdout)
}

func main() {
    var (
        srcFileName string
        targetFileName string
        outDir string
    )

    flag.StringVar(&srcFileName, "src", "test.jsonl", "source data file/dir.")
    flag.StringVar(&targetFileName, "target", "test2.jsonl", "target data file/dir which needs removing dulications.")
    flag.StringVar(&outDir, "out", "./out/", "the dir to store the data files removing duplications.")
    flag.Parse()

    srcFiles := jsonlFiles(&srcFileName)
    if srcFiles == nil {
        return
    }
    for _, fname := range srcFiles {
        d, err := dataio.NewDataIOps(fname, "", 0)
        if err != nil {
            mainLog.Fatal("new dataio failed")
        }
        d.ReadAndIndex("index")
        d.Close()
    }

    targetFiles := jsonlFiles(&targetFileName)
    mainLog.Debugf("target files: %v", targetFiles)
    if targetFiles == nil {
        return
    }
    for _, fname := range targetFiles {
        names := strings.Split(fname, "/")
        fileName := names[len(names)-1]
        if err := os.MkdirAll(outDir+"/"+fileName+"/", 0750); err != nil {
            mainLog.Fatal(err)
        }

        d, err := dataio.NewDataIOps(fname, outDir+"/"+fileName+"/", 10)
        if err != nil {
            mainLog.Fatal("new dataio failed")
        }
        d.ReadAndIndex("find")
        d.Close()
    }

}

func jsonlFiles(s *string) []string {
    info, err := os.Stat(*s)
    if err != nil {
        mainLog.Fatal(err)
    }

    res := make([]string, 0)
    if info.IsDir() {
        root := *s
        fileSystem := os.DirFS(root)
        fs.WalkDir(fileSystem, ".", func(path string, d fs.DirEntry, err error) error {
            if err != nil {
                mainLog.Fatal(err)
            }
            if strings.Split(path, ".")[1] == "jsonl" {
                res = append(res, root + "/" + path)
            }

            return nil
        })
    } else {
        return []string{*s}
    }

    return res
}
