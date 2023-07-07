package dataio

import (
    "os"

    "dedup6.8T/common/simhash"

    log "github.com/sirupsen/logrus"
)

var (
    mLog = log.WithFields(log.Fields{
        "moudle": "dataio",
    })
)

func init() {
    mLog.Logger.SetLevel(log.DebugLevel)
    mLog.Logger.SetFormatter(&log.TextFormatter{
        DisableColors: false,
        FullTimestamp: true,
    })
    log.SetOutput(os.Stdout)

    globalIndices = simhash.NewSimHashIndex()
}
