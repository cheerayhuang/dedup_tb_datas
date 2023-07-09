package simhash

import (
    "sync"
)

var (
    indexRwLock sync.RWMutex
)

const (
    dupThreshold = 6
)

type LineMeta struct {
    SimHash uint64
    //LineNum int
    FileName   string
}

type LineMetaList = []*LineMeta

type indexSubMap = map[uint16]LineMetaList

type SimHashIndex struct {
    indexHi32Hi16 indexSubMap
    indexHi32Lo16 indexSubMap
    indexLo32Hi16 indexSubMap
    indexLo32Lo16 indexSubMap
}

func NewSimHashIndex() *SimHashIndex {
    r := new(SimHashIndex)

    r.indexHi32Hi16 = make(indexSubMap)
    r.indexHi32Lo16 = make(indexSubMap)
    r.indexLo32Hi16 = make(indexSubMap)
    r.indexLo32Lo16 = make(indexSubMap)

    return r
}

func (s *SimHashIndex) Insert(m *LineMeta, keys []uint16) error {
    indexRwLock.Lock()
    defer indexRwLock.Unlock()

    s.indexHi32Hi16[keys[0]] = append(s.indexHi32Hi16[keys[0]], m)
    s.indexHi32Lo16[keys[1]] = append(s.indexHi32Lo16[keys[1]], m)
    s.indexLo32Hi16[keys[2]] = append(s.indexLo32Hi16[keys[2]], m)
    s.indexLo32Lo16[keys[3]] = append(s.indexLo32Lo16[keys[3]], m)

    return nil
}

func (s* SimHashIndex) NearBy(m *LineMeta, keys []uint16) (LineMetaList, error) {

    //resMap := make(map[*LineMeta]bool)
    r, _ := s.lookup(m, s.indexHi32Hi16[keys[0]])
    if len(r) > 0 {
        return r, nil
    }

    r, _ = s.lookup(m, s.indexHi32Lo16[keys[1]])
    if len(r) > 0 {
        return r, nil
    }

    r, _ = s.lookup(m, s.indexLo32Hi16[keys[2]])
    if len(r) > 0 {
        return r, nil
    }
    r, _ = s.lookup(m, s.indexLo32Lo16[keys[3]])

    return r, nil
}

func (s *SimHashIndex) lookup(m *LineMeta, l LineMetaList) (LineMetaList, error) {
    r := make(LineMetaList, 0)
    for _, v := range l {
        //if !resMap[v] && distance(m.SimHash, v.SimHash) <= 3 {
        if distance(m.SimHash, v.SimHash) < dupThreshold {
            r = append(r, v)
            return r, nil
            //resMap[v] = true
        }
    }

    return r, nil
}

func distance(s1 uint64, s2 uint64) uint8 {
    x := (s1 ^ s2) & ((1<<64)-1)
    //mLog.Infof("distance with(s1: %d, s2: %d)", s1, s2)

    var res uint8 = 0
    for x != 0 {
        res += 1
        x &= (x-1)
    }

    return res
}
