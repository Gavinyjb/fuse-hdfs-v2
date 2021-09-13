package main

import (
	"errors"
	"log"
	"regexp"
	"strconv"
	"strings"
)

//匹配name中的bBlockId
func match(name string) (bBlockId uint32, flag byte, err error) {
	if strings.Contains(name, "meta") {
		flag = 'c'
		path := regexp.MustCompile(`^blk_([\d]+)_([\d]+).meta$`)
		params := path.FindStringSubmatch(name)
		if len(params) < 2 {
			return 0, 'z', errors.New("匹配失败")
		}
		parseUint, err := strconv.ParseUint(params[1], 10, 32)
		if err != nil {
			log.Fatal("stringTouint32 失败!")
			return 0, 'z', err
		}
		bBlockId = uint32(parseUint)
	} else {
		flag = 'd'
		path := regexp.MustCompile(`^blk_([\d]+)$`)
		params := path.FindStringSubmatch(name)
		if len(params) < 2 {
			return 0, 'z', errors.New("匹配失败")
		}
		parseUint, err := strconv.ParseUint(params[1], 10, 32)
		if err != nil {
			log.Println("stringTouint32 失败!")
			return 0, 'z', err
		}
		bBlockId = uint32(parseUint)
	}
	return bBlockId, flag, nil
}

func readtest(data []byte) []byte {
	data = []byte("yvjinbo")
	return data
}

//func main()  {
//
//	//data:=make([]byte,12)
//	//
//	//log.Println(readtest(data))
//
//	//路经分解
//	//path:="mirror/current/BP-98/current/finall/blk_1028888_1001.meta"
//	//parts:=strings.Split(path,"/")
//	//for i, part := range parts {
//	//	log.Println(i,part)
//	//}
//	//log.Println(parts[len(parts)-1])
//	//blkId,flag,err:=match(parts[len(parts)-1])
//	//if err==nil {
//	//	log.Println(blkId,flag)
//	//}else {
//	//	log.Println("匹配失败!")
//	//}
//}
