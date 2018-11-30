package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
)

var (
	printedAB  = false
	dir1, dir2 string
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "Usage:", os.Args[0], "<dir A> <dir B>")
		os.Exit(1)
	}

	dir1, dir2 = os.Args[1], os.Args[2]
	db1, err := leveldb.OpenFile(dir1, nil)
	if err != nil {
		log.Fatalf("can't open DB %s: %v", dir1, err)
	}
	db2, err := leveldb.OpenFile(dir2, nil)
	if err != nil {
		log.Fatalf("can't open DB %s: %v", dir2, err)
	}
	defer db1.Close()
	defer db2.Close()

	iter1 := db1.NewIterator(nil, nil)
	iter2 := db2.NewIterator(nil, nil)
	defer iter1.Release()
	defer iter2.Release()
	iter1.Next()
	iter2.Next()
	for iter1.Key() != nil && iter2.Key() != nil {
		k1, k2 := iter1.Key(), iter2.Key()
		switch bytes.Compare(k1, k2) {
		case 1:
			// k1 > k2, iter1 is ahead
			printkey(k2, "only in B", fmt.Sprint("len=", len(iter2.Value())))
			iter2.Next()
		case -1:
			// k1 < k2, iter2 is ahead
			printkey(k1, "only in A", fmt.Sprint("len=", len(iter1.Value())))
			iter1.Next()
		case 0:
			// They're at the same key.
			if !bytes.Equal(iter1.Value(), iter2.Value()) {
				printkey(k1,
					"value mismatch",
					fmt.Sprint("len1=", len(iter1.Value())),
					fmt.Sprint("len2=", len(iter2.Value())),
				)
			}
			iter1.Next()
			iter2.Next()
		}
	}
	if err := iter1.Error(); err != nil {
		log.Fatalf("iterator 1 error: %v", err)
	}
	if err := iter2.Error(); err != nil {
		log.Fatalf("iterator 2 error: %v", err)
	}
}

func printkey(key []byte, info ...string) {
	// show A/B if not displayed yet
	if !printedAB {
		fmt.Println("A:", dir1, "B:", dir2)
		printedAB = true
	}
	// add ascii prefix if present
	prefix := 0
	for ; prefix < len(key); prefix++ {
		if key[prefix] < ' ' || key[prefix] > '~' {
			break
		}
	}
	if prefix > 0 {
		info = append(info, fmt.Sprintf("ascii key prefix %q", key[:prefix]))
	}

	fmt.Printf("%x %s\n", key, strings.Join(info, ", "))
}
