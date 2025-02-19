package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/torquem-ch/mdbx-go/mdbx"
	"os"
	"strings"

	dbm "github.com/tendermint/tm-db"

	"github.com/cosmos/iavl"
)

// TODO: make this configurable?
const (
	DefaultCacheSize int = 10000
)

func main() {
	version := 6982000
	dbDir := "/sandbox/terra-chain/data/application.db"

	prefixes := []string{
		"acc",
		"bank",
		"staking",
		"mint",
		"distribution",
		"slashing",
		"gov",
		"params",
		"ibc",
		"upgrade",
		"evidence",
		"transfer",
		"capability",
		"oracle",
		"market",
		"treasury",
		"wasm",
		"authz",
		"feegrant",
	}

	for _, prefix := range prefixes {
		fmt.Printf("handling prefix: %s\n", prefix)

		pipe := make(chan KeyValue)
		end := make(chan int)

		go func() {
			transformer := NewTransformer(prefix)
			for {
				select {
				case kv := <-pipe:
					transformer.walk(kv.key, kv.value)
				case <-end:
					transformer.commit()
					break
				}
			}
		}()

		treePrefix := fmt.Sprintf("s/k:%s/", prefix)
		tree, err := ReadTree(dbDir, version, []byte(treePrefix))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading data: %s\n", err)
			os.Exit(1)
		}

		tree.Iterate(func(key []byte, value []byte) bool {
			pipe <- KeyValue{key: key, value: value}
			return false
		})

		end <- 0
	}
}

func OpenDB(dir string) (dbm.DB, error) {
	switch {
	case strings.HasSuffix(dir, ".db"):
		dir = dir[:len(dir)-3]
	case strings.HasSuffix(dir, ".db/"):
		dir = dir[:len(dir)-4]
	default:
		return nil, fmt.Errorf("database directory must end with .db")
	}
	// TODO: doesn't work on windows!
	cut := strings.LastIndex(dir, "/")
	if cut == -1 {
		return nil, fmt.Errorf("cannot cut paths on %s", dir)
	}
	name := dir[cut+1:]
	db, err := dbm.NewGoLevelDB(name, dir[:cut])
	if err != nil {
		return nil, err
	}
	return db, nil
}

// nolint: deadcode
func PrintDBStats(db dbm.DB) {
	count := 0
	prefix := map[string]int{}
	itr, err := db.Iterator(nil, nil)
	if err != nil {
		panic(err)
	}

	defer itr.Close()
	for ; itr.Valid(); itr.Next() {
		key := string(itr.Key()[:1])
		prefix[key]++
		count++
	}
	if err := itr.Error(); err != nil {
		panic(err)
	}
	fmt.Printf("DB contains %d entries\n", count)
	for k, v := range prefix {
		fmt.Printf("  %s: %d\n", k, v)
	}
}

// ReadTree loads an iavl tree from the directory
// If version is 0, load latest, otherwise, load named version
// The prefix represents which iavl tree you want to read. The iaviwer will always set a prefix.
func ReadTree(dir string, version int, prefix []byte) (*iavl.MutableTree, error) {
	db, err := OpenDB(dir)
	if err != nil {
		return nil, err
	}
	if len(prefix) != 0 {
		db = dbm.NewPrefixDB(db, prefix)
	}

	tree, err := iavl.NewMutableTree(db, DefaultCacheSize)
	if err != nil {
		return nil, err
	}
	ver, err := tree.LoadVersion(int64(version))
	fmt.Printf("Got version: %d\n", ver)
	return tree, err
}

// parseWeaveKey assumes a separating : where all in front should be ascii,
// and all afterwards may be ascii or binary
func parseWeaveKey(key []byte) string {
	cut := bytes.IndexRune(key, ':')
	if cut == -1 {
		return encodeID(key)
	}
	prefix := key[:cut]
	id := key[cut+1:]
	return fmt.Sprintf("%s:%s", encodeID(prefix), encodeID(id))
}

// casts to a string if it is printable ascii, hex-encodes otherwise
func encodeID(id []byte) string {
	for _, b := range id {
		if b < 0x20 || b >= 0x80 {
			return strings.ToUpper(hex.EncodeToString(id))
		}
	}
	return string(id)
}

func nodeEncoder(id []byte, depth int, isLeaf bool) string {
	prefix := fmt.Sprintf("-%d ", depth)
	if isLeaf {
		prefix = fmt.Sprintf("*%d ", depth)
	}
	if len(id) == 0 {
		return fmt.Sprintf("%s<nil>", prefix)
	}
	return fmt.Sprintf("%s%s", prefix, parseWeaveKey(id))
}

type Transformer struct {
	env *mdbx.Env

	// it could be null, if so, we need to create it again
	currentTxn *mdbx.Txn
	dbi        mdbx.DBI

	name string

	batchNumberLeft int
}

func NewTransformer(dbName string) *Transformer {
	env, err := mdbx.NewEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create mdbx env: %s\n", err)
		os.Exit(1)
	}

	// set the maxdb
	env.SetOption(mdbx.OptMaxDB, 20)

	err = env.Open("/sandbox/terra-mdbx", 0, 0666)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open the env: %s\n", err)
		os.Exit(1)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to begin tx: %s\n", err)
		os.Exit(1)
	}

	dbi, err := txn.CreateDBI(dbName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create dbi: %s\n", err)
		os.Exit(1)
	}

	return &Transformer{
		env:             env,
		currentTxn:      txn,
		dbi:             dbi,
		name:            dbName,
		batchNumberLeft: 1000,
	}
}

func (t *Transformer) walk(key []byte, value []byte) {
	err := t.currentTxn.Put(t.dbi, key, value, 0)
	if err != nil {
		// some errors
		fmt.Fprintf(os.Stderr, "failed to write to db: %s\n", err)
		os.Exit(1)
	}

	t.batchNumberLeft -= 1
	if t.batchNumberLeft <= 0 {
		fmt.Printf("after 1000")
		t.batchNumberLeft = 1000
	}
}

func (t *Transformer) commit() {
	latency, _ := t.currentTxn.Commit()
	fmt.Printf("commit stats: %v\n", latency)
}

type KeyValue struct {
	key   []byte
	value []byte
}
