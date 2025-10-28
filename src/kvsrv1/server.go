// Package kvsrv is ....
package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Val struct {
	v   string
	ver rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvMap map[string]Val
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.kvMap = make(map[string]Val)

	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Your code here.
	v, exist := kv.kvMap[args.Key]

	if !exist {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = v.v
	reply.Version = v.ver
	reply.Err = rpc.OK

}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Your code here.
	v, exist := kv.kvMap[args.Key]

	if !exist {
		if args.Version > 0 {
			reply.Err = rpc.ErrNoKey
			return

		} else {
			kv.kvMap[args.Key] = Val{v: args.Value, ver: 1}
			reply.Err = rpc.OK
		}
	} else {
		if v.ver != args.Version {
			reply.Err = rpc.ErrVersion
		} else {
			kv.kvMap[args.Key] = Val{v: args.Value, ver: args.Version + 1}
			reply.Err = rpc.OK
		}
	}

}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
