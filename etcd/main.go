package main

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const ()

func main() {
	// Create a client
	tlsInfo := transport.TLSInfo{
		TrustedCAFile: "ca.crt",
		CertFile:      "server.crt",
		KeyFile:       "server.key",
	}
	_tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		fmt.Printf("tlsconfig failed, err:%v\n", err)
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{":2379"},
		DialTimeout: 2 * time.Second,
		TLS:         _tlsConfig,
	})
	if err != nil {
		log.Fatal("new ", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	//Watch for changes
	for i := 0; i < 10; i++ {
		watchChan := cli.Watch(context.Background(), "mykey")
		go func() {
			for wresp := range watchChan {
				for _, ev := range wresp.Events {
					log.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
				}
			}
		}()
	}

	time.Sleep(2 * time.Second)

	_, err = cli.Put(ctx, "mykey", "value")
	if err != nil {
		log.Fatal("put ", err)
	}

	_, err = cli.Put(ctx, "mykey", "2")
	if err != nil {
		log.Fatal("update ", err)
	}

	_, err = cli.Delete(ctx, "mykey")
	if err != nil {
		log.Fatal("delete", err)
	}

	fmt.Println("wait")
	time.Sleep(10000 * time.Second)
}
