package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

var (
	cli *clientv3.Client
)

//需要收集日志的配置信息
type LogEntryConf struct {
	Topic string `json:"topic"` //日志发往kafka的topic信息
}

func Init(adress string, timeout time.Duration) (err error) {
	cli, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{adress},
		DialTimeout: timeout,
	})
	if err != nil {
		fmt.Printf("connect to etcd failed, err:%v\n", err)
		return
	}
	return
}

//通过key获取topic的信息
func GetEtcdKey(key string) (logEntryConf []*LogEntryConf, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Get(ctx, key)
	cancel()
	if err != nil {
		fmt.Printf("get from etcd failed, err:%v\n", err)
		return
	}
	for _, ev := range resp.Kvs {
		//fmt.Printf("%s:%s\n", ev.Key, ev.Value)
		err = json.Unmarshal(ev.Value, &logEntryConf)
		if err != nil {
			fmt.Printf("json.Unmarshal failed, err:%v\n", err)
			return
		}
	}
	return
}
