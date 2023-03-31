package main

import (
	"app_consumer/conf"
	"app_consumer/etcd"
	"app_consumer/kafka"
	"fmt"
	"gopkg.in/ini.v1"
	"sync"
	"time"
)

var (
	cfg = new(conf.AppConfig)
	wg  sync.WaitGroup
)

func main() {

	//0.配置文件加载做初始化
	err := ini.MapTo(cfg, "./conf/config.ini")
	if err != nil {
		fmt.Printf("init config falied,err:%v\n", err)
		return
	}
	fmt.Println("init config success")

	//1.kafka连接初始化
	err = kafka.Init(cfg.KafkaConfig.Address)
	if err != nil {
		fmt.Printf("init kafka falied,err:%v\n", err)
		return
	}
	fmt.Println("init kafka success")

	//2.etcd连接初始化
	err = etcd.Init(cfg.EtcdConfig.Address, time.Duration(cfg.EtcdConfig.Timeout)*time.Second)
	if err != nil {
		fmt.Printf("init etcd falied,err:%v\n", err)
		return
	}
	fmt.Println("init etcd success")

	//获取Topic
	logEntryConf, err := etcd.GetEtcdKey(cfg.EtcdConfig.Key)
	if err != nil {
		fmt.Printf("get  topic failed, err:%v\n", err)
		return
	}
	fmt.Printf("get topic success,%v\n", logEntryConf)

	//遍历topic
	for index, value := range logEntryConf {
		fmt.Printf("index:%v  value:%v\n", index, value.Topic)
	}
	kafka.TopicToChan(logEntryConf, cfg.KafkaConfig.Chan_max)
}
