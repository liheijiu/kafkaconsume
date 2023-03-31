package kafka

import (
	"app_consumer/etcd"
	"fmt"
	"github.com/Shopify/sarama"
	"sync"
	"time"
)

var (
	consumer     sarama.Consumer
	logDataTopic chan *consumerConf
	wg           sync.WaitGroup
)

type consumerConf struct {
	topic         string
	partitionList []int32
}

func Init(address string) (err error) {
	consumer, err = sarama.NewConsumer([]string{address}, nil)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return
	}

	return
}

//拿到topic，遍历所有分区
func TopicToChan(logEntryConf []*etcd.LogEntryConf, maxSize int) {
	logDataTopic = make(chan *consumerConf, maxSize)
	for _, topic := range logEntryConf {
		partitionList, err := consumer.Partitions(topic.Topic)
		if err != nil {
			fmt.Printf("fail to get list of partition:err%v\n", err)
			return
		}
		msg := &consumerConf{
			topic:         topic.Topic,
			partitionList: partitionList,
		}
		logDataTopic <- msg
	}
	consumePartition()
}

//获取tipoc 所对应的分区组
func consumePartition() {
	for {
		select {
		case t := <-logDataTopic:
			run(t.topic, t.partitionList)
		default:
			time.Sleep(time.Millisecond * 50)
		}
	}
}

//遍历topic 与分区 一一对应发往消费
func run(topic string, partitionList []int32) {
	for partition := range partitionList {
		fmt.Println(topic, partition)
		//丢到后台执行
		go PartitionConsumer(topic, partition)

	}
}

func PartitionConsumer(topic string, partition int) {
	//针对每个分区做一个消费者
	pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
	if err != nil {
		fmt.Printf("failed to start consumer for partition %d,err:%v\n", partition, err)
		return
	}
	wg.Add(1)
	defer pc.AsyncClose()
	// 异步从每个分区消费信息
	go func(sarama.PartitionConsumer) {
		for msg := range pc.Messages() {
			fmt.Printf("Partition:%d Offset:%d Key:%v Value:%v\n", msg.Partition, msg.Offset, msg.Key, string(msg.Value))
		}
		wg.Done()
	}(pc)
	wg.Wait()
}
