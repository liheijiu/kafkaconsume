package conf

type AppConfig struct {
	KafkaConfig `ini:"kafka"`
	EtcdConfig  `ini:"etcd"`
}

type KafkaConfig struct {
	Address  string `ini:"address"`
	Chan_max int    `ini:"chan_max"`
}

type EtcdConfig struct {
	Address string `ini:"address"`
	Key     string `ini:"key"`
	Timeout int    `ini:"timeout"`
}
