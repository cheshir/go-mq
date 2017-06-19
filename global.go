// Just for that guys who love to keep all in the global scope.
package mq

import "github.com/spf13/viper"

var mq MQer

func NewGlobal(config *viper.Viper) (err error) {
	mq, err = New(config)

	return
}

func GetProducer(name string) (Producer, bool) {
	return mq.GetProducer(name)
}

func Close() {
	mq.Close()
	mq = nil
}
