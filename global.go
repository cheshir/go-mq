// Just for that guys who love to keep everything in the global scope.
package mq

var mq MQer

func NewGlobal(config Config) (err error) {
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
