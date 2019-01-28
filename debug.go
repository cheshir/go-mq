package mq

var Printer chan string

func init() {
	Printer = make(chan string)

	go func() {
		for m := range Printer {
			println(":", m)
		}
	}()
}
