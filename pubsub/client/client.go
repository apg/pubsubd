package client

import (
	"bufio"
	"github.com/okcupid/jsonw"
	L "github.com/okcupid/logchan"
	M "github.com/okcupid/mpack"
	"net"
	P "pubsub"
)

type Subscription struct {
  feed string
	cb func(string, jsonw.Wrapper)
}

type PubSubClient struct {
	clientPool *mpack.ClientPool
  subscriptions []Subscription
}

func NewPubSubClient(pool *mpack.ClientPool) *PubSubClient {
	return &PubSubClient{
    pool,
    make([]Subscription, 10)}
}

func (ps PubSubClient) Publish(feed string, msg jsonw.Wrapper) (ok bool, err error) {
	// TODO: Call the publish method

	if cli, errcp := ps.clientPool.Get(); errcp != nil {
		ok = false
		err = errcp
	} else {
		defer ps.clientPool.Put(cli)

		arg := jsonw.NewDictionary()
		arg.SetKey("feed", jsonw.NewString(feed))
		arg.SetKey("message", msg)

		if res, err := cli.CallSync("property.1.get_external_property", arg); err == nil {

	}
	return

}

func (ps *PubSubClient) Subscribe(feed string, ch mpack.ReplyPairChan) (ok bool, err error) {
  fmt.Println("in Subscribe")
  sub := Subscription{feed, ch, 0}
  ps.subscriptions = append(ps.subscriptions, sub)

  if ps.looping == false {
    ps.looping = true
    go ps.checkLoop()
  }
}

