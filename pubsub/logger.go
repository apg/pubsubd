package pubsub

import (
	L "github.com/okcupid/logchan"
	)

const (
	LOG_PUBLISH L.Level = 0x1
	LOG_SUBSCRIBE L.Level = 0x2
	LOG_STREAM L.Level = 0x4
)

const (
	CHANNEL_PUBLISH byte = 'P'
	CHANNEL_SUBSCRIBE byte = 'S'
	CHANNEL_STREAM byte = 's'
)

var Logger *L.Logger = L.NewLogger([]L.Channel {
	L.Channel{LOG_PUBLISH, CHANNEL_PUBLISH, "publish"},
	L.Channel{LOG_STREAM, CHANNEL_STREAM, "stream"},
	L.Channel{LOG_SUBSCRIBE, CHANNEL_SUBSCRIBE, "subscribe"}}, L.LOG_ERROR)
