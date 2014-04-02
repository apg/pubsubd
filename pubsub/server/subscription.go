package server

import (
	"fmt"
	L "github.com/okcupid/logchan"
	P "pubsub"
	"regexp"
	"sort"
	"strings"
	"time"
)

type Notification struct {
	Name FeedName
	Time int64
}

type Subscription struct {
	id P.SubId
	key string
	keyPrefix string // before the wildcard if any...
	keyRx *regexp.Regexp
	subscriber Subscriber
}

type Subscriptions []*Subscription

type Subscriber interface {
	Id() P.SubId
	Notify(note Notification) bool
}

type BySubscriptionKey struct { Subscriptions }
type BySubscriptionId struct { Subscriptions }

func (b BySubscriptionKey) Less(i, j int) bool {
	return b.Subscriptions[i].keyPrefix < b.Subscriptions[j].keyPrefix
}

func (b BySubscriptionId) Less(i, j int) bool {
	return b.Subscriptions[i].id < b.Subscriptions[j].id
}

func (s Subscription) Id() P.SubId {
	return s.id
}

func (s Subscriptions) Len() int {
	return len(s)
}

func (s Subscriptions) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Subscription) Key() string {
	return s.key
}

func (s Subscription) String() string {
	return fmt.Sprintf("Subscription{%s, key=%s}", s.id, s.key)
}

func NewSubscription(id P.SubId, key string, subscriber Subscriber) *Subscription {
	var err error
	var keyRx *regexp.Regexp
	keyPrefix := key

	if idx := strings.Index(key, "*"); idx > -1 {
		keyPrefix = key[0:idx]
		if keyRx, err = P.CompileSubscriptionKey(key); err != nil {
			P.Logger.Printf(P.LOG_SUBSCRIBE, "NewSubscription: subscription key (%s) is not a" +
				" valid wildcard", key)
		}
	}
	return &Subscription{id, key, keyPrefix, keyRx, subscriber}
}

// Notify feeds that there are things available!
func (ss Subscriptions) nudge(fname FeedName, searchLen int) (cnt int) {
	sL := len(ss)
	name := string(fname)
	note := Notification{fname, time.Now().Unix()}

	// TODO: we can pick a better starting value, but this is easiest for now.
  start := sort.Search(sL, func (i int) bool {
		return Substr(ss[i].keyPrefix, 0, searchLen) >= Substr(name, 0, searchLen)
	})

	if start < sL {
		for i := start; i < sL; i++ {
			if Substr(name, 0, searchLen) != Substr(ss[i].keyPrefix, 0, searchLen) {
				break
			}
			if (ss[i].keyRx != nil && ss[i].keyRx.MatchString(name)) || ss[i].key == name {
				P.Logger.Printf(L.LOG_DEBUG, "Nudging %s because of %s\n", ss[i].id, name)
				ss[i].subscriber.Notify(note)
				cnt++
			}
		}
	}

	return
}


// I have no clue why Go doesn't have a Substr function that works
// like this...
func Substr(s string, start, end int) string {
	l := len(s)
	if end < 0 {
		end = l + end
	}
	if start > l {
		return ""
	} else if start > end {
		return ""
	} else if end > l {
		end = l
	}

	return s[start:end]
}