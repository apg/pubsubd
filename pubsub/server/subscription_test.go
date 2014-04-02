package server

import (
	"fmt"
	P "pubsub"
	"sort"
	"strings"
	"testing"
)

var feedNames []string = []string{
	"foo.bar", "foo.baz", "foo.quux", "foo.baz.bag",
	"help.me", "help.johnny", "help", "foof" }

var testKeys []string = []string{
	"foo.*", "foo.bar",
	"help.*", "event*.bar"}

type TrackSubscriber struct {
	id P.SubId
  err []string
	recv map[FeedName]bool
}

func (e TrackSubscriber) Id() P.SubId {
	return e.id
}

func (e TrackSubscriber) Notify(f Notification) bool {
	// if not already marked as received, mark it!
	// if marked and recieved, fail

	if val, ok := e.recv[f.Name]; !ok {
		e.recv[f.Name] = true
	} else if val {
		e.err = append(e.err, fmt.Sprintf("%s is already marked", f))
	} else {
		e.err = append(e.err, fmt.Sprintf("%s is unexpected", f))
	}
	return true
}


func TestSubscriptionSort(t *testing.T) {
	subs := make(Subscriptions, 0)

	for _, key := range testKeys {
		subs = append(subs, NewSubscription(P.RandomSubId(), key, nil))
	}

	// sort by subscription key
	sort.Sort(BySubscriptionKey{subs})

	for i := 0; i < len(subs) - 1; i++ {
		if subs[i].key > subs[i+1].key {
			t.Errorf("%s > %s, which means we're not sorted!", subs[i].key, subs[i+1].key)
		}
	}

	// sort by subscription key
	sort.Sort(BySubscriptionId{subs})

	for i := 0; i < len(subs) - 1; i++ {
		if subs[i].id > subs[i+1].id {
			t.Errorf("%s > %s, which means we're not sorted!", subs[i].id, subs[i+1].id)
		}
	}
}

// test's nudging of client notification
func TestSubscriptionNudge(t *testing.T) {
	coll := NewFeedCollection()

	recv := map[FeedName]bool{
		FeedName("foo.bar"): false, 
		FeedName("foo.baz"): false,
		FeedName("foo.quux"): false,
		FeedName("foo.baz.bag"): false,
		FeedName("help.me"): false, 
		FeedName("help.johnny"): false }

	subscrbr := &TrackSubscriber{P.RandomSubId(), make([]string, 0), recv}

	// setup subscriptions
	for _, f := range testKeys {
		coll.Subscribe(NewSubscription(P.RandomSubId(), f, subscrbr))
	}

	for i, f := range feedNames {
		msg := fmt.Sprintf("Message #%d", i)
		coll.Publish(FeedName(f), msg)
	}

	if len(subscrbr.err) > 0 {
		t.Errorf("%s", strings.Join(subscrbr.err, "; "))
	}
}

