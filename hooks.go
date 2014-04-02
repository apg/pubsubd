package main

import (
	"fmt"
	"github.com/okcupid/jsonw"
	L "github.com/okcupid/logchan"
	M "m8lt"
	"net"
	P "pubsub"
	S "pubsub/server"
)

func (srv *Server) H_set_debug(a jsonw.Wrapper, conn net.Conn) (r jsonw.Wrapper) {
	if channels, err := a.AtKey("pubsub").GetString(); err == nil {
		P.Logger.SetChannelsEasy("pubsub", channels, true)
	}

	if channels, err := a.AtKey("pubsubd").GetString(); err == nil {
		L.SetChannelsEasy("pubsubd", channels, true)
	}

	if channels, err := a.AtKey("rpc").GetString(); err == nil {
		srv.Mps.Logger.SetChannelsEasy("rpc", channels, true)
	}

  srv.Status.SetCode(&r, "OK", "")

	return
}

func (srv *Server) H_get_status(a jsonw.Wrapper, conn net.Conn) (r jsonw.Wrapper) {
	// need more meaningful status here, number of messages per feed, active subscriptions, etc..
	r = *jsonw.NewDictionary()

	status := jsonw.NewDictionary()

	status.SetKey("total_messages", jsonw.NewUint64(srv.totalMsgCount))

	feedStatus := jsonw.NewDictionary()

	fstat := srv.feeds.Status()

	for key, val := range fstat {
		feedStatus.SetKey(key, jsonw.NewInt(val))
	}
	status.SetKey("feeds", feedStatus)

	subStatus := jsonw.NewDictionary()

	srv.lock.Lock()
	sstat := SubscriberMapStatus(srv.activeMap)
	srv.lock.Unlock()

	status.SetKey("subscribers", subStatus)
	for scrid, subscrib := range sstat {
		a := jsonw.NewDictionary()
		for ripid, key := range subscrib {
			a.SetKey(ripid, jsonw.NewString(key))
		}
		subStatus.SetKey(scrid, a)
	}

	dieStatus := jsonw.NewDictionary()

	srv.lock.Lock()
	dstat := SubscriberMapStatus(srv.timeoutMap)
	srv.lock.Unlock()

	status.SetKey("dying", dieStatus)
	for scrid, subscrib := range dstat {
		a := jsonw.NewDictionary()
		for ripid, key := range subscrib {
			a.SetKey(ripid, jsonw.NewString(key))
		}
		dieStatus.SetKey(scrid, a)
	}
	r.SetKey("status", status)

  return
}

func (srv *Server) H_subscribe(a jsonw.Wrapper, conn net.Conn) (r jsonw.Wrapper) {
	// takes a subscriber_hexid and subscription_key
	// looks up subscriber_hexid in the subscriber maps, and if found
	// setups a subscription to them

	// find the socketSubscriber according to subscriber_hexid.
  // if it exists, create a subscription for subscription_key
	// add it to socketSubscriber, and add it to the feeds

	r = *jsonw.NewDictionary()

	var ok bool
	var subId, scripId P.SubId
	var rawSubId, subKey string
	var sockSub *SocketSubscriber
	var err error

	if rawSubId, err = a.AtKey("subscriber_hexid").GetString(); err != nil {
		srv.Status.SetCode(&r, "E_MISSING_QUERY", "subscriber_hexid is required")
		return
	} else if subId, err = P.ToSubId(rawSubId); err != nil {
		srv.Status.SetCode(&r, "E_MISSING_QUERY", "subscriber_hexid is not valid")
		return
	}

	if subKey, err = a.AtKey("subscription_key").GetString(); err != nil {
		srv.Status.SetCode(&r, "E_MISSING_QUERY", "subscription_key is required")
		return
	} else if valid := P.IsValidSubscriptionKey(subKey); !valid {
		srv.Status.SetCode(&r, "E_MISSING_QUERY", "subscription_key is not valid")
		return
	}

	srv.lock.Lock()
	if sockSub, ok = srv.activeMap[subId]; !ok {
		if sockSub, ok = srv.timeoutMap[subId]; !ok {
			srv.Status.SetCode(&r, "E_NOT_FOUND", "subscriber_hexid was not found")
			srv.lock.Unlock()
			M.StatIncr("subscription.subscriber_not_found")
			return
		}
	}
	srv.lock.Unlock()

	scripId = P.RandomSubId()

	subscription := S.NewSubscription(scripId, subKey, sockSub)

	if _, err := srv.feeds.Subscribe(subscription); err != nil {
		srv.Status.SetCode(&r, "E_ALREADY_EXISTS", "subscription already exists")
		M.StatIncr("subscription.already_exists")
	} else {
		sockSub.addSubscription(subscription)
		// ok, we're subscribed, send success!
		r.SetKey("subscription_hexid", jsonw.NewString(scripId.String()))
		M.StatIncr("subscription.success")
	}

	return
}

func (srv *Server) H_unsubscribe(a jsonw.Wrapper, conn net.Conn) (r jsonw.Wrapper) {
	// takes a subscription_hexid, and unsubscribes it.
	r = *jsonw.NewDictionary()

	var subId, scripId P.SubId
	var rawSubId, rawScripId string
	var err error
	var sockSub *SocketSubscriber
	var ok bool

	if rawSubId, err = a.AtKey("subscriber_hexid").GetString(); err != nil {
		srv.Status.SetCode(&r, "E_MISSING_QUERY", "subscriber_hexid is required")
		return
	} else if subId, err = P.ToSubId(rawSubId); err != nil {
		srv.Status.SetCode(&r, "E_MISSING_QUERY", "subscriber_hexid is not valid")
		return
	}

	if rawScripId, err = a.AtKey("subscription_hexid").GetString(); err != nil {
		srv.Status.SetCode(&r, "E_MISSING_QUERY", "subscription_hexid is required")
		return
	} else if scripId, err = P.ToSubId(rawScripId); err != nil {
		srv.Status.SetCode(&r, "E_MISSING_QUERY", "subscription_hexid is not valid")
		return
	}

	// TODO: factor this out, we're using it in subscribe too!
	srv.lock.Lock()
	if sockSub, ok = srv.activeMap[subId]; !ok {
		if sockSub, ok = srv.timeoutMap[subId]; !ok {
			srv.Status.SetCode(&r, "E_NOT_FOUND", "subscriber_hexid was not found")
			srv.lock.Unlock()
			M.StatIncr("subscription.subscriber_not_found")

			return
		}
	}
	srv.lock.Unlock()

	if subscription := sockSub.removeSubscription(scripId); subscription != nil {
		srv.feeds.Unsubscribe(subscription)
	} else {
		srv.Status.SetCode(&r, "E_NOT_FOUND", "subscription_hexid was not found")
		M.StatIncr("subscription.not_found")
		return
	}

	M.StatIncr("subscription.unsubscribe")
	r.SetKey("unsubscribed", jsonw.NewString(rawScripId))

	return
}

func (srv *Server) H_publish(a jsonw.Wrapper, conn net.Conn) (r jsonw.Wrapper) {
	// takes a feed(string), message(json, which will be msgpack 
	// encoded before delivery) returns success or failure in some
	// way... not sure :)

	// NOTE: Publish needs to know absolutely nothing about anything
	// except for the message the the feed to publish too

	r = *jsonw.NewDictionary()

	var rawFeed string
	var err error
	var incomingMessage, outMessage interface{}

	if rawFeed, err = a.AtKey("feed").GetString(); err != nil {
		srv.Status.SetCode(&r, "E_MISSING_QUERY", "feed is required")
		return
	} else if ok := S.FeedName(rawFeed).IsValid() ; !ok {
		srv.Status.SetCode(&r, "E_MISSING_QUERY", "feed is not valid")
		return
	}

	feed := S.FeedName(rawFeed)

	if incomingMessage, err = a.AtKey("message").GetData(); err != nil {
		srv.Status.SetCode(&r, "E_MISSING_QUERY", "message is required")
		return
	}

	msg := *jsonw.NewDictionary()
	msg.SetKey("id", jsonw.NewString(P.RandomSubId().String()))
	msg.SetKey("feed", jsonw.NewString(string(feed)))
	msg.SetKey("message", jsonw.NewWrapper(incomingMessage))

	// now, we should get data on everything and publish it!
	if outMessage, err = msg.GetData(); err != nil {
    srv.Status.SetCode(&r, "E_UNKNOWN", fmt.Sprintf("unknown error: %s", err))
		return
	}

	// OK, do some publishing!
	if _, err := srv.feeds.Publish(feed, outMessage); err != nil {
		srv.Status.SetCode(&r, "E_NOT_FOUND", fmt.Sprintf("%s", err))
		return
	}

	srv.totalMsgCount++

	M.StatIncr("subscription.publish")
  srv.Status.SetCode(&r, "OK", "")

	return
}

func (srv *Server) H_null(a jsonw.Wrapper, conn net.Conn) (jsonw.Wrapper) {
	return a
}
