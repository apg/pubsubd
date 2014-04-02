package server

/**
 A Feed is just a reference-counted LinkedList backed queue of Items that
 allows for multiple consumers (at different positions).
 */

import (
	"errors"
	"fmt"
	L "github.com/okcupid/logchan"
	M "m8lt"
	P "pubsub"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	)

var defaultCollectionThreshold int64 = 0

var validFeedRegexp *regexp.Regexp = 
	regexp.MustCompile("^([a-zA-Z0-9_-]+)(\\.[a-zA-Z0-9_-]+)*$")

// We keep feeds sorted by name
type FeedCollection struct {
	lock *sync.RWMutex
	feeds []*Feed
	subsByKey Subscriptions
	subsById map[P.SubId]*Subscription
	smallPrefixLen int // in subscriptions, this is the smallest prefix length
}

type FeedName string

type Feed struct {
	lock *sync.RWMutex
	name FeedName
	length int
	head *FeedItem
	tail *FeedItem
	last *FeedItem
}

type FeedItem struct {
	lock *sync.RWMutex
	_next *FeedItem
	_refs int
	_content interface{}
	_time int64
}

func NewFeedCollection() *FeedCollection {
	f := &FeedCollection{new(sync.RWMutex), make([]*Feed, 0),
		make(Subscriptions, 0), make(map[P.SubId]*Subscription), 10}
	return f
}

func (fc *FeedCollection) Len() int {
	return len(fc.feeds)
}

func (fc *FeedCollection) Less(i, j int) bool {
  // We want Pop to give us the highest, not lowest, priority so we
	//use greater than here.
  return fc.feeds[i].name < fc.feeds[j].name
}

func (fc *FeedCollection) Swap(i, j int) {
  fc.feeds[i], fc.feeds[j] = fc.feeds[j], fc.feeds[i]
}

func (fc *FeedCollection) Status() map[string]int {
	status := make(map[string]int)
	fc.lock.RLock()
	defer fc.lock.RUnlock()

	for _, feed := range fc.feeds {
		status[string(feed.name)] = feed.length
	}
	return status
}

func (fc *FeedCollection) PeriodicFeedPurge() {
	if defaultCollectionThreshold > 0 {
		for {
			select {
			case <-time.After(time.Duration(defaultCollectionThreshold * 2) * time.Second):
				for _, feed := range fc.feeds {
					if feed != nil {
						feed.lock.Lock()
						n := feed.collect()
						feed.lock.Unlock()

						M.StatRecord(fmt.Sprintf("feeds.collect.%v",
							strings.Replace(string(feed.name), ".", "-", -1)),
							float64(n))
					}
				}
			}
		}
	}
	P.Logger.Printf(L.LOG_WARN, "defaultCollectionThreshold set to 0. Will not periodically purge feed")
}

// Add a Feed to the collection. We sort feeds by name. Feed creation
// doesn't occur all that often so we take the hit and sort on every insertion
func (fc *FeedCollection) Add(name FeedName) (feed *Feed) {
	fc.lock.Lock()

	if exists, index := fc.findByName(name); !exists {
		feed = NewFeed(name)
		fc.feeds = append(fc.feeds, feed)
		sort.Sort(fc)
		M.StatIncr("pubsub.feeds.add")
	} else {
		feed = fc.feeds[index]
	}

	fc.lock.Unlock()
	return
}

func (fc *FeedCollection) exists(feed *Feed) bool {
	exists, _ := fc.findByName(feed.name)
	return exists
}

func (fc *FeedCollection) findByName(name FeedName) (exists bool, index int) {
	exists = false
	index = -1

	indexTmp := sort.Search(len(fc.feeds), func(i int) bool {
		return fc.feeds[i].name >= name
	})

	if indexTmp < len(fc.feeds) {
		if fc.feeds[indexTmp].name == name {
			exists = true
			index = indexTmp
		}
		return
	}
	return
}

func (fc *FeedCollection) Subscribe(sub *Subscription) (ok bool, err error) {
	// Subscriptions are less frequent than publishes which require a sorted
	// subscription list, as such, we'll sort on insert, and keep sorted
	fc.lock.Lock()
	defer fc.lock.Unlock()

	if _, exists := fc.subsById[sub.id]; exists {
		ok = false
		err = errors.New("already exists")
		P.Logger.Printf(P.LOG_SUBSCRIBE, "subscription %s already exists", sub.id)
	} else {
		// attempt to find by subscription key.
		indexTmp := sort.Search(len(fc.subsByKey), func(i int) bool {
			return Substr(fc.subsByKey[i].key, 0, fc.smallPrefixLen) >= Substr(sub.key, 0, fc.smallPrefixLen)
		})

		if indexTmp < len(fc.subsByKey) {
			// linear search on sub key
			i := 0
			for i = indexTmp; i < len(fc.subsByKey) && Substr(fc.subsByKey[i].key, 0, fc.smallPrefixLen) == Substr(sub.key, 0, fc.smallPrefixLen); i++ {
				if fc.subsByKey[i].key == sub.key && fc.subsByKey[i].subscriber == sub.subscriber {
					P.Logger.Printf(P.LOG_SUBSCRIBE, "subscription for key: %s already exists for subscriber %s",
						sub.key, sub.subscriber.Id())
					ok = false
					err = errors.New("already exists")
					break
				}
			}
		}
	}

	if err == nil {
		ok = true
		fc.subsById[sub.id] = sub
		fc.subsByKey = append(fc.subsByKey, sub)

		if fc.smallPrefixLen > len(sub.keyPrefix) {
			fc.smallPrefixLen = len(sub.keyPrefix)
		}

		sort.Sort(BySubscriptionKey{ fc.subsByKey })
	}

	return
}

func (fc *FeedCollection) Unsubscribe(sub *Subscription) (ok bool, err error) {
	fc.lock.Lock()
	defer fc.lock.Unlock()

	if _, exists := fc.subsById[sub.id]; exists {
		subL := len(fc.subsByKey)
		index := subL // not found.

		// find starting index by key
		indexTmp := sort.Search(subL, func (i int) bool {
			return fc.subsByKey[i].key >= sub.key
		})

		// remove from map
		delete(fc.subsById, sub.id)

		if indexTmp == subL {
			ok = false
			err = errors.New("not found")
		} else if fc.subsByKey[indexTmp].key == sub.key { // found
			// linear search to find the right subscription to kill.
			for i := indexTmp; i < len(fc.subsByKey) && Substr(fc.subsByKey[i].key, 0, fc.smallPrefixLen) == Substr(sub.key, 0, fc.smallPrefixLen); i++ {
				if fc.subsByKey[i].key == sub.key && fc.subsByKey[i].subscriber == sub.subscriber {
					index = i
					break
				}
			}

			if index < subL {
				if index == 0 {
					fc.subsByKey = fc.subsByKey[1:subL]
				} else if index == (subL - 1) {
					fc.subsByKey = fc.subsByKey[0:subL-1]
				} else {
					fc.subsByKey = append(fc.subsByKey[0:index], fc.subsByKey[index+1:subL]...)
				}
			}
			ok = true
		}
	} else {
		ok = false
		err = errors.New("not found")
	}

	return
}

// TODO: does it make sense to only publish a message if someone is subscribed? 
//  If so, we can make publish a no-op when no subscribers would get a message, though
//  this changes the nudge implementation!
func (fc *FeedCollection) Publish(name FeedName, value interface{}) (ok bool, err error) {
	var feed *Feed
	fc.lock.RLock()
	spl := fc.smallPrefixLen
	exists, index := fc.findByName(name)

	// find or create new feed to publish to.
	if !exists {
		if name.IsValid() {
			fc.lock.RUnlock()
			P.Logger.Printf(P.LOG_PUBLISH, "FeedCollection.Publish: Publishing message to unknown feed (%s), creating.", name)
			feed = fc.Add(name)
		} else {
			fc.lock.RUnlock()
			P.Logger.Printf(L.LOG_ERROR, "FeedCollection.Publish: Attempt to publish to invalid feed (%s)", name)
			ok = false
			err = errors.New("invalid feed name")
			return
		}
	} else {
		feed = fc.feeds[index]
		fc.lock.RUnlock()
	}

	// found it, so we just call publish on the feed (TODO: publish to subscribers!)
	P.Logger.Printf(P.LOG_PUBLISH, "FeedCollection.Publish: Publishing message to feed (%s)", name)

	feed.Publish(value)
	cnt := fc.subsByKey.nudge(name, spl)

	// don't keep a message around that cant be delivered, and might never be delivered
	if cnt == 0 {
		P.Logger.Printf(P.LOG_PUBLISH, "collecting %s since there was no one to deliver to", name)
		feed.lock.Lock()
		feed.collect()
		feed.lock.Unlock()
	}
	ok = true

	return
}

func (fc *FeedCollection) Get(name FeedName) *Feed {
	fc.lock.RLock()
	defer fc.lock.RUnlock()

	if exists, index := fc.findByName(name); exists {
		return fc.feeds[index]
	}
	return nil
}

func (fc *FeedCollection) String() string {
	fc.lock.RLock()
	defer fc.lock.RUnlock()

	bits := make([]string, 0)
	bits = append(bits, "FeedCollection {")
	for i, feed := range fc.feeds {
		bits = append(bits, fmt.Sprintf(" Feed[%d]{%s, %d} ", i, feed.name, feed.Len()))
	}
	for i, sub := range fc.subsByKey {
		bits = append(bits, fmt.Sprintf(" Subscription[%d]{%s, %s} ", i, sub.key, sub.subscriber))
	}
	bits = append(bits, "}")
	return strings.Join(bits, "")
}

func (f FeedName) IsValid() bool {
	return validFeedRegexp.MatchString(string(f))
}

func NewFeed(name FeedName) *Feed {
	proxy := &FeedItem{new(sync.RWMutex), nil, 0, nil, 0}
	return &Feed{new(sync.RWMutex), name, 0, proxy, proxy, proxy}
}

func (f *Feed) Len() (ret int) {
	f.lock.RLock()
	ret = f.length
	f.lock.RUnlock()
	return
}

func (f *Feed) Last() (ret *FeedItem) {
	f.lock.Lock()

	f.last.Incr()
	f.collect()
	ret = f.last

	f.lock.Unlock()
	return
}

func (f *Feed) Publish(value interface{}) {
	f.lock.Lock()

	oldProxy := f.tail
	newProxy := &FeedItem{new(sync.RWMutex), nil, 0, nil, 0}
	oldProxy.setContent(value)
	oldProxy.setNext(newProxy)
	oldProxy.setTime(time.Now().Unix())
	f.last = oldProxy
	f.tail = newProxy
	f.length += 1

	f.lock.Unlock()
}

// It's *very* important to call Decr() since this Incr() the ref count
func (f *Feed) Head() (ret *FeedItem) {
	f.lock.RLock()

	f.head.Incr()
	ret = f.head

	f.lock.RUnlock()
	return
}

func (f *Feed) FirstAt(t int64) (ret *FeedItem) {
	n := f.head

	P.Logger.Printf(L.LOG_DEBUG, "Head is: %v", n)

	f.lock.Lock()
	for n != f.tail && n._next != nil {
		P.Logger.Printf(L.LOG_DEBUG, "Is %v newer or equal to %v?", n, t)
		if n._time >= t {
			ret = n
			ret.Incr()
			break
		}
		n = n._next
	}
	f.lock.Unlock()
	return
}

// Returns a *new* FeedItem that is a copy of head, in order to retain
// proper reference counts, et. al.
func (f *Feed) Peek() (ret *FeedItem) {
	f.lock.RLock()
	ret = &FeedItem{new(sync.RWMutex), f.head.next(), f.head.Refs(), f.head.Content(), f.head.Time()}
	f.lock.RUnlock()
	return
}

func (f *Feed) HasNext(item *FeedItem) (ret bool) {
	f.lock.RLock()
	ret = f.hasNext(item)
	f.lock.RUnlock()
	return
}

func (f *Feed) hasNext(item *FeedItem) bool {
	if item != nil {
		return item.next() != nil
	}
	return false
}

func (f *Feed) Next(item *FeedItem) (next *FeedItem) {
	f.lock.Lock()
	if f.hasNext(item) {
		item.Decr()
		next = item.next()
		next.Incr()
		f.collect()
	} else {
		next = f.tail
		next.Incr()
	}
	f.lock.Unlock()
	return
}

func (f *Feed) collect() int {
	return f.collectOlderThan(defaultCollectionThreshold)
}

func (f *Feed) collectOlderThan(olderThan int64) int {
	oldest := time.Now().Unix() - olderThan
	cnt := 0

	for {
		if f.length > 0 {
			// e.g. it's not the proxy!
			if f.head.Time() > oldest {
				break
			} else if f.head.Refs() == 0 && f.hasNext(f.head) {
				f.head = f.head.next()
				f.length--
				cnt++
				continue
			}
		}
		break
	}

	return cnt
}

func (m *FeedItem) Time() (ret int64) {
	m.lock.RLock()
	ret = m._time
	m.lock.RUnlock()
	return
}

func (m *FeedItem) Refs() (ret int) {
	m.lock.RLock()
	ret = m._refs
	m.lock.RUnlock()
	return
}

func (m *FeedItem) Incr() (ret int) {
	m.lock.Lock()
	m._refs++
	ret = m._refs
	m.lock.Unlock()
	return
}

func (m *FeedItem) Decr() (ret int) {
	m.lock.Lock()

	if m._refs > 0 {
		m._refs--
	}
	ret = m._refs

	m.lock.Unlock()
	return
}

func (m *FeedItem) setNext(n *FeedItem) {
	m.lock.Lock()
	m._next = n
	m.lock.Unlock()
}

func (m *FeedItem) setTime(n int64) {
	m.lock.Lock()
	m._time = n
	m.lock.Unlock()
}

func (m *FeedItem) next() (ret *FeedItem) {
	m.lock.RLock()
	ret = m._next
	m.lock.RUnlock()
	return
}

func (m *FeedItem) setContent(c interface{}) {
	m.lock.Lock()
	m._content = c
	m.lock.Unlock()
}

func (m *FeedItem) Content() (ret interface{}) {
	m.lock.Lock()
	ret = m._content
	m.lock.Unlock()
	return
}

func (m *FeedItem) String() string {
	return fmt.Sprintf("{time: %d, refs: %d, content: %v}", m.Time(), m.Refs(), m.Content())
}

func SetCollectionThreshold(n int64) {
	defaultCollectionThreshold = n
}

func CollectionThreshold() int64 {
	return defaultCollectionThreshold
}