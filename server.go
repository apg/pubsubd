package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/okcupid/jsonw"
	L "github.com/okcupid/logchan"
	"github.com/okcupid/mpack"
	"io"
	"log"
	M "m8lt"
	"net"
	P "pubsub"
	S "pubsub/server"
	"reflect"
	"strings"
	"sync"
	"time"
)

const backfillSeconds int64 = 5

type SubStatus int8

const (
	LOG_NETWORK L.Level = 0x10
	LOG_CLIENT  L.Level = 0x20
	LOG_PURGE   L.Level = 0x40
	LOG_PING   L.Level = 0x80

	CHANNEL_NETWORK byte = 'N'
	CHANNEL_CLIENT  byte = 'C'
	CHANNEL_PURGE   byte = 'P'
	CHANNEL_PING   byte = 'p'

	STATUS_PURGED SubStatus = -1
	STATUS_TIMEOUT SubStatus = 0
	STATUS_ACTIVE SubStatus = 1
)

type Server struct {
	M.BaseServer
	streamHost    string
	feeds         *S.FeedCollection
	activeMap     SubscriberMap
	timeoutMap    SubscriberMap
	totalMsgCount uint64
	lock          *sync.Mutex
}

// this gets dropped for a SocketSubscriber as soon as possible
type ServerConn struct {
	conn net.TCPConn
	srv  *Server
}

type SocketSubscriber struct {
	id            P.SubId
	conn          []net.TCPConn // reconnections might reestablish here..
	isConnected   bool

	notifications chan S.Notification
	reap chan bool
	disconnects chan bool
	connectionAdds chan net.TCPConn

	feedMap       FeedMap
	subscriptions map[P.SubId]*S.Subscription
	mtime         time.Time
	srv           *Server
	lock          *sync.RWMutex
}

type SubscriberMap map[P.SubId]*SocketSubscriber
type FeedMap map[S.FeedName]*S.FeedItem

var pingPacket []byte = makePingPacket()

func NewServer(c string, rpcHost string, rpcPort int, streamHost string, streamPort int) *Server {
	ret := new(Server)
	ret.streamHost = fmt.Sprintf("%s:%d", streamHost, streamPort)
	ret.feeds = S.NewFeedCollection()
	ret.activeMap = make(SubscriberMap)
	ret.timeoutMap = make(SubscriberMap)
	ret.lock = new(sync.Mutex)

	ret.InitBaseServer(c, rpcHost, rpcPort)
	ret.DiscoverHooks("pubsub", 1)

	S.SetCollectionThreshold(5) // hold on to messages for 5 seconds if no subscribers

	L.AddChannels(L.Channels{
		L.Channel{LOG_NETWORK, CHANNEL_NETWORK, "network"},
		L.Channel{LOG_CLIENT, CHANNEL_CLIENT, "client"},
		L.Channel{LOG_PURGE, CHANNEL_PURGE, "purge"},
		L.Channel{LOG_PING, CHANNEL_PING, "ping"}})

	return ret
}

// Note: This would be a great thing to add to m8lt, if only it were
// possible. Of course the problem is that there's no way to get the
// `Server` methods from a `BaseServer.` Some trickery with interfaces
// could probably be done... but the effort to do so would be exhausting
// and unrewarded.
func makeDiscoveredHook(meth reflect.Value, s *Server) M.Hook {
	return func(a jsonw.Wrapper, conn net.Conn) jsonw.Wrapper {
		var ret jsonw.Wrapper
		var ok bool
		result := meth.Call([]reflect.Value{reflect.ValueOf(s), reflect.ValueOf(a), reflect.ValueOf(conn)})
		if ret, ok = result[0].Interface().(jsonw.Wrapper); !ok {
			L.Printf(L.LOG_ERROR, "Whoops, type assertion failed in call to hook")
		}
		return ret
	}
}

func (srv *Server) DiscoverHooks(prog string, vers int) {
	hooks := make(M.BoundHooks, 0)

	typeof := reflect.TypeOf(srv)
	numMethods := typeof.NumMethod()

	for i := 0; i < numMethods; i++ {
		meth := typeof.Method(i)

		if strings.HasPrefix(meth.Name, "H_") {
			L.Printf(L.LOG_DEBUG, "+ discovered hook: %s", meth.Name)
			proc := meth.Name[2:]
			hook := makeDiscoveredHook(meth.Func, srv)
			hooks = append(hooks, M.BoundHook{proc, hook})
		}
	}

	if len(hooks) > 0 {
		srv.AddHooks(prog, vers, hooks)
	}
}

func (srv *Server) Run() {
	var err error
	if err = srv.BaseRun(); err == nil {
		M.StatIncr("start")

		go srv.feeds.PeriodicFeedPurge()
		go srv.periodicSubscriberPurge()
		go srv.Mps.ListenAndServe()
		err = srv.streamServerLoop()
	}

	if err != nil {
		log.Printf("Server exited with error: %s\n", err)
	} else {
		log.Printf("Server exited normally\n")
	}
}

func NewServerConn(conn net.TCPConn, srv *Server) *ServerConn {
	return &ServerConn{conn, srv}
}

func NewSocketSubscriber(id P.SubId, conn net.TCPConn, srv *Server) *SocketSubscriber {
	// TODO: determine if 100 in feedName channel is correct
	ret := new(SocketSubscriber)
	ret.id = id
	ret.conn = []net.TCPConn{conn}
	ret.isConnected = true
	ret.reap = make(chan bool, 1)
	ret.notifications = make(chan S.Notification, 100)
	ret.connectionAdds = make(chan net.TCPConn, 10)
	ret.disconnects = make(chan bool, 100)

	ret.feedMap = make(FeedMap)
	ret.subscriptions = make(map[P.SubId]*S.Subscription)
	ret.mtime = time.Now()
	ret.srv = srv
	ret.lock = new(sync.RWMutex)

	return ret
}

func (srv *Server) statusOf(sid P.SubId) (status SubStatus, ret *SocketSubscriber) {
	var ok bool
	if ret, ok = srv.timeoutMap[sid]; ok {
		status = STATUS_TIMEOUT
	} else if ret, ok = srv.activeMap[sid]; ok {
		status = STATUS_ACTIVE
	} else {
		status = STATUS_PURGED
	}
	return
}

func (srv *Server) activeSubscriber(sid P.SubId, sock *SocketSubscriber) {
	srv.lock.Lock()
	if _, ok := srv.activeMap[sid]; !ok {
		if _, ok := srv.timeoutMap[sid]; !ok {
			srv.activeMap[sid] = sock
		} else {
			srv.activeMap[sid] = sock
			delete(srv.timeoutMap, sid)
		}
	}
	srv.lock.Unlock()
	return
}

func (srv *Server) timeoutSubscriber(sid P.SubId, sock *SocketSubscriber) {
	srv.lock.Lock()
	if _, ok := srv.timeoutMap[sid]; !ok {
		if _, ok := srv.activeMap[sid]; !ok {
			srv.timeoutMap[sid] = sock
		} else {
			srv.timeoutMap[sid] = sock
			delete(srv.activeMap, sid)
		}
	}
	srv.lock.Unlock()
	return
}


func (srv *Server) purgeSubscriber(sid P.SubId) {
	srv.lock.Lock()
	delete(srv.timeoutMap, sid)
	delete(srv.activeMap, sid)
	srv.lock.Unlock()
	M.StatIncr("subscriber.purge")
}

// OK
func (srv *Server) periodicSubscriberPurge() {
	for {
		select {
		case <-time.After(1 * time.Minute):
			L.Printf(L.LOG_DEBUG, "PURGE woke up")
			t := time.Now()
			srv.lock.Lock()
			if len(srv.timeoutMap) > 0 {
				L.Printf(L.LOG_DEBUG, "PURGE len(timeout map) > 0: %v", len(srv.timeoutMap))
				for subId, sockSub := range srv.timeoutMap {
					if sockSub.mtime.Add(1 * time.Minute).Before(t) {
						// purge!
						L.Printf(LOG_PURGE, "Purging subscriber (%s) due to timeout",
							subId)
						sockSub.purge()
						delete(srv.timeoutMap, subId)
					}
				}
			}
			srv.lock.Unlock()
		}
	}
}

// OK
func (srv *Server) streamServerLoop() error {
	var listener *net.TCPListener

	tcpAddr, err := net.ResolveTCPAddr("tcp", srv.streamHost)

	if err != nil {
		log.Printf("error resolving address %s: %s", srv.streamHost, err)
	} else {
		listener, err = net.ListenTCP(tcpAddr.Network(), tcpAddr)
		if err != nil {
			log.Printf("error listening: %s", err)
		}
	}

	if err == nil {
		L.Printf(LOG_NETWORK,
			"Starting stream server run loop, listening on %s", srv.streamHost)

		for {
			conn, err := listener.AcceptTCP()
			if err != nil {
				L.Printf(L.LOG_ERROR, "accept error: %v", err)
			} else {
				sc := NewServerConn(*conn, srv)
				go sc.serve()
				M.StatIncr("subscriber.connect")
			}
		}

		listener.Close()
	}
	return err
}

// OK, well, almost. The write's could be taking forever. Might be best
// to put deadlines on them to ensure they timeout properly.
//
// TODO: locking related stuff with makeActive, etc.
func (sc *ServerConn) serve() {
	// read a line to see how to proceed, and upgrade to a SocketSubscriber
	// if required
	var line []byte
	var subId P.SubId
	var err error
	var sockSub *SocketSubscriber
	var status SubStatus

	reader := bufio.NewReader(&sc.conn)
	if line, _, err = reader.ReadLine(); err == nil {
		bits := splitAndStrip(string(line), " ")
		if len(bits) == 2 && bits[0] == "RECONNECT" {
			if subId, err = P.ToSubId(bits[1]); err != nil {
				L.Printf(LOG_CLIENT, "invalid subscriber id")
				sc.conn.Write([]byte("ERROR unable to parse subscriber id\r\n"))
				sc.conn.Close()
				return

			} else {
				status, sockSub = sc.srv.statusOf(subId)
				if status == STATUS_PURGED {
					sc.conn.Write([]byte(fmt.Sprintf("PURGED %s\r\n", subId)))
					sc.conn.Close()
					M.StatIncr("subscriber.was_purged")
					return
				} else if status == STATUS_ACTIVE {
					// mirrored subscription
					L.Printf(LOG_CLIENT, "reconnect attempted with subscriber already in use")
					M.StatIncr("subscriber.mirrored")
				} else {
					sc.srv.activeSubscriber(subId, sockSub)
					M.StatIncr("subscriber.reconnect")
				}

				sockSub.addConnection(sc.conn)
				sc.conn.Write([]byte(fmt.Sprintf("OK %s\r\n", subId)))
			}
		} else if len(bits) == 1 && bits[0] == "CONNECT" {
			subId = P.RandomSubId()
			L.Printf(LOG_CLIENT, "creating new subscriber (%s)", subId.String())
			sockSub = NewSocketSubscriber(subId, sc.conn, sc.srv)
			sc.srv.activeSubscriber(subId, sockSub)
			sc.conn.Write([]byte(fmt.Sprintf("OK %s\r\n", subId)))
			M.StatIncr("subscriber.new")
			go sockSub.serve()
		} else {
			L.Printf(LOG_CLIENT, "unknown command sent")
			sc.conn.Write([]byte("ERROR unknown command\r\n"))
			sc.conn.Close()
		}
	} else {
		L.Printf(L.LOG_ERROR, "readline returned error: %v", err)
		sc.conn.Close()
	}

	return
}

func (s *SocketSubscriber) serve() {
	var flushReadyFeeds bool
	var err error

	readyFeeds := make(map[S.FeedName]bool)

	L.Printf(L.LOG_INFO, "SERVE: subscriberId=%v", s.id)

	for {
		flushReadyFeeds = false
		select {
		// request to terminate this loop, and the subscriber
		case <-s.reap: 
			L.Printf(L.LOG_INFO, "REAP: subscriberId=%s", s.id)
			s.srv.purgeSubscriber(s.id)

			s.isConnected = false
			s.lock.Lock()
			for _, c := range s.conn {
				// Attempt to read (hopefully get EOF)
				e := HardClose(c)
				if e != nil {
					L.Printf(L.LOG_DEBUG, "REAP: subscriberId=%s: hard close resulted in error: %v", s.id, e)
				}

			}

			// remove subscriptions
			for id, sub := range s.subscriptions {
				L.Printf(L.LOG_INFO, "subscriberId=%v is unsubscribing from %v", s.id, id)
				s.srv.feeds.Unsubscribe(sub)
				delete(s.subscriptions, id)
			}

			for name, item := range s.feedMap {
				L.Printf(L.LOG_INFO, "subscriberId=%v releasing %v", s.id, name)
				item.Decr()
				delete(s.feedMap, name)
			}

			s.lock.Unlock()

			return

		// purge connections and tell server to start timeout process
		case <-s.disconnects:
			L.Printf(L.LOG_INFO, "DISCONNECT: subscriberId=%s", s.id)

			s.isConnected = false
			s.lock.Lock()
			for _, c := range s.conn {
				e := HardClose(c)
				if e != nil {
					L.Printf(L.LOG_DEBUG, "DISCONNECT: subscriberId=%s: hard close resulted in error: %v", s.id, e)
				}
			}
			s.conn = s.conn[0:0]
			s.lock.Unlock()
			s.srv.timeoutSubscriber(s.id, s)

		case newConn := <-s.connectionAdds:
			L.Printf(L.LOG_INFO, "NEW_CONN: subscriberId=%s, conn=%v", s.id, newConn)
			s.lock.Lock()
			s.conn = append(s.conn, newConn)
			s.lock.Unlock()

			s.isConnected = true
			s.srv.activeSubscriber(s.id, s)
			s.mtime = time.Now()

			flushReadyFeeds = true

		// notification that feed has something new
		case notify := <-s.notifications:
			L.Printf(L.LOG_INFO, "NOTIFY: subscriberId=%s, feed=%v", s.id, notify)
			if err = s.notifiedFlush(notify); err == nil {
				if s.isConnected {
					flushReadyFeeds = true
					s.mtime = time.Now()
				} else {
					L.Printf(L.LOG_INFO, "Subscriber %v is disconnected, marking feed %v as ready", s.id, notify.Name)
				}
				readyFeeds[notify.Name] = true
			} else {
				L.Printf(L.LOG_ERROR, "Error while flushing %v (disconnecting): %v", notify, err)
				s.disconnect()
			}

		// ping! or flush if things are ready
		case <-time.After(3 * time.Second):
			// ping or flush waiting feeds.
			if s.isConnected {
				if len(readyFeeds) > 0 {
					flushReadyFeeds = true
				} else if err = s.sendPing(); err != nil {
					M.StatIncr("ping.error")
					s.disconnect()
				} else {
					s.mtime = time.Now()
				}
			}
		}

		if flushReadyFeeds && s.isConnected {
			L.Printf(L.LOG_DEBUG, "Flushing ready feeds")
			if err = s.flushFeeds(readyFeeds); err != nil {
				L.Printf(L.LOG_ERROR, "Error while flushing ready feeds (disconnecting): %v", err)
				M.StatIncr("flush.error")
				s.disconnect()
			} else {
				s.mtime = time.Now()
			}
		}
	}
}

func (s *SocketSubscriber) Id() (P.SubId) {
	return s.id
}

// subscribers keep track of their subscriptions, so they can easily cleanup
func (s *SocketSubscriber) addSubscription(sub *S.Subscription) {
	s.lock.Lock()
	if sub != nil {
		s.subscriptions[sub.Id()] = sub
	}
	s.lock.Unlock()
}

// subscribers keep track of their subscriptions, so they can easily cleanup
func (s *SocketSubscriber) removeSubscription(sub P.SubId) *S.Subscription {
	s.lock.Lock()
	defer s.lock.Unlock()

	if subscription, ok := s.subscriptions[sub]; ok {
		delete(s.subscriptions, sub)
		return subscription
	}
	return nil
}

// implementation of Notify
func (s *SocketSubscriber) Notify(note S.Notification) bool {
	s.notifications <- note
	return true
}

func (s *SocketSubscriber) purge() {
	s.reap <- true
}

func (s *SocketSubscriber) disconnect() { 
	L.Printf(L.LOG_DEBUG, "DISCONNECT: %v", s.id)
  s.disconnects <- true
}

func (s *SocketSubscriber) addConnection(conn net.TCPConn) {
	s.connectionAdds <- conn
}

func (s *SocketSubscriber) notifiedFlush(note S.Notification) (err error) {
	var ok bool
	var item *S.FeedItem

	L.Printf(L.LOG_DEBUG, "notified flush!")

	if item, ok = s.feedMap[note.Name]; !ok || item == nil {
		L.Printf(L.LOG_INFO, "Notified flush on %v, no cursor for %v", note.Name, s.id)
		if feed := s.srv.feeds.Get(note.Name); feed != nil {
			// Back fill a few seconds
			if item = feed.FirstAt(note.Time - backfillSeconds); item != nil {
				L.Printf(L.LOG_INFO, "Establishing message (%v, %v) as first for %v, feed %v", item.Content(), item.Time(), s.id, note.Name)
				s.feedMap[note.Name] = item
			}
		}
	}

	return
}

func (s *SocketSubscriber) flushFeed(name S.FeedName) (err error) {
	var item *S.FeedItem
	var ok bool

	if item, ok = s.feedMap[name]; ok && item != nil {
		if feed := s.srv.feeds.Get(name); feed != nil {
			for feed.HasNext(item) {
				if err = s.send(item.Content()); err != nil {
					break
				}
				item = feed.Next(item)
			}
			s.feedMap[name] = item
		} else {
			L.Printf(L.LOG_WARN, "Failed to find feed %v to flush", name)
		}
	} else {
		L.Printf(L.LOG_WARN, "Flush called before notification setup feed position")
	}
	return
}

// we're periodically calling this which flushes all feeds
func (s *SocketSubscriber) flushFeeds(ready map[S.FeedName]bool) (err error) {
	for name, ok := range ready {
		if ok {
			if err = s.flushFeed(name); err != nil {
				return
			}
		}
		delete(ready, name)
	}

	return
}

func (s *SocketSubscriber) send(v interface{}) error {
	buf, err := pack(v)
	if err == nil {
		err = s.writeAll(buf)
	}
	return err
}


// TODO: rethink this, but it should work.
func (s *SocketSubscriber) writeAll(buf []byte) error {
	// we have potentially multiple clients to send to for this Subscriber.
	// writeAll succeeds if one of those sockets succeeds, and fails if
	// no socket succeeds

	if !s.isConnected {
		return fmt.Errorf("attempting to write %d bytes to an unconnected client", len(buf))
	}

	var lastErr error
	var errs, succs int

	if len(s.conn) > 0 {
		lconn := len(s.conn)
		for i := 0; i < lconn; i++ {
			conn := s.conn[i]
			skip := false
			buflen := len(buf)
			left := buflen

			for left > 0 {
				conn.SetWriteDeadline(time.Now().Add(250 * time.Millisecond))
				if n, err := conn.Write(buf[buflen-left : buflen]); err == nil {
					left -= n
				} else if err == io.ErrShortWrite {
					L.Printf(L.LOG_DEBUG, "Short write on %v: %s", s.id, err)
					left -= n
				} else {
					if e := HardClose(conn); e != nil {
						L.Printf(L.LOG_DEBUG, "Hard close error for subscriberId=%v, %v: %v", s.id, conn, e)
					}
					L.Printf(L.LOG_DEBUG, "Err in write: %v. Connection closed for %v", err, s.id)
					errs++
					lastErr = err
					skip = true
					break
				}
				conn.SetWriteDeadline(time.Unix(0, 0))
			}

			if !skip {
				s.conn[succs] = conn
				succs++
			}
		}

		if succs > 0 {
			s.conn = s.conn[0:succs]
		} else if errs > 0 {
			return lastErr
		}
	}
	return nil
}

func (s *SocketSubscriber) sendPing() error {
	L.Printf(LOG_PING, "PING to %v", s.id)
	return s.writeAll(pingPacket)
}


func SubscriberMapStatus(s SubscriberMap) map[string]map[string]string {
	status := make(map[string]map[string]string)

	/*
	 subscriberId: { subscriptionId: key }
	*/

	for subId, subscriber := range s {
		status[subId.String()] = make(map[string]string)
		substr := subId.String()
		for ripId, subscript := range subscriber.subscriptions {
			status[substr][ripId.String()] = subscript.Key()
		}
	}

	return status
}

func pack(v interface{}) (b []byte, err error) {
	buf := new(bytes.Buffer)
	if _, err = mpack.Pack(buf, v, true); err == nil {
		b = buf.Bytes()
	}
	return
}

func makePingPacket() []byte {
	var buf []byte
	var err error
	p := map[string]string{"ping": "pong"}

	if buf, err = pack(p); err != nil {
		log.Fatalf("FATAL: Unable to make ping packet--aborting! (%s)", err)
	}

	return buf
}

func splitAndStrip(src, sep string) []string {
	bits := strings.Split(src, sep)
	lbits := len(bits)
	for i := 0; i < lbits; i++ {
		bits[i] = strings.TrimSpace(bits[i])
	}
	return bits
}

func HardClose(conn net.TCPConn) (e error) {
	buf := make([]byte, 1)
	conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	_, e = conn.Read(buf)
	conn.SetReadDeadline(time.Unix(0, 0))
	conn.Close()
	return
}
