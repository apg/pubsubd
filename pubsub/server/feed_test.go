package server

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
	)

const debug bool = false

func TestFeed(t *testing.T) {
	numsToPush := []int{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }

	feed := NewFeed(FeedName("foo"))

	for _, num := range numsToPush {
		feed.Publish(num)
	}

	cursor := feed.Head()

	i := 0
	for feed.HasNext(cursor) {
		if numsToPush[i] != cursor.Content() {
			t.Errorf("expected: %d, got: %d -> should be equal!", numsToPush[i], cursor.Content())
		}
		cursor = feed.Next(cursor)
		i++
	}
}


func TestMultipleConsumersWholeFeed(t *testing.T) {
	i := 0
	done := make(chan bool, 5)
	numsToPush := []int{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }
	feedItems := make([]*FeedItem, 5)

	feed := NewFeed(FeedName("foo"))

	for ; i < 5; i++ {
		feedItems[i] = feed.Head()
		if debug {
			fmt.Printf("Ref-count: %d\n", feedItems[i].Refs())
		}
	}

	if debug {
		for j, fi := range feedItems {
			fmt.Printf("feedItem[%d] = %s\n", j, fi.String())
		}
	}

	// create 5 consumers, all of which should receive the whole feed
	for i = 0; i < 5; i++ {
		go func(which int, cursor *FeedItem, done chan bool) {
			i := 0
			for i < len(numsToPush) {
				if feed.HasNext(cursor) {
					if numsToPush[i] != cursor.Content() {
						t.Errorf("expected: %d, got: %d -> should be equal!", numsToPush[i], cursor.Content())
					}
					cursor = feed.Next(cursor)
					i++
					if debug {
						fmt.Printf("Feed view for %d: %s\n", which, cursor)
					}
				}
				time.Sleep(time.Duration(rand.Intn(1000) + 1) * time.Millisecond)
			}
			done <- true
		}(i, feedItems[i], done)
	}

	for _, num := range numsToPush {
		feed.Publish(num)
	}
	if debug {
		fmt.Printf("Done pushing results!\n")
	}

	if debug {
		fmt.Printf("Feed is now! %s\n", feed.Peek().String())
	}

	for i > 0 {
		<-done
		if debug {
			fmt.Printf("Goroutine done.\n")
		}
		i--
	}
}

// Need a way to test that cleanup occurs...