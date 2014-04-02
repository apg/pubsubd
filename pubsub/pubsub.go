package pubsub

import (
  "fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
)

var validKey *regexp.Regexp =
	regexp.MustCompile("^[a-zA-Z0-9_-]+[a-zA-Z0-9.*_-]*$")

type SubId int64

func IsValidSubscriptionKey(key string) bool {
	return validKey.MatchString(key)
}

func CompileSubscriptionKey(key string) (keyRx *regexp.Regexp, err error) {
	keyTmp := strings.Replace(regexp.QuoteMeta(key), "\\*", ".*", -1)
	keyRx, err = regexp.Compile(fmt.Sprintf("^%s$", keyTmp))
	return
}

func (s SubId) String() string {
	return strconv.FormatUint(uint64(s), 16)
}

func ToSubId(id string) (n SubId, err error) {
	u, err := strconv.ParseUint(id, 16, 64)
	if err != nil {
		return SubId(0), err
	}
	return SubId(u), nil
}


func RandomSubId() (n SubId) {
	return SubId(uint64(rand.Int63()) + uint64((1<<32)-1))
}
