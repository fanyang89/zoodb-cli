package zoodb

import (
	"bufio"
	"encoding/base64"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

type ZooDb struct {
	LastProcessedZxid uint64
	Znodes            []*Znode
	Sessions          []*Session
}

func NewZooDb(s *bufio.Scanner) (*ZooDb, error) {
	db := &ZooDb{
		Znodes:   make([]*Znode, 0),
		Sessions: make([]*Session, 0),
	}

	s.Scan()
	for strings.HasPrefix(s.Text(), "WARNING") {
		s.Scan()
	}
	db.LastProcessedZxid = hexStrToUint64(getValueByDelim(s.Text(), ":"))

	s.Scan()
	re := regexp.MustCompile(`(?m)ZNode Details \(count=(\d+)\):`)
	match := re.FindAllStringSubmatch(s.Text(), 1)
	count := decStrToInt32(match[0][1])

	for s.Scan() {
		if s.Text() == "----" {
			continue
		}

		if strings.HasPrefix(s.Text(), "Session Details") {
			s.Scan()
			for s.Scan() {
				session, err := SessionFromString(s.Text())
				if err != nil {
					return nil, err
				}
				db.Sessions = append(db.Sessions, session)
			}
		} else {
			buf := make([]string, 0)
			for i := 0; i < 12; i++ {
				buf = append(buf, s.Text())
				s.Scan()
			}
			n, err := ZnodeFromString(buf)
			if err != nil {
				return nil, err
			}
			db.Znodes = append(db.Znodes, n)
		}
	}

	if int(count)-1 != len(db.Znodes) {
		// there will be an extra root node in the ZkDatabase
		return nil, errors.Newf("corrupted, znode count(%v) does not match actual(%v)", count, len(db.Znodes))
	}

	return db, nil
}

type Session struct {
	SessionID      uint64
	Timeout        time.Duration
	EphemeralCount int32
}

type Znode struct {
	Path           string
	CZxid          uint64
	Ctime          time.Time
	MZxid          uint64
	Mtime          time.Time
	PZxid          uint64
	CVersion       int32
	DataVersion    int32
	AclVersion     int32
	EphemeralOwner uint64
	Data           []byte
}

func SplitLines(s string) []string {
	var lines []string
	sc := bufio.NewScanner(strings.NewReader(s))
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	return lines
}

func getValue(s string) string {
	return getValueByDelim(s, "=")
}

func getValueByDelim(s string, delim string) string {
	// input eg. cZxid = 0x00000000000000
	p := strings.Index(s, delim)
	if p < 0 {
		return ""
	}
	return strings.TrimSpace(s[p+1:])
}

func hexStrToUint64(s string) uint64 {
	s = strings.TrimPrefix(s, "0x")
	n, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		panic(errors.Wrap(err, "parse uint failed"))
	}
	return n
}

func decStrToInt32(s string) int32 {
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		panic(errors.Wrap(err, "parse uint failed"))
	}
	return int32(n)
}

func SessionFromString(s string) (*Session, error) {
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ' '
	})
	if len(fields) != 3 {
		return nil, errors.Newf("invalid session line, '%v'", s)
	}
	session := &Session{}
	session.SessionID = hexStrToUint64(fields[0])
	session.Timeout = time.Duration(decStrToInt32(fields[1])) * time.Millisecond
	session.EphemeralCount = decStrToInt32(fields[2])
	return session, nil
}

func ZnodeFromString(s []string) (*Znode, error) {
	if len(s) != 12 {
		return nil, errors.New("invalid input string slice")
	}

	n := &Znode{}
	n.Path = strings.TrimSpace(s[0])

	n.CZxid = hexStrToUint64(getValue(s[1]))
	ctime, err := time.Parse(time.UnixDate, getValue(s[2]))
	if err != nil {
		return nil, errors.Wrap(err, "parse ctime failed")
	}
	n.Ctime = ctime

	n.MZxid = hexStrToUint64(getValue(s[3]))
	mtime, err := time.Parse(time.UnixDate, getValue(s[4]))
	if err != nil {
		return nil, errors.Wrap(err, "parse ctime failed")
	}
	n.Mtime = mtime

	n.PZxid = hexStrToUint64(getValue(s[5]))
	n.CVersion = decStrToInt32(getValue(s[6]))
	n.DataVersion = decStrToInt32(getValue(s[7]))
	n.AclVersion = decStrToInt32(getValue(s[8]))

	n.EphemeralOwner = hexStrToUint64(getValue(s[9]))

	dataLen := decStrToInt32(getValue(s[10]))
	dataValue := getValue(s[11])
	if dataValue == "" || dataValue == "''" {
		n.Data = make([]byte, 0)
	} else {
		n.Data, err = base64.StdEncoding.DecodeString(dataValue)
		if err != nil {
			return nil, errors.Wrap(err, "decode base64 failed")
		}
		if len(n.Data) != int(dataLen) {
			return nil, errors.New("invalid data length")
		}
	}

	return n, nil
}
