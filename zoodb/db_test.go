package zoodb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type DbTestSuite struct {
	suite.Suite
}

func TestExampleTestSuite(t *testing.T) {
	suite.Run(t, &DbTestSuite{})
}

func (d *DbTestSuite) TestGetValue() {
	val := getValue("cZxid = 0x00000000000000")
	d.Require().Equal("0x00000000000000", val)
}

func (d *DbTestSuite) TestParseData() {
	t, err := time.Parse(time.UnixDate, "Thu Jan 01 08:00:00 CST 1970")
	d.Require().NoError(err)
	d.Require().Equal(time.UnixMilli(0), t)
}

func (d *DbTestSuite) TestZnodeParse() {
	znodeStr := `/
  cZxid = 0x00000000000000
  ctime = Thu Jan 01 08:00:00 CST 1970
  mZxid = 0x00000000000000
  mtime = Thu Jan 01 08:00:00 CST 1970
  pZxid = 0x00011a00039bab
  cversion = 8
  dataVersion = 0
  aclVersion = 125
  ephemeralOwner = 0x00000000000000
  dataLength = 0
  data =`

	znode, err := ZnodeFromString(SplitLines(znodeStr))
	d.Require().NoError(err)
	d.Require().Equal("/", znode.Path)
	d.Require().Equal(uint64(0), znode.CZxid)
	d.Require().Equal(time.UnixMilli(0), znode.Ctime)
	d.Require().Equal(uint64(0), znode.MZxid)
	d.Require().Equal(time.UnixMilli(0), znode.Mtime)
	d.Require().Equal(uint64(0x00011a00039bab), znode.PZxid)
	d.Require().Equal(int32(8), znode.CVersion)
	d.Require().Equal(int32(0), znode.DataVersion)
	d.Require().Equal(int32(125), znode.AclVersion)
	d.Require().Equal(uint64(0), znode.EphemeralOwner)
	d.Require().Empty(znode.Data)
}
