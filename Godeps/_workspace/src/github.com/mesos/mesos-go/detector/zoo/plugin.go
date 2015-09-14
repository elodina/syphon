package zoo

import (
	"github.com/elodina/syphon/Godeps/_workspace/src/github.com/mesos/mesos-go/detector"
)

func init() {
	detector.Register("zk://", detector.PluginFactory(func(spec string) (detector.Master, error) {
		return NewMasterDetector(spec)
	}))
}
