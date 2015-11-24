package tracer
import (
	"github.com/elodina/go-zipkin"
	"github.com/elodina/syphon/log"
)

var (
	Tracer *zipkin.Tracer
)

func NewDefaultTracer(serviceName string, sampleRate float64, brokerList[]string, kafkaTopic string) *zipkin.Tracer {
	traceConfig := zipkin.NewTraceConfig(serviceName, sampleRate, brokerList)
	traceConfig.Topic = kafkaTopic
	tracer, err := zipkin.NewTracer(traceConfig)
	if (err != nil) {
		log.Logger.Warn("Couldn't start Zipkin tracer: ", err)
		return nil
	}
	return tracer
}