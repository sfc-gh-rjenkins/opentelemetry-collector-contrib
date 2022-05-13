package foundationdbreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/foundationdbreceiver"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
)

const (
	typeStr                 = "foundationdb"
	defaultAddress          = "localhost:8889"
	defaultMaxPacketSize    = 65_527 // max size for udp packet body (assuming ipv6)
	defaultSocketBufferSize = 0
	defaultFormat           = "opentelemetry"
)

// NewFactory creates a factory for the foundationDBReceiver.
func NewFactory() component.ReceiverFactory {
	return receiverhelper.NewFactory(
		typeStr,
		createDefaultConfig,
		receiverhelper.WithTraces(createTracesReceiver),
	)
}

func createTracesReceiver(ctx context.Context,
	settings component.ReceiverCreateSettings,
	cfg config.Receiver,
	consumer consumer.Traces) (component.TracesReceiver, error) {
	c := cfg.(*Config)
	if consumer == nil {
		return nil, fmt.Errorf("nil consumer")
	}

	return NewFoundationDBReceiver(settings, c, consumer)
}

func NewFoundationDBReceiver(settings component.ReceiverCreateSettings, config *Config,
	consumer consumer.Traces) (component.TracesReceiver, error) {
	ts, err := NewUDPServer(config.Address, config.SocketBufferSize)
	if err != nil {
		return nil, err
	}
	handler := createHandler(config, consumer)
	if handler == nil {
      return nil, fmt.Errorf("unable to create handler, tracing format %s unsupported", config.Format)
	}
	return &foundationDBReceiver{listener: ts, consumer: consumer, config: config, logger: settings.Logger, handler: handler}, nil
}

func createHandler(c *Config, consumer consumer.Traces) fdbTraceHandler {
	switch {
	case c.Format == OPENTELEMETRY:
		return &openTelemetryHandler{consumer: consumer}
	case c.Format == OPENTRACING:
		return &openTracingHandler{consumer: consumer}
	default:
		return nil
	}
}

func createDefaultConfig() config.Receiver {
	return &Config{
		ReceiverSettings: config.NewReceiverSettings(config.NewComponentID(typeStr)),
		Address:          defaultAddress,
		MaxPacketSize:    defaultMaxPacketSize,
		SocketBufferSize: defaultSocketBufferSize,
		Format:           defaultFormat,
	}
}
