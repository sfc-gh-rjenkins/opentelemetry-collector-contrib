package foundationdbreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/foundationdbreceiver"

import (
	"context"

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
	return NewFoundationDBReceiver(settings, c, consumer)
}

func NewFoundationDBReceiver(settings component.ReceiverCreateSettings, config *Config,
	consumer consumer.Traces) (component.TracesReceiver, error) {
	ts, err := NewUDPServer(config.Address, config.SocketBufferSize)
	if err != nil {
      return nil, err
	}
	return &foundationDBReceiver{server: ts, consumer: consumer, config: config}, nil
}

func createDefaultConfig() config.Receiver {
	return &Config{
		ReceiverSettings: config.NewReceiverSettings(config.NewComponentID(typeStr)),
		Address:          defaultAddress,
		MaxPacketSize:    defaultMaxPacketSize,
		SocketBufferSize: defaultSocketBufferSize,
	}
}

