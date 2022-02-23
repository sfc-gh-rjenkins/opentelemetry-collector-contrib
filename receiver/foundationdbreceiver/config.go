package foundationdbreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/foundationdbreceiver"

import (
	"fmt"
	"net"
	"strconv"

    "go.opentelemetry.io/collector/config"
)

const (
	typeStr                 = "foundationdb"
	defaultAddress          = "localhost:8889"
	defaultMaxPacketSize    = 65_527 // max size for udp packet body (assuming ipv6)
	defaultSocketBufferSize = 0
)

type Config struct {
	config.ReceiverSettings `mapstructure:",squash"`
	Address                 string `mapstructure:"address"`
	MaxPacketSize           int    `mapstructure:"max_packet_size"`
	SocketBufferSize        int    `mapstructure:"socket_buffer_size"`
}

func (c *Config) validate() error {
  if c.MaxPacketSize > 65535 || c.MaxPacketSize <= 0 {
    return fmt.Errorf("max_packet_size must be between 1 and 65535")
  }

  if c.SocketBufferSize < 0 {
    return fmt.Errorf("socket_buffer_size must be > 0")
  }

  err := validateAddress(c.Address)
  if err != nil {
    return err
  }
  return nil
}

// extract the port number from string in "address:port" format. If the
// port number cannot be extracted returns an error.
func validateAddress(endpoint string) (error) {
	_, portStr, err := net.SplitHostPort(endpoint)
	if err != nil {
		return fmt.Errorf("endpoint is not formatted correctly: %s", err.Error())
	}
	port, err := strconv.ParseInt(portStr, 10, 0)
	if err != nil {
		return fmt.Errorf("endpoint port is not a number: %s", err.Error())
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("port number must be between 1 and 65535")
	}
	return nil
}

func createDefaultConfig() config.Receiver {
	return &Config{
		ReceiverSettings: config.NewReceiverSettings(config.NewComponentID(typeStr)),
		Address:          defaultAddress,
		MaxPacketSize:    defaultMaxPacketSize,
		SocketBufferSize: defaultSocketBufferSize,
	}
}
