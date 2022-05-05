package foundationdbreceiver

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/service/servicetest"
)

func TestLoadConfig(t *testing.T) {
	factories, err := componenttest.NopFactories()
	assert.Nil(t, err)

	factory := NewFactory()
	factories.Receivers[typeStr] = factory
	cfg, err := servicetest.LoadConfigAndValidate(filepath.Join("testdata", "config.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, len(cfg.Receivers), 2)

	r0 := cfg.Receivers[config.NewComponentID(typeStr)]
	assert.Equal(t, factory.CreateDefaultConfig(), r0)

	r1 := cfg.Receivers[config.NewComponentIDWithName(typeStr, "receiver_settings")]
	assert.Equal(t, &Config{
		ReceiverSettings: config.NewReceiverSettings(config.NewComponentIDWithName(typeStr, "receiver_settings")),
		Address:          "localhost:8889",
		MaxPacketSize:    65535,
		SocketBufferSize: 2097152,
		Format:           "opentelemetry",
	}, r1)
}

func TestValidateConfig(t *testing.T) {
	type test struct {
		name   string
		config *Config
		error  error
	}

	tests := []test{
		{
			name: "maxPacketSizeTooSmall",
			config: &Config{
				MaxPacketSize: -100,
				Format:        "opentelemetry",
			},
			error: maxPacketSizeErr,
		},
		{
			name: "maxPacketSizeTooLarge",
			config: &Config{
				MaxPacketSize: 65536,
				Format:        "opentelemetry",
			},
			error: maxPacketSizeErr,
		},
		{
			name: "socketBufferSizeToSmall",
			config: &Config{
				MaxPacketSize:    65535,
				Format:           "opentelemetry",
				SocketBufferSize: -1,
			},
			error: socketBufferSizeErr,
		},
		{
			name: "improperAddress",
			config: &Config{
				MaxPacketSize: 65535,
				Address:       "foo",
				Format:        "opentelemetry",
			},
			error: fmt.Errorf("endpoint is not formatted correctly: address foo: missing port in address"),
		},
		{
			name: "improperNANPortAddress",
			config: &Config{
				MaxPacketSize: 65535,
				Address:       "foo:xyx",
				Format:        "opentelemetry",
			},
			error: fmt.Errorf("endpoint port is not a number: strconv.ParseInt: parsing \"xyx\": invalid syntax"),
		},
		{
			name: "illegalPortAddress",
			config: &Config{
				MaxPacketSize: 65535,
				Address:       "foo:70000",
				Format:        "opentelemetry",
			},
			error: portNumberRangeErr,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.EqualError(t, test.config.validate(), test.error.Error())
		})
	}
}
