package pubsub

import (
	"testing"

	"github.com/jeremybower/go-common/optional"
	"github.com/stretchr/testify/assert"
)

func TestConfiguration(t *testing.T) {
	t.Parallel()

	// Create a test harness.
	h := NewTestHarness(t, NewPostgresDataStore())
	defer h.Close()

	// Read the default configuration.
	configuration := h.ReadConfiguration()
	assert.Equal(t, int32(120), configuration.MissedMessageSeconds)

	// Create the expected configuration after patching.
	expected := &Configuration{
		MissedMessageSeconds: configuration.MissedMessageSeconds * 2,
	}

	// Patch the configuration.
	configuration = h.PatchConfiguration(ConfigurationPatch{
		MissedMessageSeconds: optional.NewValue[int32](expected.MissedMessageSeconds),
	})
	assert.Equal(t, expected, configuration)
}
