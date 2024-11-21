package partition

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeDuration_Value(t *testing.T) {
	tests := []struct {
		name     string
		duration TimeDuration
		want     driver.Value
		wantErr  bool
	}{
		{
			name:     "24 hours",
			duration: TimeDuration(24 * time.Hour),
			want:     "24h0m0s",
			wantErr:  false,
		},
		{
			name:     "zero duration",
			duration: TimeDuration(0),
			want:     nil,
			wantErr:  false,
		},
		{
			name:     "complex duration",
			duration: TimeDuration(24*time.Hour + 30*time.Minute + 15*time.Second),
			want:     "24h30m15s",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.duration.Value()
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
