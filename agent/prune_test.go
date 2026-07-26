package agent

import (
	"math"
	"testing"
)

func TestParseDockerSize(t *testing.T) {
	cases := []struct {
		in   string
		want float64 // MB
	}{
		{"4.881GB", 4.881 * 1024},
		{"512.3MB", 512.3},
		{"1.2kB", 1.2 / 1024},
		{"0B", 0},
		{"731B", 731.0 / (1024 * 1024)},
		{"2TB", 2 * 1024 * 1024},
		{" 10MB ", 10},
		{"garbage", 0},
		{"", 0},
		{"GB", 0}, // suffix with no number
	}
	for _, c := range cases {
		got := parseDockerSize(c.in)
		if math.Abs(got-c.want) > 1e-9 {
			t.Errorf("parseDockerSize(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
