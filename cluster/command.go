// Package cluster wires the rate limiter's state through Raft, so every node
// holds the same counters and a failover loses nothing.
package cluster

import (
	"encoding/json"
	"fmt"
	"time"
)

// CommandType distinguishes the kinds of change that can be replicated
// (PRD.md Section 14).
type CommandType string

const (
	// CommandConsume spends tokens from one caller's bucket.
	CommandConsume CommandType = "consume"
	// CommandConfig changes the limit in force for everyone.
	CommandConfig CommandType = "config"
)

// Command is one replicated change to the limiter.
//
// TimestampNS is the reason this type exists rather than the nodes simply
// calling AllowN themselves. The token bucket refills with elapsed time, so the
// instant a request is judged at is an *input* to the result. The leader stamps
// that instant once, here, and every node applies the entry with it. If each
// node read its own clock while applying, the replicas would compute different
// balances from the same log and drift apart - exactly the divergence the
// replicated state machine exists to prevent.
type Command struct {
	Type     CommandType `json:"type"`
	CallerID string      `json:"caller_id,omitempty"`
	Amount   float64     `json:"amount,omitempty"`

	Limit    int   `json:"limit,omitempty"`
	WindowMS int64 `json:"window_ms,omitempty"`

	TimestampNS int64 `json:"timestamp_ns"`
}

// ConsumeCommand builds a command spending amount tokens for a caller at the
// given instant.
func ConsumeCommand(callerID string, amount float64, at time.Time) Command {
	return Command{
		Type:        CommandConsume,
		CallerID:    callerID,
		Amount:      amount,
		TimestampNS: at.UnixNano(),
	}
}

// ConfigCommand builds a command changing the limit.
func ConfigCommand(limit int, window time.Duration, at time.Time) Command {
	return Command{
		Type:        CommandConfig,
		Limit:       limit,
		WindowMS:    window.Milliseconds(),
		TimestampNS: at.UnixNano(),
	}
}

// At is the instant this command should be applied at.
//
// Always UTC. time.Unix hands back a time in the machine's local zone, and a
// time.Time carries its zone, so two nodes in different regions would produce
// snapshots that compare as different while describing the same moment.
// Normalising here keeps replica state comparable byte for byte.
func (c Command) At() time.Time {
	return time.Unix(0, c.TimestampNS).UTC()
}

// Window is the configured window this command carries.
func (c Command) Window() time.Duration {
	return time.Duration(c.WindowMS) * time.Millisecond
}

// Encode serialises the command for the Raft log.
func (c Command) Encode() ([]byte, error) {
	return json.Marshal(c)
}

// DecodeCommand parses a command out of a Raft log entry.
func DecodeCommand(raw []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(raw, &c); err != nil {
		return Command{}, fmt.Errorf("cluster: decode command: %w", err)
	}
	switch c.Type {
	case CommandConsume, CommandConfig:
		return c, nil
	default:
		return Command{}, fmt.Errorf("cluster: unknown command type %q", c.Type)
	}
}
