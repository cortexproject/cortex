package cmds

import "strings"

const (
	optInTag = uint16(1 << 15)
	blockTag = uint16(1 << 14)
	readonly = uint16(1 << 13)
	noRetTag = uint16(1<<12) | readonly // make noRetTag can also be retried
	mtGetTag = uint16(1<<11) | readonly // make mtGetTag can also be retried
	// InitSlot indicates that the command be sent to any redis node in cluster
	InitSlot = uint16(1 << 14)
	// NoSlot indicates that the command has no key slot specified
	NoSlot = uint16(1 << 15)
)

var (
	// OptInCmd is predefined CLIENT CACHING YES
	OptInCmd = Completed{
		cs: &CommandSlice{s: []string{"CLIENT", "CACHING", "YES"}},
		cf: optInTag,
	}
	// MultiCmd is predefined MULTI
	MultiCmd = Completed{
		cs: &CommandSlice{s: []string{"MULTI"}},
	}
	// ExecCmd is predefined EXEC
	ExecCmd = Completed{
		cs: &CommandSlice{s: []string{"EXEC"}},
	}
	// RoleCmd is predefined ROLE
	RoleCmd = Completed{
		cs: &CommandSlice{s: []string{"ROLE"}},
	}
	// QuitCmd is predefined QUIT
	QuitCmd = Completed{
		cs: &CommandSlice{s: []string{"QUIT"}},
	}
	// UnsubscribeCmd is predefined UNSUBSCRIBE
	UnsubscribeCmd = Completed{
		cs: &CommandSlice{s: []string{"UNSUBSCRIBE"}},
		cf: noRetTag,
	}
	// PUnsubscribeCmd is predefined PUNSUBSCRIBE
	PUnsubscribeCmd = Completed{
		cs: &CommandSlice{s: []string{"PUNSUBSCRIBE"}},
		cf: noRetTag,
	}
	// SUnsubscribeCmd is predefined SUNSUBSCRIBE
	SUnsubscribeCmd = Completed{
		cs: &CommandSlice{s: []string{"SUNSUBSCRIBE"}},
		cf: noRetTag,
	}
	// PingCmd is predefined PING
	PingCmd = Completed{
		cs: &CommandSlice{s: []string{"PING"}},
	}
	// SlotCmd is predefined CLUSTER SLOTS
	SlotCmd = Completed{
		cs: &CommandSlice{s: []string{"CLUSTER", "SLOTS"}},
	}
	// AskingCmd is predefined CLUSTER ASKING
	AskingCmd = Completed{
		cs: &CommandSlice{s: []string{"ASKING"}},
	}
	// SentinelSubscribe is predefined SUBSCRIBE ASKING
	SentinelSubscribe = Completed{
		cs: &CommandSlice{s: []string{"SUBSCRIBE", "+sentinel", "+switch-master", "+reboot"}},
		cf: noRetTag,
	}
)

// Completed represents a completed Redis command, should be created by the Build() of command builder.
type Completed struct {
	cs *CommandSlice
	cf uint16
	ks uint16
}

// IsEmpty checks if it is an empty command.
func (c *Completed) IsEmpty() bool {
	return c.cs == nil || len(c.cs.s) == 0
}

// IsOptIn checks if it is client side caching opt-int command.
func (c *Completed) IsOptIn() bool {
	return c.cf&optInTag == optInTag
}

// IsBlock checks if it is blocking command which needs to be process by dedicated connection.
func (c *Completed) IsBlock() bool {
	return c.cf&blockTag == blockTag
}

// ToBlock marks the command with blockTag
func (c *Completed) ToBlock() {
	c.cf |= blockTag
}

// NoReply checks if it is one of the SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE or PUNSUBSCRIBE commands.
func (c *Completed) NoReply() bool {
	return c.cf&noRetTag == noRetTag
}

// IsReadOnly checks if it is readonly command and can be retried when network error.
func (c *Completed) IsReadOnly() bool {
	return c.cf&readonly == readonly
}

// IsWrite checks if it is not readonly command.
func (c *Completed) IsWrite() bool {
	return !c.IsReadOnly()
}

// Commands returns the commands as []string.
// Note that the returned []string should not be modified
// and should not be read after passing into the Client interface, because it will be recycled.
func (c *Completed) Commands() []string {
	return c.cs.s
}

// CommandSlice it the command container which will be recycled by the sync.Pool.
func (c *Completed) CommandSlice() *CommandSlice {
	return c.cs
}

// Slot returns the command key slot
func (c *Completed) Slot() uint16 {
	return c.ks
}

// Cacheable represents a completed Redis command which supports server-assisted client side caching,
// and it should be created by the Cache() of command builder.
type Cacheable Completed

// Slot returns the command key slot
func (c *Cacheable) Slot() uint16 {
	return c.ks
}

// Commands returns the commands as []string.
// Note that the returned []string should not be modified
// and should not be read after passing into the Client interface, because it will be recycled.
func (c *Cacheable) Commands() []string {
	return c.cs.s
}

// CommandSlice returns the command container which will be recycled by the sync.Pool.
func (c *Cacheable) CommandSlice() *CommandSlice {
	return c.cs
}

// IsMGet returns if the command is MGET
func (c *Cacheable) IsMGet() bool {
	return c.cf == mtGetTag
}

// MGetCacheCmd returns the cache command of the MGET singular command
func (c *Cacheable) MGetCacheCmd() string {
	if c.cs.s[0][0] == 'J' {
		return "JSON.GET" + c.cs.s[len(c.cs.s)-1]
	}
	return "GET"
}

// MGetCacheKey returns the cache key of the MGET singular command
func (c *Cacheable) MGetCacheKey(i int) string {
	return c.cs.s[i+1]
}

// CacheKey returns the cache key used by the server-assisted client side caching
func (c *Cacheable) CacheKey() (key, command string) {
	if len(c.cs.s) == 2 {
		return c.cs.s[1], c.cs.s[0]
	}

	length := 0
	for i, v := range c.cs.s {
		if i == 1 {
			continue
		}
		length += len(v)
	}
	sb := strings.Builder{}
	sb.Grow(length)
	for i, v := range c.cs.s {
		if i == 1 {
			key = v
		} else {
			sb.WriteString(v)
		}
	}
	return key, sb.String()
}

// NewCompleted creates an arbitrary Completed command.
func NewCompleted(ss []string) Completed {
	return Completed{cs: &CommandSlice{s: ss}}
}

// NewBlockingCompleted creates an arbitrary blocking Completed command.
func NewBlockingCompleted(ss []string) Completed {
	return Completed{cs: &CommandSlice{s: ss}, cf: blockTag}
}

// NewReadOnlyCompleted creates an arbitrary readonly Completed command.
func NewReadOnlyCompleted(ss []string) Completed {
	return Completed{cs: &CommandSlice{s: ss}, cf: readonly}
}

// NewMGetCompleted creates an arbitrary readonly Completed command.
func NewMGetCompleted(ss []string) Completed {
	return Completed{cs: &CommandSlice{s: ss}, cf: mtGetTag}
}

// MGets groups keys by their slot and returns multi MGET commands
func MGets(keys []string) map[uint16]Completed {
	return slotMGets("MGET", keys)
}

// JsonMGets groups keys by their slot and returns multi JSON.MGET commands
func JsonMGets(keys []string, path string) map[uint16]Completed {
	ret := slotMGets("JSON.MGET", keys)
	for _, jsonmget := range ret {
		jsonmget.cs.s = append(jsonmget.cs.s, path)
	}
	return ret
}

func slotMGets(cmd string, keys []string) map[uint16]Completed {
	ret := make(map[uint16]Completed, 16)
	for _, key := range keys {
		var cs *CommandSlice
		ks := slot(key)
		if cp, ok := ret[ks]; ok {
			cs = cp.cs
		} else {
			cs = get()
			cs.s = append(cs.s, cmd)
			ret[ks] = Completed{cs: cs, cf: mtGetTag, ks: ks}
		}
		cs.s = append(cs.s, key)
	}
	return ret
}

// NewMultiCompleted creates multiple arbitrary Completed commands.
func NewMultiCompleted(cs [][]string) []Completed {
	ret := make([]Completed, len(cs))
	for i, c := range cs {
		ret[i] = NewCompleted(c)
	}
	return ret
}

func check(prev, new uint16) uint16 {
	if prev == InitSlot || prev == new {
		return new
	}
	panic(multiKeySlotErr)
}

const multiKeySlotErr = "multi key command with different key slots are not allowed"
