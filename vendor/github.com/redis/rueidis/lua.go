package rueidis

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"runtime"
	"sync/atomic"

	"github.com/redis/rueidis/internal/util"
)

// NewLuaScript creates a Lua instance whose Lua.Exec uses EVALSHA and EVAL.
func NewLuaScript(script string) *Lua {
	return newLuaScript(script, false, false)
}

// NewLuaScriptReadOnly creates a Lua instance whose Lua.Exec uses EVALSHA_RO and EVAL_RO.
func NewLuaScriptReadOnly(script string) *Lua {
	return newLuaScript(script, true, false)
}

// NewLuaScriptNoSha creates a Lua instance whose Lua.Exec uses EVAL.
// Sha1 is not calculated, SCRIPT LOAD is not used, no EVALSHA is used.
// The main motivation is to be FIPS compliant, also avoid the tiny chance of SHA-1 collisions.
// This comes with a performance cost as the script is sent to a server every time.
func NewLuaScriptNoSha(script string) *Lua {
	return newLuaScript(script, false, true)
}

// NewLuaScriptReadOnlyNoSha creates a Lua instance whose Lua.Exec uses EVAL_RO.
// Sha1 is not calculated, SCRIPT LOAD is not used, no EVALSHA_RO is used.
// The main motivation is to be FIPS compliant, also avoid the tiny chance of SHA-1 collisions.
// This comes with a performance cost as the script is sent to a server every time.
func NewLuaScriptReadOnlyNoSha(script string) *Lua {
	return newLuaScript(script, true, true)
}

func newLuaScript(script string, readonly bool, noSha1 bool) *Lua {
	var sha1Hex string
	if !noSha1 {
		// It's important to avoid calling sha1 methods since Go will panic in FIPS mode.
		sum := sha1.Sum([]byte(script))
		sha1Hex = hex.EncodeToString(sum[:])
	}
	return &Lua{
		script:   script,
		sha1:     sha1Hex,
		maxp:     runtime.GOMAXPROCS(0),
		readonly: readonly,
		nosha1:   noSha1,
	}
}

// Lua represents a redis lua script. It should be created from the NewLuaScript() or NewLuaScriptReadOnly().
type Lua struct {
	script   string
	sha1     string
	maxp     int
	readonly bool
	nosha1   bool
}

// Exec the script to the given Client.
// It will first try with the EVALSHA/EVALSHA_RO and then EVAL/EVAL_RO if the first try failed.
// If Lua is initialized with disabled SHA1, it will use EVAL/EVAL_RO without the EVALSHA/EVALSHA_RO attempt.
// Cross-slot keys are prohibited if the Client is a cluster client.
func (s *Lua) Exec(ctx context.Context, c Client, keys, args []string) (resp RedisResult) {
	var isNoScript bool
	if !s.nosha1 {
		if s.readonly {
			resp = c.Do(ctx, c.B().EvalshaRo().Sha1(s.sha1).Numkeys(int64(len(keys))).Key(keys...).Arg(args...).Build())
		} else {
			resp = c.Do(ctx, c.B().Evalsha().Sha1(s.sha1).Numkeys(int64(len(keys))).Key(keys...).Arg(args...).Build())
		}
		err, isErr := IsRedisErr(resp.Error())
		isNoScript = isErr && err.IsNoScript()
	}
	if s.nosha1 || isNoScript {
		if s.readonly {
			resp = c.Do(ctx, c.B().EvalRo().Script(s.script).Numkeys(int64(len(keys))).Key(keys...).Arg(args...).Build())
		} else {
			resp = c.Do(ctx, c.B().Eval().Script(s.script).Numkeys(int64(len(keys))).Key(keys...).Arg(args...).Build())
		}
	}
	return resp
}

// LuaExec is a single execution unit of Lua.ExecMulti.
type LuaExec struct {
	Keys []string
	Args []string
}

// ExecMulti exec the script multiple times by the provided LuaExec to the given Client.
// It will first try SCRIPT LOAD the script to all redis nodes and then exec it with the EVALSHA/EVALSHA_RO.
// If Lua is initialized with disabled SHA1, it will use EVAL/EVAL_RO and no script loading.
// Cross-slot keys within the single LuaExec are prohibited if the Client is a cluster client.
func (s *Lua) ExecMulti(ctx context.Context, c Client, multi ...LuaExec) (resp []RedisResult) {
	if !s.nosha1 {
		var e atomic.Value
		util.ParallelVals(s.maxp, c.Nodes(), func(n Client) {
			if err := n.Do(ctx, n.B().ScriptLoad().Script(s.script).Build()).Error(); err != nil {
				e.CompareAndSwap(nil, &errs{error: err})
			}
		})
		if err := e.Load(); err != nil {
			resp = make([]RedisResult, len(multi))
			for i := 0; i < len(resp); i++ {
				resp[i] = newErrResult(err.(*errs).error)
			}
			return
		}
	}
	cmds := make(Commands, 0, len(multi))
	for _, m := range multi {
		if !s.nosha1 {
			if s.readonly {
				cmds = append(cmds, c.B().EvalshaRo().Sha1(s.sha1).Numkeys(int64(len(m.Keys))).Key(m.Keys...).Arg(m.Args...).Build())
			} else {
				cmds = append(cmds, c.B().Evalsha().Sha1(s.sha1).Numkeys(int64(len(m.Keys))).Key(m.Keys...).Arg(m.Args...).Build())
			}
		} else {
			if s.readonly {
				cmds = append(cmds, c.B().EvalRo().Script(s.script).Numkeys(int64(len(m.Keys))).Key(m.Keys...).Arg(m.Args...).Build())
			} else {
				cmds = append(cmds, c.B().Eval().Script(s.script).Numkeys(int64(len(m.Keys))).Key(m.Keys...).Arg(m.Args...).Build())
			}
		}
	}
	return c.DoMulti(ctx, cmds...)
}
