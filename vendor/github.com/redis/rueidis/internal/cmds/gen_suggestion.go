// Code generated DO NOT EDIT

package cmds

import "strconv"

type FtSugadd Completed

func (b Builder) FtSugadd() (c FtSugadd) {
	c = FtSugadd{cs: get(), ks: b.ks}
	c.cs.s = append(c.cs.s, "FT.SUGADD")
	return c
}

func (c FtSugadd) Key(key string) FtSugaddKey {
	c.cs.s = append(c.cs.s, key)
	return (FtSugaddKey)(c)
}

type FtSugaddIncrementScoreIncr Completed

func (c FtSugaddIncrementScoreIncr) Payload(payload string) FtSugaddPayload {
	c.cs.s = append(c.cs.s, "PAYLOAD", payload)
	return (FtSugaddPayload)(c)
}

func (c FtSugaddIncrementScoreIncr) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

type FtSugaddKey Completed

func (c FtSugaddKey) String(string string) FtSugaddString {
	c.cs.s = append(c.cs.s, string)
	return (FtSugaddString)(c)
}

type FtSugaddPayload Completed

func (c FtSugaddPayload) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

type FtSugaddScore Completed

func (c FtSugaddScore) Incr() FtSugaddIncrementScoreIncr {
	c.cs.s = append(c.cs.s, "INCR")
	return (FtSugaddIncrementScoreIncr)(c)
}

func (c FtSugaddScore) Payload(payload string) FtSugaddPayload {
	c.cs.s = append(c.cs.s, "PAYLOAD", payload)
	return (FtSugaddPayload)(c)
}

func (c FtSugaddScore) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

type FtSugaddString Completed

func (c FtSugaddString) Score(score float64) FtSugaddScore {
	c.cs.s = append(c.cs.s, strconv.FormatFloat(score, 'f', -1, 64))
	return (FtSugaddScore)(c)
}

type FtSugdel Completed

func (b Builder) FtSugdel() (c FtSugdel) {
	c = FtSugdel{cs: get(), ks: b.ks}
	c.cs.s = append(c.cs.s, "FT.SUGDEL")
	return c
}

func (c FtSugdel) Key(key string) FtSugdelKey {
	c.cs.s = append(c.cs.s, key)
	return (FtSugdelKey)(c)
}

type FtSugdelKey Completed

func (c FtSugdelKey) String(string string) FtSugdelString {
	c.cs.s = append(c.cs.s, string)
	return (FtSugdelString)(c)
}

type FtSugdelString Completed

func (c FtSugdelString) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

type FtSugget Completed

func (b Builder) FtSugget() (c FtSugget) {
	c = FtSugget{cs: get(), ks: b.ks}
	c.cs.s = append(c.cs.s, "FT.SUGGET")
	return c
}

func (c FtSugget) Key(key string) FtSuggetKey {
	c.cs.s = append(c.cs.s, key)
	return (FtSuggetKey)(c)
}

type FtSuggetFuzzy Completed

func (c FtSuggetFuzzy) Withscores() FtSuggetWithscores {
	c.cs.s = append(c.cs.s, "WITHSCORES")
	return (FtSuggetWithscores)(c)
}

func (c FtSuggetFuzzy) Withpayloads() FtSuggetWithpayloads {
	c.cs.s = append(c.cs.s, "WITHPAYLOADS")
	return (FtSuggetWithpayloads)(c)
}

func (c FtSuggetFuzzy) Max(max int64) FtSuggetMax {
	c.cs.s = append(c.cs.s, "MAX", strconv.FormatInt(max, 10))
	return (FtSuggetMax)(c)
}

func (c FtSuggetFuzzy) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

type FtSuggetKey Completed

func (c FtSuggetKey) Prefix(prefix string) FtSuggetPrefix {
	c.cs.s = append(c.cs.s, prefix)
	return (FtSuggetPrefix)(c)
}

type FtSuggetMax Completed

func (c FtSuggetMax) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

type FtSuggetPrefix Completed

func (c FtSuggetPrefix) Fuzzy() FtSuggetFuzzy {
	c.cs.s = append(c.cs.s, "FUZZY")
	return (FtSuggetFuzzy)(c)
}

func (c FtSuggetPrefix) Withscores() FtSuggetWithscores {
	c.cs.s = append(c.cs.s, "WITHSCORES")
	return (FtSuggetWithscores)(c)
}

func (c FtSuggetPrefix) Withpayloads() FtSuggetWithpayloads {
	c.cs.s = append(c.cs.s, "WITHPAYLOADS")
	return (FtSuggetWithpayloads)(c)
}

func (c FtSuggetPrefix) Max(max int64) FtSuggetMax {
	c.cs.s = append(c.cs.s, "MAX", strconv.FormatInt(max, 10))
	return (FtSuggetMax)(c)
}

func (c FtSuggetPrefix) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

type FtSuggetWithpayloads Completed

func (c FtSuggetWithpayloads) Max(max int64) FtSuggetMax {
	c.cs.s = append(c.cs.s, "MAX", strconv.FormatInt(max, 10))
	return (FtSuggetMax)(c)
}

func (c FtSuggetWithpayloads) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

type FtSuggetWithscores Completed

func (c FtSuggetWithscores) Withpayloads() FtSuggetWithpayloads {
	c.cs.s = append(c.cs.s, "WITHPAYLOADS")
	return (FtSuggetWithpayloads)(c)
}

func (c FtSuggetWithscores) Max(max int64) FtSuggetMax {
	c.cs.s = append(c.cs.s, "MAX", strconv.FormatInt(max, 10))
	return (FtSuggetMax)(c)
}

func (c FtSuggetWithscores) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

type FtSuglen Completed

func (b Builder) FtSuglen() (c FtSuglen) {
	c = FtSuglen{cs: get(), ks: b.ks}
	c.cs.s = append(c.cs.s, "FT.SUGLEN")
	return c
}

func (c FtSuglen) Key(key string) FtSuglenKey {
	c.cs.s = append(c.cs.s, key)
	return (FtSuglenKey)(c)
}

type FtSuglenKey Completed

func (c FtSuglenKey) Build() Completed {
	c.cs.Build()
	return Completed(c)
}
