package main

// price is USD per million tokens. longIn and longOut apply to calls whose prompt exceeds
// 100K tokens; zero means the model has a single tier.
type price struct{ in, out, longIn, longOut float64 }

// prices is the small table used to estimate spend. Extend it when benchmarking a new model.
// Haiku is $0.10/$0.50 per MTok for prompts up to 100K tokens and $0.50/$2.50 above that.
var prices = map[string]price{
	"claude-haiku-5-5":  {0.10, 0.50, 0.50, 2.50},
	"claude-sonnet-5-5": {2, 10, 0, 0},
	"claude-opus-5-5":   {4, 20, 0, 0},
}

// usd estimates the cost of the token counts under model; ok is false for an unknown model.
// longIn and longOut are the parts of inTokens and outTokens that belong to calls with a
// prompt over 100K tokens, charged at the model's long-prompt rate when it has one.
func usd(model string, inTokens, outTokens, longIn, longOut int) (cost float64, ok bool) {
	p, ok := prices[model]
	li, lo := p.longIn, p.longOut
	if li == 0 {
		li, lo = p.in, p.out
	}
	cost = (float64(inTokens-longIn)*p.in + float64(outTokens-longOut)*p.out +
		float64(longIn)*li + float64(longOut)*lo) / 1e6
	return cost, ok
}
