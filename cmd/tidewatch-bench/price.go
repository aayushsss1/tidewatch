package main

// price is USD per million tokens.
type price struct{ in, out float64 }

// prices is the small table used to estimate spend. Extend it when benchmarking a new model.
var prices = map[string]price{
	"claude-haiku-5-5":  {0.10, 0.50},
	"claude-sonnet-5-5": {2, 10},
	"claude-opus-5-5":   {4, 20},
}

// usd estimates the cost of the token counts under model; ok is false for an unknown model.
func usd(model string, inTokens, outTokens int) (cost float64, ok bool) {
	p, ok := prices[model]
	return (float64(inTokens)*p.in + float64(outTokens)*p.out) / 1e6, ok
}
