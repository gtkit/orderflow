package worker

import (
	"context"
	"sync"

	"github.com/gtkit/orderflow"
)

// Options 聚合三个 worker 的配置。零值字段会被替换为默认。
type Options struct {
	Close            CloseOptions
	CloseFallback    CloseFallbackOptions
	DeliveryFallback DeliveryFallbackOptions
}

// StartAll 按默认配置一次性启动三个 worker，阻塞直到 ctx 取消。
// 需要自定义节奏时使用 StartAllWithOptions。
func StartAll[O orderflow.OrderSnapshot](ctx context.Context, engine *orderflow.Engine[O]) {
	StartAllWithOptions(ctx, engine, Options{})
}

// StartAllWithOptions 使用自定义配置启动三个 worker，阻塞直到 ctx 取消。
func StartAllWithOptions[O orderflow.OrderSnapshot](ctx context.Context, engine *orderflow.Engine[O], opts Options) {
	cw := NewCloseWorker(engine, opts.Close)
	cf := NewCloseFallback(engine, opts.CloseFallback)
	df := NewDeliveryFallback(engine, opts.DeliveryFallback)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		cw.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		cf.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		df.Run(ctx)
	}()
	wg.Wait()
}
