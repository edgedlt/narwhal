package narwhal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// FetcherConfig configures the Fetcher.
type FetcherConfig struct {
	// MaxRetries is the maximum number of retry attempts per fetch.
	MaxRetries int

	// RetryDelay is the base delay between retries (with exponential backoff).
	RetryDelay time.Duration

	// FetchTimeout is the timeout for a single fetch attempt.
	FetchTimeout time.Duration

	// MaxConcurrentFetches limits parallel fetch operations.
	MaxConcurrentFetches int
}

// DefaultFetcherConfig returns sensible defaults for the Fetcher.
func DefaultFetcherConfig() FetcherConfig {
	return FetcherConfig{
		MaxRetries:           3,
		RetryDelay:           100 * time.Millisecond,
		FetchTimeout:         5 * time.Second,
		MaxConcurrentFetches: 10,
	}
}

// Fetcher handles fetching missing batches and certificates from peers.
// Standalone component with retry logic and request deduplication.
type Fetcher[H Hash, T Transaction[H]] struct {
	mu sync.Mutex

	cfg        FetcherConfig
	network    Network[H, T]
	storage    Storage[H, T]
	validators ValidatorSet
	hashFunc   func([]byte) H

	// Pending fetch requests (digest string -> channel for result)
	pendingBatches map[string][]chan fetchResult[*Batch[H, T]]
	pendingCerts   map[string][]chan fetchResult[*Certificate[H]]

	// Semaphore for limiting concurrent fetches
	sem chan struct{}

	hooks  *Hooks[H, T]
	logger *zap.Logger
}

// fetchResult wraps a fetch result with possible error.
type fetchResult[T any] struct {
	value T
	err   error
}

// NewFetcher creates a new Fetcher instance.
func NewFetcher[H Hash, T Transaction[H]](
	cfg FetcherConfig,
	network Network[H, T],
	storage Storage[H, T],
	validators ValidatorSet,
	hashFunc func([]byte) H,
	logger *zap.Logger,
) *Fetcher[H, T] {
	return NewFetcherWithHooks(cfg, network, storage, validators, hashFunc, nil, logger)
}

// NewFetcherWithHooks creates a new Fetcher instance with observability hooks.
func NewFetcherWithHooks[H Hash, T Transaction[H]](
	cfg FetcherConfig,
	network Network[H, T],
	storage Storage[H, T],
	validators ValidatorSet,
	hashFunc func([]byte) H,
	hooks *Hooks[H, T],
	logger *zap.Logger,
) *Fetcher[H, T] {
	if logger == nil {
		logger = zap.NewNop()
	}
	if cfg.MaxConcurrentFetches <= 0 {
		cfg.MaxConcurrentFetches = 10
	}

	return &Fetcher[H, T]{
		cfg:            cfg,
		network:        network,
		storage:        storage,
		validators:     validators,
		hashFunc:       hashFunc,
		pendingBatches: make(map[string][]chan fetchResult[*Batch[H, T]]),
		pendingCerts:   make(map[string][]chan fetchResult[*Certificate[H]]),
		sem:            make(chan struct{}, cfg.MaxConcurrentFetches),
		hooks:          hooks,
		logger:         logger,
	}
}

// FetchBatch fetches a batch by digest, trying multiple peers if needed.
// It deduplicates concurrent requests for the same digest.
func (f *Fetcher[H, T]) FetchBatch(ctx context.Context, digest H, preferredPeer uint16) (*Batch[H, T], error) {
	// Check local storage first
	if batch, err := f.storage.GetBatch(digest); err == nil {
		return batch, nil
	}

	digestStr := digest.String()

	// Check for existing pending request
	f.mu.Lock()
	if waiters, exists := f.pendingBatches[digestStr]; exists {
		// Join existing fetch request
		resultCh := make(chan fetchResult[*Batch[H, T]], 1)
		f.pendingBatches[digestStr] = append(waiters, resultCh)
		f.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case result := <-resultCh:
			return result.value, result.err
		}
	}

	// Start new fetch request
	resultCh := make(chan fetchResult[*Batch[H, T]], 1)
	f.pendingBatches[digestStr] = []chan fetchResult[*Batch[H, T]]{resultCh}
	f.mu.Unlock()

	// Invoke hook
	startTime := time.Now()
	if f.hooks != nil && f.hooks.OnFetchStarted != nil {
		f.hooks.OnFetchStarted(FetchStartedEvent[H]{
			Type:          FetchTypeBatch,
			Digest:        digest,
			PreferredPeer: preferredPeer,
			StartedAt:     startTime,
		})
	}

	// Acquire semaphore
	select {
	case f.sem <- struct{}{}:
	case <-ctx.Done():
		f.completeBatchFetch(digestStr, nil, ctx.Err())
		return nil, ctx.Err()
	}

	// Perform fetch in background
	go func() {
		defer func() { <-f.sem }()

		batch, err := f.fetchBatchWithRetry(ctx, digest, preferredPeer)
		f.completeBatchFetchWithHook(digestStr, digest, batch, err, startTime, preferredPeer)
	}()

	// Wait for result
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultCh:
		return result.value, result.err
	}
}

func (f *Fetcher[H, T]) fetchBatchWithRetry(ctx context.Context, digest H, preferredPeer uint16) (*Batch[H, T], error) {
	peers := f.getPeersToTry(preferredPeer)
	var lastErr error

	for attempt := 0; attempt <= f.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			delay := f.cfg.RetryDelay * time.Duration(1<<(attempt-1))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}

		// Try each peer
		for _, peer := range peers {
			_, cancel := context.WithTimeout(ctx, f.cfg.FetchTimeout)
			batch, err := f.network.FetchBatch(peer, digest)
			cancel()

			if err != nil {
				lastErr = err
				f.logger.Debug("batch fetch attempt failed",
					zap.String("digest", digest.String()),
					zap.Uint16("peer", peer),
					zap.Int("attempt", attempt),
					zap.Error(err))
				continue
			}

			// Verify the batch
			if err := batch.Verify(f.hashFunc); err != nil {
				lastErr = fmt.Errorf("invalid batch from peer %d: %w", peer, err)
				f.logger.Warn("received invalid batch",
					zap.String("digest", digest.String()),
					zap.Uint16("peer", peer),
					zap.Error(err))
				continue
			}

			// Store the batch
			if err := f.storage.PutBatch(batch); err != nil {
				f.logger.Warn("failed to store fetched batch", zap.Error(err))
			}

			f.logger.Debug("batch fetched successfully",
				zap.String("digest", digest.String()),
				zap.Uint16("peer", peer),
				zap.Int("attempt", attempt))

			return batch, nil
		}
	}

	return nil, fmt.Errorf("failed to fetch batch %s after %d attempts: %w",
		digest.String(), f.cfg.MaxRetries+1, lastErr)
}

func (f *Fetcher[H, T]) completeBatchFetch(digestStr string, batch *Batch[H, T], err error) {
	f.mu.Lock()
	waiters := f.pendingBatches[digestStr]
	delete(f.pendingBatches, digestStr)
	f.mu.Unlock()

	result := fetchResult[*Batch[H, T]]{value: batch, err: err}
	for _, ch := range waiters {
		ch <- result
		close(ch)
	}
}

func (f *Fetcher[H, T]) completeBatchFetchWithHook(digestStr string, digest H, batch *Batch[H, T], err error, startTime time.Time, preferredPeer uint16) {
	// Invoke hook before notifying waiters
	if f.hooks != nil && f.hooks.OnFetchCompleted != nil {
		var fromPeer uint16
		if batch != nil {
			fromPeer = batch.ValidatorID
		}
		f.hooks.OnFetchCompleted(FetchCompletedEvent[H]{
			Type:        FetchTypeBatch,
			Digest:      digest,
			Success:     err == nil,
			Error:       err,
			Attempts:    0, // We don't track attempts here, could enhance later
			FromPeer:    fromPeer,
			Latency:     time.Since(startTime),
			CompletedAt: time.Now(),
		})
	}

	f.completeBatchFetch(digestStr, batch, err)
}

// FetchCertificate fetches a certificate by digest, trying multiple peers if needed.
func (f *Fetcher[H, T]) FetchCertificate(ctx context.Context, digest H, preferredPeer uint16) (*Certificate[H], error) {
	// Check local storage first
	if cert, err := f.storage.GetCertificate(digest); err == nil {
		return cert, nil
	}

	digestStr := digest.String()

	// Check for existing pending request
	f.mu.Lock()
	if waiters, exists := f.pendingCerts[digestStr]; exists {
		resultCh := make(chan fetchResult[*Certificate[H]], 1)
		f.pendingCerts[digestStr] = append(waiters, resultCh)
		f.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case result := <-resultCh:
			return result.value, result.err
		}
	}

	// Start new fetch request
	resultCh := make(chan fetchResult[*Certificate[H]], 1)
	f.pendingCerts[digestStr] = []chan fetchResult[*Certificate[H]]{resultCh}
	f.mu.Unlock()

	// Invoke hook
	startTime := time.Now()
	if f.hooks != nil && f.hooks.OnFetchStarted != nil {
		f.hooks.OnFetchStarted(FetchStartedEvent[H]{
			Type:          FetchTypeCertificate,
			Digest:        digest,
			PreferredPeer: preferredPeer,
			StartedAt:     startTime,
		})
	}

	// Acquire semaphore
	select {
	case f.sem <- struct{}{}:
	case <-ctx.Done():
		f.completeCertFetch(digestStr, nil, ctx.Err())
		return nil, ctx.Err()
	}

	// Perform fetch in background
	go func() {
		defer func() { <-f.sem }()

		cert, err := f.fetchCertWithRetry(ctx, digest, preferredPeer)
		f.completeCertFetchWithHook(digestStr, digest, cert, err, startTime, preferredPeer)
	}()

	// Wait for result
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultCh:
		return result.value, result.err
	}
}

func (f *Fetcher[H, T]) fetchCertWithRetry(ctx context.Context, digest H, preferredPeer uint16) (*Certificate[H], error) {
	peers := f.getPeersToTry(preferredPeer)
	var lastErr error

	for attempt := 0; attempt <= f.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := f.cfg.RetryDelay * time.Duration(1<<(attempt-1))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}

		for _, peer := range peers {
			_, cancel := context.WithTimeout(ctx, f.cfg.FetchTimeout)
			cert, err := f.network.FetchCertificate(peer, digest)
			cancel()

			if err != nil {
				lastErr = err
				f.logger.Debug("certificate fetch attempt failed",
					zap.String("digest", digest.String()),
					zap.Uint16("peer", peer),
					zap.Int("attempt", attempt),
					zap.Error(err))
				continue
			}

			// Validate the certificate
			if err := cert.Validate(f.validators); err != nil {
				lastErr = fmt.Errorf("invalid certificate from peer %d: %w", peer, err)
				f.logger.Warn("received invalid certificate",
					zap.String("digest", digest.String()),
					zap.Uint16("peer", peer),
					zap.Error(err))
				continue
			}

			// Store the certificate
			if err := f.storage.PutCertificate(cert); err != nil {
				f.logger.Warn("failed to store fetched certificate", zap.Error(err))
			}

			f.logger.Debug("certificate fetched successfully",
				zap.String("digest", digest.String()),
				zap.Uint16("peer", peer),
				zap.Int("attempt", attempt))

			return cert, nil
		}
	}

	return nil, fmt.Errorf("failed to fetch certificate %s after %d attempts: %w",
		digest.String(), f.cfg.MaxRetries+1, lastErr)
}

func (f *Fetcher[H, T]) completeCertFetch(digestStr string, cert *Certificate[H], err error) {
	f.mu.Lock()
	waiters := f.pendingCerts[digestStr]
	delete(f.pendingCerts, digestStr)
	f.mu.Unlock()

	result := fetchResult[*Certificate[H]]{value: cert, err: err}
	for _, ch := range waiters {
		ch <- result
		close(ch)
	}
}

func (f *Fetcher[H, T]) completeCertFetchWithHook(digestStr string, digest H, cert *Certificate[H], err error, startTime time.Time, preferredPeer uint16) {
	// Invoke hook before notifying waiters
	if f.hooks != nil && f.hooks.OnFetchCompleted != nil {
		var fromPeer uint16
		if cert != nil {
			fromPeer = cert.Header.Author
		}
		f.hooks.OnFetchCompleted(FetchCompletedEvent[H]{
			Type:        FetchTypeCertificate,
			Digest:      digest,
			Success:     err == nil,
			Error:       err,
			Attempts:    0, // We don't track attempts here, could enhance later
			FromPeer:    fromPeer,
			Latency:     time.Since(startTime),
			CompletedAt: time.Now(),
		})
	}

	f.completeCertFetch(digestStr, cert, err)
}

// FetchBatchesParallel fetches multiple batches in parallel.
func (f *Fetcher[H, T]) FetchBatchesParallel(ctx context.Context, digests []H, preferredPeer uint16) ([]*Batch[H, T], error) {
	if len(digests) == 0 {
		return nil, nil
	}

	type indexedResult struct {
		index int
		batch *Batch[H, T]
		err   error
	}

	resultCh := make(chan indexedResult, len(digests))
	var wg sync.WaitGroup

	for i, digest := range digests {
		wg.Add(1)
		go func(idx int, d H) {
			defer wg.Done()
			batch, err := f.FetchBatch(ctx, d, preferredPeer)
			resultCh <- indexedResult{index: idx, batch: batch, err: err}
		}(i, digest)
	}

	// Close channel when all fetches complete
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect results
	batches := make([]*Batch[H, T], len(digests))
	var firstErr error

	for result := range resultCh {
		if result.err != nil && firstErr == nil {
			firstErr = result.err
		}
		batches[result.index] = result.batch
	}

	if firstErr != nil {
		return nil, firstErr
	}

	return batches, nil
}

// getPeersToTry returns a list of peers to try, with preferredPeer first.
func (f *Fetcher[H, T]) getPeersToTry(preferredPeer uint16) []uint16 {
	n := f.validators.Count()
	peers := make([]uint16, 0, n)

	// Add preferred peer first
	if f.validators.Contains(preferredPeer) {
		peers = append(peers, preferredPeer)
	}

	// Add other peers
	for i := 0; i < n; i++ {
		peer := uint16(i)
		if peer != preferredPeer {
			peers = append(peers, peer)
		}
	}

	return peers
}

// PendingFetchCount returns the number of pending fetch operations.
func (f *Fetcher[H, T]) PendingFetchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pendingBatches) + len(f.pendingCerts)
}
