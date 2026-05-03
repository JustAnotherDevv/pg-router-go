// Daemon: shared lifecycle scaffolding for long-running types.
//
// Three production types — replica.Manager, replica.PrimaryMonitor,
// pkg/pgrouter.Server — independently re-implement the same
// startOnce + stopOnce + stopCh + wg quartet. Bugs in one (e.g.
// goroutine leak on early Stop) used to need three separate patches.
//
// Daemon embeds via composition. Owners call:
//   - d.Run(fn): launches fn as a goroutine tracked by the internal
//     WaitGroup. Safe across multiple goroutines.
//   - d.StopCh(): channel closed when Stop fires; goroutines select on
//     it to exit cleanly.
//   - d.Stop(): closes StopCh (once), waits for all Run goroutines.
//   - d.Start(once): runs `once` exactly once across concurrent calls.
//
// All four are safe to call concurrently + idempotent. Daemon's zero
// value is ready to use.

package util

import "sync"

// Daemon is the lifecycle primitive embedded by long-running types.
//
// Zero value is usable — the stopCh is allocated lazily via sync.Once
// so embedders don't need an explicit constructor.
type Daemon struct {
	stopOnce  sync.Once
	stopChMu  sync.Mutex
	stopCh    chan struct{}
	startOnce sync.Once
	wg        sync.WaitGroup
}

// ensureCh creates the stopCh if it doesn't exist yet. Caller must
// hold no Daemon locks. Returns the channel.
func (d *Daemon) ensureCh() chan struct{} {
	d.stopChMu.Lock()
	if d.stopCh == nil {
		d.stopCh = make(chan struct{})
	}
	c := d.stopCh
	d.stopChMu.Unlock()
	return c
}

// StopCh returns the channel that closes when Stop fires. Goroutines
// pass this into their select.
func (d *Daemon) StopCh() <-chan struct{} {
	return d.ensureCh()
}

// Start runs `once` exactly once across concurrent calls. Use for
// goroutine spawning that must be idempotent (replica.Manager.Start /
// PrimaryMonitor.Start were both bitten by double-Start before).
func (d *Daemon) Start(once func()) {
	d.startOnce.Do(once)
}

// Run launches fn as a tracked goroutine. Stop's wg.Wait will block
// for all in-flight Run-spawned goroutines.
func (d *Daemon) Run(fn func()) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		fn()
	}()
}

// Stop closes StopCh (idempotent) and waits for all Run goroutines.
func (d *Daemon) Stop() {
	d.stopOnce.Do(func() {
		ch := d.ensureCh()
		close(ch)
	})
	d.wg.Wait()
}
