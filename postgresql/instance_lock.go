package postgresql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gofrs/flock"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

type instanceLockSlot struct {
	once  sync.Once
	flock *flock.Flock
	err   error
}

// instanceLockSlots deduplicates flock acquisition when multiple provider aliases
// use the same instance_name in one Terraform process. Without it, a second
// Configure would open another fd and TryLock could succeed in-process while
// the first alias already holds the lock.
var instanceLockSlots sync.Map // map[string]*instanceLockSlot

// acquireRunInstanceLock takes an exclusive cross-process lock for the Terraform
// run. The lock is held until process exit.
func acquireRunInstanceLock(ctx context.Context, cfg *Config) error {
	lockDir := cfg.InstanceLockDir
	if lockDir == "" {
		lockDir = defaultInstanceLockDir
	}
	lockPath := filepath.Join(lockDir, cfg.InstanceName+".lock")

	slot := &instanceLockSlot{}
	actual, _ := instanceLockSlots.LoadOrStore(cfg.InstanceName, slot)
	s := actual.(*instanceLockSlot)

	s.once.Do(func() {
		if err := os.MkdirAll(lockDir, 0o700); err != nil {
			s.err = fmt.Errorf("create lock directory %q: %w", lockDir, err)
			return
		}

		fl := flock.New(lockPath)

		waitInterval := cfg.InstanceLockWaitLogInterval
		if waitInterval <= 0 {
			waitInterval = defaultInstanceLockWaitLogInterval
		}

		s.err = tryAcquireExclusiveInstanceLock(ctx, fl, lockPath, waitInterval, cfg.InstanceLockTimeout)
		if s.err == nil {
			s.flock = fl
		}
	})

	return s.err
}

func tryAcquireExclusiveInstanceLock(ctx context.Context, fl *flock.Flock, lockPath string, waitLogIntervalSec, timeoutSec int) error {
	deadline := time.Time{}
	if timeoutSec > 0 {
		deadline = time.Now().Add(time.Duration(timeoutSec) * time.Second)
	}

	waitInterval := time.Duration(waitLogIntervalSec) * time.Second
	var lastLog time.Time

	for {
		locked, err := fl.TryLock()
		if err != nil {
			return fmt.Errorf("flock %q: %w", lockPath, err)
		}
		if locked {
			return nil
		}

		if !deadline.IsZero() && time.Now().After(deadline) {
			return fmt.Errorf("timed out after %ds waiting for instance lock %q (another Terraform process is using this PostgreSQL instance)", timeoutSec, lockPath)
		}

		now := time.Now()
		if lastLog.IsZero() || now.Sub(lastLog) >= waitInterval {
			tflog.Info(ctx, "waiting for instance lock (another Terraform process is using this PostgreSQL instance)", map[string]any{
				"lock_path": lockPath,
			})
			lastLog = now
		}

		time.Sleep(waitInterval)
	}
}
