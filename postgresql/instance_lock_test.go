package postgresql

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for file %q", path)
}

func TestAcquireRunInstanceLock_DedupesSameProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("flock not supported on windows")
	}

	dir := t.TempDir()
	cfg := &Config{
		InstanceLock:                true,
		InstanceName:                "test-dedupe",
		InstanceLockDir:             dir,
		InstanceLockWaitLogInterval: 1,
		InstanceLockTimeout:         5,
	}

	ctx := context.Background()
	require.NoError(t, acquireRunInstanceLock(ctx, cfg))
	require.NoError(t, acquireRunInstanceLock(ctx, cfg))

	_, ok := instanceLockSlots.Load("test-dedupe")
	require.True(t, ok)
}

func TestAcquireRunInstanceLock_CrossProcessBlocks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("flock not supported on windows")
	}

	dir := t.TempDir()
	lockPath := filepath.Join(dir, "crossproc.lock")
	readyPath := filepath.Join(dir, "crossproc.ready")

	holder := exec.Command(os.Args[0], "-test.run=TestHelperHoldInstanceLock", "--", dir, "crossproc")
	holder.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	require.NoError(t, holder.Start())
	t.Cleanup(func() {
		_ = holder.Process.Kill()
		_, _ = holder.Process.Wait()
	})

	waitForFile(t, readyPath, 10*time.Second)

	holderPID, err := strconv.Atoi(readFileString(t, readyPath))
	require.NoError(t, err)
	assert.NotEqual(t, os.Getpid(), holderPID)

	cfg := &Config{
		InstanceLock:                true,
		InstanceName:                "crossproc",
		InstanceLockDir:             dir,
		InstanceLockWaitLogInterval: 1,
		InstanceLockTimeout:         2,
	}

	err = acquireRunInstanceLock(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
	assert.Contains(t, err.Error(), lockPath)
}

func TestAcquireRunInstanceLock_AcquireAfterHolderExit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("flock not supported on windows")
	}

	dir := t.TempDir()
	readyPath := filepath.Join(dir, "serial.ready")

	holder := exec.Command(os.Args[0], "-test.run=TestHelperHoldInstanceLock", "--", dir, "serial")
	holder.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	require.NoError(t, holder.Start())

	waitForFile(t, readyPath, 10*time.Second)
	require.NoError(t, holder.Process.Kill())
	_, err := holder.Process.Wait()
	require.NoError(t, err)

	// Lock is released when the holder process exits (no explicit Unlock).

	cfg := &Config{
		InstanceLock:                true,
		InstanceName:                "serial",
		InstanceLockDir:             dir,
		InstanceLockWaitLogInterval: 1,
		InstanceLockTimeout:         5,
	}
	require.NoError(t, acquireRunInstanceLock(context.Background(), cfg))
}

func readFileString(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(b)
}

// TestHelperHoldInstanceLock is a subprocess helper that holds flock until killed.
func TestHelperHoldInstanceLock(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}

	args := os.Args
	var dir, name string
	for i, a := range args {
		if a == "--" && i+2 < len(args) {
			dir = args[i+1]
			name = args[i+2]
			break
		}
	}
	if dir == "" || name == "" {
		t.Fatal("missing -- dir name")
	}

	cfg := &Config{
		InstanceLock:                true,
		InstanceName:                name,
		InstanceLockDir:             dir,
		InstanceLockWaitLogInterval: 30,
	}
	require.NoError(t, acquireRunInstanceLock(context.Background(), cfg))

	readyPath := filepath.Join(dir, name+".ready")
	require.NoError(t, os.WriteFile(readyPath, []byte(strconv.Itoa(os.Getpid())), 0o600))
	select {}
}
