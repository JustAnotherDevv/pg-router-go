//go:build linux

package multiproc

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
)

const envWorker = "PGRouter_WORKER"

func IsWorker() bool { return os.Getenv(envWorker) == "1" }

func WorkerID() int {
	id, _ := strconv.Atoi(os.Getenv("PGRouter_WORKER_ID"))
	return id
}

func SpawnWorkers(n int, configPath string) *sync.WaitGroup {
	if n <= 1 {
		return nil
	}

	binary, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "multiproc: executable: %v\n", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	children := make([]*exec.Cmd, 0, n-1)

	for i := 1; i < n; i++ {
		cmd := exec.Command(binary, "run", "--config", configPath)
		cmd.Env = append(os.Environ(),
			envWorker+"=1",
			fmt.Sprintf("PGRouter_WORKER_ID=%d", i),
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

		if err := cmd.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "multiproc: spawn worker %d: %v\n", i, err)
			for _, c := range children {
				_ = c.Process.Signal(syscall.SIGTERM)
			}
			os.Exit(1)
		}
		children = append(children, cmd)
		wg.Add(1)
		go func(c *exec.Cmd, id int) {
			defer wg.Done()
			_ = c.Wait()
		}(cmd, i)
	}

	sigCh := make(chan os.Signal, 16)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGINT)
	go func() {
		for sig := range sigCh {
			for _, c := range children {
				_ = c.Process.Signal(sig)
			}
		}
	}()

	return &wg
}
