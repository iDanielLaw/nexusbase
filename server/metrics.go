package server

import (
	"expvar"
	"log/slog"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
)

// SystemCollector is responsible for periodically collecting system-level metrics
// like CPU and Disk usage and publishing them via expvar.
type SystemCollector struct {
	cpuUsagePercent *expvar.Float
	memUsagePercent *expvar.Float
	diskUsage       *expvar.Float
	diskPath        string
	interval        time.Duration
	stopChan        chan struct{}
	wg              sync.WaitGroup
	logger          *slog.Logger
}

// NewSystemCollector creates a new collector.
// diskPath should be the path of the disk to monitor (e.g., the data directory).
func NewSystemCollector(diskPath string, interval time.Duration, logger *slog.Logger) *SystemCollector {
	return &SystemCollector{
		cpuUsagePercent: expvar.NewFloat("system_cpu_usage_percent"),
		memUsagePercent: expvar.NewFloat("system_mem_usage_percent"),
		diskUsage:       expvar.NewFloat("system_disk_usage_percent"),
		diskPath:        diskPath,
		interval:        interval,
		stopChan:        make(chan struct{}),
		logger:          logger.With("component", "SystemCollector"),
	}
}

// Start begins the background collection loop.
func (sc *SystemCollector) Start() {
	sc.logger.Info("Starting system metrics collector", "interval", sc.interval)
	sc.wg.Add(1)
	go sc.collectLoop()
}

// Stop signals the collection loop to terminate and waits for it to finish.
func (sc *SystemCollector) Stop() {
	sc.logger.Info("Stopping system metrics collector")
	close(sc.stopChan)
	sc.wg.Wait()
}

func (sc *SystemCollector) collectLoop() {
	defer sc.wg.Done()
	ticker := time.NewTicker(sc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Collect CPU Usage
			// The interval for cpu.Percent should be slightly less than the ticker interval
			// to avoid race conditions where the next tick arrives before the measurement is done.
			cpuPercentages, err := cpu.Percent(sc.interval-time.Second, false)
			if err == nil && len(cpuPercentages) > 0 {
				sc.cpuUsagePercent.Set(cpuPercentages[0])
			}

			// Collect Memory Usage
			if vm, err := mem.VirtualMemory(); err == nil {
				sc.memUsagePercent.Set(vm.UsedPercent)
			}

			// Collect Disk Usage
			if du, err := disk.Usage(sc.diskPath); err == nil {
				sc.diskUsage.Set(du.UsedPercent)
			}
		case <-sc.stopChan:
			return
		}
	}
}
