package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"workflowscheduler/internal/actions"
	"workflowscheduler/internal/api"
	"workflowscheduler/internal/domain"
	"workflowscheduler/internal/engine"
	"workflowscheduler/internal/queue"
	"workflowscheduler/internal/scheduler"
	"workflowscheduler/internal/store"
	"workflowscheduler/internal/telemetry"
	"workflowscheduler/internal/worker"
	"workflowscheduler/pkg/utils"
)

func main() {
	var (
		mode                    = flag.String("mode", "run", "run, schedule, serve, or worker")
		workflowFile            = flag.String("workflow", "examples/sample_workflow.json", "path to workflow JSON")
		storeBackend            = flag.String("store-backend", "file", "store backend: file or sqlite")
		dataDir                 = flag.String("data-dir", "data", "directory for persistent store")
		sqlitePath              = flag.String("sqlite-path", "data/orchestrator.db", "sqlite database path when store-backend=sqlite")
		apiAddr                 = flag.String("api-addr", ":8080", "http API listen address for serve mode")
		apiAutoPort             = flag.Bool("api-auto-port", false, "auto-select the next available port if api-addr is unavailable")
		apiAutoPortMaxOffset    = flag.Int("api-auto-port-max-offset", 20, "maximum port increments to probe when api-auto-port is enabled")
		apiToken                = flag.String("api-token", "", "legacy full-access API token")
		apiReadToken            = flag.String("api-read-token", "", "read-only API token")
		apiWriteToken           = flag.String("api-write-token", "", "write API token")
		interval                = flag.Duration("interval", 15*time.Second, "schedule interval")
		cronExpr                = flag.String("cron", "", "cron expression for schedule mode (alternative to interval)")
		runOnStartup            = flag.Bool("run-on-startup", true, "run once immediately when schedule starts")
		allowOverlap            = flag.Bool("allow-overlap", false, "allow overlapping scheduled runs")
		schedulerLockFile       = flag.String("scheduler-lock-file", "", "optional lock file path for single active scheduler leader")
		scheduleStoreFile       = flag.String("schedule-store-file", "", "optional JSON file path to persist scheduler registrations")
		workerID                = flag.String("worker-id", "", "worker identifier for worker mode")
		workerPoll              = flag.Duration("worker-poll", 500*time.Millisecond, "worker queue poll interval")
		workerLease             = flag.Duration("worker-lease", 30*time.Second, "worker queue lease duration")
		workerDrainTimeout      = flag.Duration("worker-drain-timeout", 10*time.Second, "grace period for worker drain on shutdown")
		queueRetentionEnabled   = flag.Bool("queue-retention-enabled", false, "enable periodic queue retention cleanup")
		queueRetentionStatus    = flag.String("queue-retention-status", "SUCCEEDED", "queue status to purge during retention cleanup")
		queueRetentionOlderThan = flag.Duration("queue-retention-older-than", 24*time.Hour, "minimum job age before purge")
		queueRetentionInterval  = flag.Duration("queue-retention-interval", 15*time.Minute, "interval for queue retention cleanup")
		queueRetentionBatch     = flag.Int("queue-retention-batch", 1000, "max jobs to purge per retention sweep")
		runFor                  = flag.Duration("run-for", 1*time.Minute, "how long scheduler mode runs before stopping")
	)
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	workflow, err := utils.LoadWorkflowFromFile(*workflowFile)
	if err != nil {
		log.Fatalf("load workflow: %v", err)
	}

	workflowStore, runQueue, cleanup, err := buildPersistence(*storeBackend, *dataDir, *sqlitePath)
	if err != nil {
		log.Fatalf("init persistence: %v", err)
	}
	defer cleanup()

	if err := workflowStore.SaveWorkflow(ctx, workflow); err != nil {
		log.Fatalf("save workflow: %v", err)
	}

	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, workflowStore)
	metricsCollector := telemetry.NewCollector()
	executor.SetMetricsCollector(metricsCollector)

	resolvedAPIToken := *apiToken
	if resolvedAPIToken == "" {
		resolvedAPIToken = os.Getenv("ORCHESTRATOR_API_TOKEN")
	}
	resolvedReadToken := *apiReadToken
	if resolvedReadToken == "" {
		resolvedReadToken = os.Getenv("ORCHESTRATOR_API_READ_TOKEN")
	}
	resolvedWriteToken := *apiWriteToken
	if resolvedWriteToken == "" {
		resolvedWriteToken = os.Getenv("ORCHESTRATOR_API_WRITE_TOKEN")
	}

	authConfig := buildAuthConfig(resolvedAPIToken, resolvedReadToken, resolvedWriteToken)

	switch *mode {
	case "run":
		run, err := executor.Run(ctx, workflow)
		if err != nil {
			log.Fatalf("execute workflow: %v", err)
		}
		printRun(run)

	case "schedule":
		svc := scheduler.NewService(executor, workflowStore)
		svc.SetRunQueue(runQueue)
		svc.SetLeaderLockPath(*schedulerLockFile)
		svc.SetScheduleStorePath(*scheduleStoreFile)
		err := svc.Register(domain.ScheduleConfig{
			WorkflowID:   workflow.ID,
			Interval:     *interval,
			Cron:         *cronExpr,
			RunOnStartup: *runOnStartup,
			AllowOverlap: *allowOverlap,
		})
		if err != nil {
			log.Fatalf("register schedule: %v", err)
		}
		svc.Start(ctx)

		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, *runFor)
		defer timeoutCancel()
		<-timeoutCtx.Done()

		runs, err := workflowStore.ListRuns(context.Background(), workflow.ID)
		if err != nil {
			log.Fatalf("list runs: %v", err)
		}
		fmt.Printf("scheduled run count: %d\n", len(runs))
		if len(runs) > 0 {
			printRun(runs[len(runs)-1])
		}

	case "serve":
		resolvedAPIAddr, err := resolveAPIAddr(*apiAddr, *apiAutoPort, *apiAutoPortMaxOffset)
		if err != nil {
			log.Fatal(apiServerStartError(*apiAddr, err))
		}
		if resolvedAPIAddr != *apiAddr {
			log.Printf("api address %s unavailable, falling back to %s", *apiAddr, resolvedAPIAddr)
		}

		svc := scheduler.NewService(executor, workflowStore)
		svc.SetRunQueue(runQueue)
		svc.SetLeaderLockPath(*schedulerLockFile)
		svc.SetScheduleStorePath(*scheduleStoreFile)
		svc.Start(ctx)

		server := api.NewServer(resolvedAPIAddr, authConfig, workflowStore, runQueue, executor, svc, metricsCollector)
		if *queueRetentionEnabled {
			server.SetQueueRetention(*queueRetentionStatus, *queueRetentionOlderThan, *queueRetentionInterval, *queueRetentionBatch)
		}
		fmt.Printf("api listening on %s\n", resolvedAPIAddr)
		if err := server.Start(ctx); err != nil {
			log.Fatal(apiServerStartError(resolvedAPIAddr, err))
		}

	case "worker":
		if runQueue == nil {
			log.Fatalf("worker mode requires a configured queue")
		}
		workerService := worker.NewService(*workerID, runQueue, workflowStore, executor)
		workerService.SetPolling(*workerPoll, *workerLease)
		workerService.SetMetricsCollector(metricsCollector)

		workerCtx, workerCancel := context.WithCancel(context.Background())
		defer workerCancel()

		go func() {
			<-ctx.Done()
			workerService.Drain()
			select {
			case <-time.After(*workerDrainTimeout):
			case <-workerCtx.Done():
			}
			workerCancel()
		}()

		workerService.Start(workerCtx)

	default:
		log.Fatalf("unsupported mode: %s", *mode)
	}
}

func buildPersistence(backend string, dataDir string, sqlitePath string) (domain.WorkflowStore, domain.RunQueue, func(), error) {
	switch strings.ToLower(strings.TrimSpace(backend)) {
	case "sqlite":
		sqlStore, err := store.NewSQLiteStore(sqlitePath)
		if err != nil {
			return nil, nil, nil, err
		}
		return sqlStore, sqlStore, func() { _ = sqlStore.Close() }, nil
	case "file", "":
		fileStore, err := store.NewFileStore(dataDir)
		if err != nil {
			return nil, nil, nil, err
		}
		return fileStore, queue.NewMemoryRunQueue(), func() {}, nil
	default:
		return nil, nil, nil, fmt.Errorf("unsupported store backend: %s", backend)
	}
}

func buildAuthConfig(adminToken string, readToken string, writeToken string) string {
	segments := make([]string, 0, 3)
	if strings.TrimSpace(adminToken) != "" {
		segments = append(segments, "admin:"+strings.TrimSpace(adminToken))
	}
	if strings.TrimSpace(readToken) != "" {
		segments = append(segments, "read:"+strings.TrimSpace(readToken))
	}
	if strings.TrimSpace(writeToken) != "" {
		segments = append(segments, "write:"+strings.TrimSpace(writeToken))
	}
	if len(segments) == 0 {
		return ""
	}
	return strings.Join(segments, ",")
}

func printRun(run domain.Run) {
	output, err := json.MarshalIndent(run, "", "  ")
	if err != nil {
		log.Fatalf("marshal run: %v", err)
	}
	fmt.Println(string(output))
}

func apiServerStartError(addr string, err error) string {
	if err == nil {
		return "api server error"
	}
	message := err.Error()
	lower := strings.ToLower(message)
	if strings.Contains(lower, "permission denied") || strings.Contains(lower, "access is denied") {
		return fmt.Sprintf("api server error: %v (insufficient permission to bind %s; choose a non-privileged port or run with appropriate privileges)", err, addr)
	}
	if strings.Contains(lower, "bind") || strings.Contains(lower, "address already in use") || strings.Contains(lower, "only one usage of each socket address") {
		return fmt.Sprintf("api server error: %v (address %s is already in use; choose a different -api-addr or stop the conflicting process)", err, addr)
	}
	return fmt.Sprintf("api server error: %v", err)
}

func checkAPIAddrAvailable(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return listener.Close()
}

func resolveAPIAddr(addr string, autoPort bool, maxOffset int) (string, error) {
	return resolveAPIAddrWithCheck(addr, autoPort, maxOffset, checkAPIAddrAvailable)
}

func resolveAPIAddrWithCheck(addr string, autoPort bool, maxOffset int, check func(string) error) (string, error) {
	if err := check(addr); err == nil {
		return addr, nil
	} else if !autoPort {
		return "", err
	} else {
		host, portStr, splitErr := net.SplitHostPort(addr)
		if splitErr != nil {
			return "", err
		}
		port, convErr := strconv.Atoi(portStr)
		if convErr != nil {
			return "", err
		}
		if maxOffset <= 0 {
			maxOffset = 20
		}
		for step := 1; step <= maxOffset; step++ {
			candidatePort := port + step
			candidateAddr := net.JoinHostPort(host, strconv.Itoa(candidatePort))
			if host == "" {
				candidateAddr = ":" + strconv.Itoa(candidatePort)
			}
			if candidateErr := check(candidateAddr); candidateErr == nil {
				return candidateAddr, nil
			}
		}
		return "", err
	}
}
