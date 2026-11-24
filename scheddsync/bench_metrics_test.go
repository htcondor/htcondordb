package scheddsync

// Ingest cost of resource sampling. The question these answer is what sampling adds to the
// tailer's steady state -- and in particular what it costs to compare each incoming attribute
// against the value the row already holds, which is a decode of the job ad per transaction on top
// of the one the sampler already does to build the row.
//
// The log is synthetic, which is the wrong shape for a throughput number (see bench_ingest_test.go
// for why a real log is required there) but the right one for this comparison: what the extra
// decode costs is set by how WIDE the job ad is and how many attributes a shadow flush re-sends,
// and both are reproduced here directly. Run the two sub-benchmarks against each other, not
// against another machine's numbers.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// benchJobAd is a job ad of roughly the width a real one has: the submit-time attributes a schedd
// carries for every job, which is what a pre-image decode has to walk.
func benchJobAd(cluster, proc int) string {
	var b strings.Builder
	key := fmt.Sprintf("%d.%d", cluster, proc)
	fmt.Fprintf(&b, "101 %s Job Machine\n", key)
	set := func(name, value string) { fmt.Fprintf(&b, "103 %s %s %s\n", key, name, value) }
	set("ClusterId", fmt.Sprint(cluster))
	set("ProcId", fmt.Sprint(proc))
	set("Owner", `"alice"`)
	set("User", `"alice@example.org"`)
	set("GlobalJobId", fmt.Sprintf(`"ap.example.org#%d.%d#1700000000"`, cluster, proc))
	set("JobStatus", "1")
	set("JobUniverse", "5")
	set("RequestCpus", "2")
	set("RequestMemory", "4096")
	set("RequestDisk", "10485760")
	set("Cmd", `"/home/alice/run.sh"`)
	set("Iwd", `"/home/alice/work"`)
	set("Args", `"--input data.root --output out.root"`)
	set("Environment", `"PATH=/usr/bin;HOME=/home/alice"`)
	set("Requirements", "(TARGET.Arch == \"X86_64\") && (TARGET.OpSys == \"LINUX\") && (TARGET.Disk >= RequestDisk)")
	set("Rank", "0.0")
	set("TransferInput", `"data.root,setup.sh,lib.tar.gz"`)
	set("TransferOutput", `"out.root"`)
	set("QDate", "1700000000")
	set("JobLeaseDuration", "2400")
	set("LeaveJobInQueue", "false")
	set("OnExitHold", "false")
	set("OnExitRemove", "true")
	set("PeriodicHold", "false")
	set("PeriodicRelease", "false")
	set("PeriodicRemove", "false")
	set("WantCheckpoint", "false")
	set("WantRemoteIO", "true")
	set("WantRemoteSyscalls", "false")
	set("ShouldTransferFiles", `"YES"`)
	set("WhenToTransferOutput", `"ON_EXIT"`)
	set("CondorVersion", `"$CondorVersion: 24.0.0 Jan 01 2026 BuildID: 1 $"`)
	set("CondorPlatform", `"$CondorPlatform: X86_64-CentOS_9 $"`)
	set("AccountingGroup", `"group_physics.alice"`)
	set("ProjectName", `"CMS"`)
	set("NumJobStarts", "0")
	set("NumShadowStarts", "0")
	set("NumRestarts", "0")
	set("NumSystemHolds", "0")
	set("TotalSuspensions", "0")
	set("CumulativeSlotTime", "0")
	set("CommittedTime", "0")
	set("CommittedSlotTime", "0")
	set("CumulativeSuspensionTime", "0")
	set("RemoteWallClockTime", "0.0")
	set("ExitStatus", "0")
	set("ImageSize", "0")
	set("DiskUsage", "0")
	set("ExecutableSize", "128")
	set("MinHosts", "1")
	set("MaxHosts", "1")
	set("CurrentHosts", "0")
	set("JobPrio", "0")
	set("NiceUser", "false")
	set("CoreSize", "0")
	set("KillSig", `"SIGTERM"`)
	set("RootDir", `"/"`)
	set("JobNotification", "0")
	set("UserLog", `"/home/alice/work/run.log"`)
	set("Out", `"out.txt"`)
	set("Err", `"err.txt"`)
	set("In", `"/dev/null"`)
	set("BufferSize", "524288")
	set("BufferBlockSize", "32768")
	set("StreamOut", "false")
	set("StreamErr", "false")
	set("MyType", `"Job"`)
	set("TargetType", `"Machine"`)
	return b.String()
}

// benchShadowFlush is one periodic shadow update: the whitelist QmgrJobUpdater re-sends on every
// flush. Most of it is unchanged from the previous flush, which is the whole point -- that is the
// traffic the comparison in note has to sort through.
func benchShadowFlush(cluster, proc, round int) string {
	var b strings.Builder
	key := fmt.Sprintf("%d.%d", cluster, proc)
	at := 1700000000 + 300*round
	b.WriteString("105\n")
	set := func(name, value string) { fmt.Fprintf(&b, "103 %s %s %s\n", key, name, value) }
	// Moves on every flush.
	set("StatsLastUpdateTimeStarter", fmt.Sprint(at))
	set("StatsLifetimeStarter", fmt.Sprint(300*round))
	set("RemoteUserCpu", fmt.Sprintf("%d.0", 540*round))
	set("RemoteSysCpu", fmt.Sprintf("%d.0", 20*round))
	set("BlockReadBytes", fmt.Sprint(1048576*round))
	set("BlockWriteBytes", fmt.Sprint(524288*round))
	set("BytesSent", fmt.Sprintf("%d.0", 4096*round))
	set("BytesRecvd", fmt.Sprintf("%d.0", 1048576*round))
	set("RemoteWallClockTime", fmt.Sprintf("%d.0", 300*round))
	set("CommittedTime", fmt.Sprint(300*round))
	// Re-sent unchanged, exactly as the shadow does.
	set("JobStatus", "2")
	set("NumShadowStarts", "1")
	set("NumJobStarts", "1")
	set("JobCurrentStartExecutingDate", "1700000000")
	set("MemoryUsage", "2048")
	set("ResidentSetSize", "2097152")
	set("ImageSize", "2097152")
	set("DiskUsage", "4096")
	set("ScratchDirFileCount", "12")
	set("BlockReads", "1024")
	set("BlockWrites", "512")
	set("CumulativeSuspensionTime", "0")
	set("TotalSuspensions", "0")
	set("CommittedSlotTime", "0")
	set("CumulativeSlotTime", "0")
	b.WriteString("106\n")
	return b.String()
}

// BenchmarkJobMetricsTail measures the steady state: a mirror holding the queue, fed shadow
// flushes for every running job. "off" is the tailer with sampling disabled -- the floor.
func BenchmarkJobMetricsTail(b *testing.B) {
	const (
		jobs   = 500
		rounds = 4
	)
	var head strings.Builder
	head.WriteString("105\n")
	for i := 0; i < jobs; i++ {
		head.WriteString(benchJobAd(i+1, 0))
	}
	head.WriteString("106\n")
	// Each job starts a shadow.
	for i := 0; i < jobs; i++ {
		fmt.Fprintf(&head, "105\n103 %d.0 JobStatus 2\n103 %d.0 NumShadowStarts 1\n"+
			"103 %d.0 JobCurrentStartExecutingDate 1700000000\n106\n", i+1, i+1, i+1)
	}
	var tail strings.Builder
	for r := 1; r <= rounds; r++ {
		for i := 0; i < jobs; i++ {
			tail.WriteString(benchShadowFlush(i+1, 0, r))
		}
	}

	for _, sampling := range []bool{false, true} {
		name := "off"
		if sampling {
			name = "on"
		}
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir := b.TempDir()
				logPath := filepath.Join(dir, "job_queue.log")
				if err := os.WriteFile(logPath, []byte("105\n106\n"+head.String()), 0o600); err != nil {
					b.Fatal(err)
				}
				jobsTable, cfg := benchTables(b, dir, false)
				cfg.Filename = logPath
				cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
				if sampling {
					arch, cleanup := newMetricsArchive(b)
					b.Cleanup(cleanup)
					cfg.Metrics = JobMetricsConfig{Archive: arch, Logger: cfg.Logger}
				}
				s := NewJobSync(jobsTable, cfg)
				if err := s.Poll(context.Background()); err != nil {
					b.Fatalf("warm poll: %v", err)
				}
				f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := f.WriteString(tail.String()); err != nil {
					b.Fatal(err)
				}
				f.Close()
				b.StartTimer()

				if err := s.Poll(context.Background()); err != nil {
					b.Fatalf("tail poll: %v", err)
				}

				b.StopTimer()
				if sampling {
					st := s.metrics.status()
					b.ReportMetric(float64(st.Appended), "samples")
					b.ReportMetric(float64(st.Unchanged), "unchanged")
				}
				b.StartTimer()
			}
		})
	}
}
