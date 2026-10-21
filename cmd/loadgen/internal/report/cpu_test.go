package report

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReadCPUSplitsPhasesAndKeepsPeakMemory(t *testing.T) {
	dir := t.TempDir()
	stats := "" +
		"99,wallet-1,90.00%,60MiB / 64MiB\n" + // before the start: ignored
		"100,wallet-1,20.00%,30.5MiB / 64MiB\n" +
		"101,wallet-1,24.00%,48MiB / 64MiB\n" +
		"102,wallet-1,10.00%,40MiB / 64MiB\n" + // drain
		"100,postgres,50.00%,1.5GiB / 11.68GiB\n" +
		"100,loadgen,1.00%\n" // old format without memory
	if err := os.WriteFile(filepath.Join(dir, DockerStatsFile), []byte(stats), 0o600); err != nil {
		t.Fatal(err)
	}
	rows := readCPU(dir, time.Unix(100, 0), time.Unix(101, 0))
	by := map[string]CPURow{}
	for _, r := range rows {
		by[r.Service] = r
	}
	w := by["wallet-1"]
	if w.LoadMean != 22 || w.LoadMax != 24 || w.DrainMax != 10 || w.MemMaxMiB != 48 || w.MemLimitMiB != 64 {
		t.Fatalf("wallet-1: %+v", w)
	}
	if p := by["postgres"]; p.MemMaxMiB != 1536 || p.MemLimitMiB < 11960 || p.MemLimitMiB > 11961 {
		t.Fatalf("postgres: %+v", p)
	}
	if l := by["loadgen"]; l.LoadMean != 1 || l.MemLimitMiB != 0 || memCell(l) != "—" {
		t.Fatalf("loadgen: %+v", l)
	}
}
