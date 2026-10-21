package report

import (
	"bufio"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// DockerStatsFile is written by loadtest/run.sh: one line per container per
// sample, "epochSeconds,composeService,cpuPercent,memUsage".
const DockerStatsFile = "docker_stats.csv"

// CPURow is CPU use of one service (100% = one core), split by phase, and
// its peak memory over the whole run against the limit Docker reports (the
// VM memory when the container has no limit).
type CPURow struct {
	Service     string  `json:"service"`
	LoadMean    float64 `json:"loadMeanPct"`
	LoadMax     float64 `json:"loadMaxPct"`
	DrainMean   float64 `json:"drainMeanPct"`
	DrainMax    float64 `json:"drainMaxPct"`
	MemMaxMiB   float64 `json:"memMaxMiB"`
	MemLimitMiB float64 `json:"memLimitMiB"`
	Samples     int     `json:"samples"`
}

// parseMem reads the "usage / limit" column of docker stats, e.g.
// "25.1MiB / 64MiB", in MiB.
func parseMem(s string) (usage, limit float64, ok bool) {
	u, l, found := strings.Cut(s, "/")
	if !found {
		return 0, 0, false
	}
	usage, ok1 := parseSize(strings.TrimSpace(u))
	limit, ok2 := parseSize(strings.TrimSpace(l))
	return usage, limit, ok1 && ok2
}

func parseSize(s string) (float64, bool) {
	units := []struct {
		suffix string
		mib    float64
	}{{"GiB", 1024}, {"MiB", 1}, {"KiB", 1.0 / 1024}, {"TiB", 1024 * 1024}, {"B", 1.0 / (1024 * 1024)}}
	for _, u := range units {
		if v, found := strings.CutSuffix(s, u.suffix); found {
			f, err := strconv.ParseFloat(v, 64)
			return f * u.mib, err == nil
		}
	}
	return 0, false
}

type acc struct {
	sum, max float64
	n        int
}

func (a *acc) add(v float64) {
	a.sum += v
	a.n++
	if v > a.max {
		a.max = v
	}
}

func (a acc) mean() float64 {
	if a.n == 0 {
		return 0
	}
	return a.sum / float64(a.n)
}

// readCPU splits samples into the load window [start, loadEnd] and the
// drain window after it.
func readCPU(dir string, start, loadEnd time.Time) []CPURow {
	f, err := os.Open(filepath.Join(dir, DockerStatsFile))
	if err != nil {
		return nil
	}
	defer f.Close()
	load, drain := map[string]*acc{}, map[string]*acc{}
	memMax, memLimit := map[string]float64{}, map[string]float64{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		parts := strings.Split(sc.Text(), ",")
		if len(parts) < 3 {
			continue
		}
		ts, err1 := strconv.ParseInt(parts[0], 10, 64)
		cpu, err2 := strconv.ParseFloat(strings.TrimSuffix(parts[2], "%"), 64)
		if err1 != nil || err2 != nil {
			continue
		}
		svc := parts[1]
		at := time.Unix(ts, 0)
		m := drain
		switch {
		case at.Before(start):
			continue
		case !at.After(loadEnd):
			m = load
		}
		if m[svc] == nil {
			m[svc] = &acc{}
		}
		m[svc].add(cpu)
		if len(parts) >= 4 {
			if usage, limit, ok := parseMem(parts[3]); ok {
				memMax[svc] = max(memMax[svc], usage)
				memLimit[svc] = limit
			}
		}
	}
	names := map[string]bool{}
	for k := range load {
		names[k] = true
	}
	for k := range drain {
		names[k] = true
	}
	var rows []CPURow
	for n := range names {
		r := CPURow{Service: n, MemMaxMiB: memMax[n], MemLimitMiB: memLimit[n]}
		if a := load[n]; a != nil {
			r.LoadMean, r.LoadMax, r.Samples = a.mean(), a.max, a.n
		}
		if a := drain[n]; a != nil {
			r.DrainMean, r.DrainMax = a.mean(), a.max
		}
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].LoadMean > rows[j].LoadMean })
	return rows
}

// meanCPU is the mean CPU of one service between from and to, or 0.
func meanCPU(dir, service string, from, to time.Time) float64 {
	f, err := os.Open(filepath.Join(dir, DockerStatsFile))
	if err != nil {
		return 0
	}
	defer f.Close()
	var a acc
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		parts := strings.Split(sc.Text(), ",")
		if len(parts) < 3 || parts[1] != service {
			continue
		}
		ts, err1 := strconv.ParseInt(parts[0], 10, 64)
		cpu, err2 := strconv.ParseFloat(strings.TrimSuffix(parts[2], "%"), 64)
		if err1 != nil || err2 != nil {
			continue
		}
		if at := time.Unix(ts, 0); !at.Before(from) && at.Before(to) {
			a.add(cpu)
		}
	}
	return a.mean()
}
