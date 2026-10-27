"""Phase 6: what the machine does while a run runs.

A thread samples, every `sample_s` seconds or as fast as the probes allow: each worker JVM (CPU share of one core, resident
memory and cumulative CPU time, from `ps`), the OSRM and Kafka containers (cumulative CPU and memory over the Docker
socket, which sees inside the virtual machine), every process on the host by bucket (the virtual machine itself,
Docker's host-side processes, the JVMs, everything else; from `ps`), and the machine's busy share over a one-second
interval (from `iostat`). The summary gives per series the mean, median, p95 and max, and the core-seconds each
consumed over the run; for the JVMs and the containers the exact CPU time charged to them. The probes cost about
40 ms of CPU per sample.
"""
import http.client, json, os, re, socket, subprocess, threading, time

def _ps(pids):
    """pid -> (cpu share of one core, rss MB, cpu seconds) for live pids."""
    if not pids:
        return {}
    r = subprocess.run(["ps", "-o", "pid=,%cpu=,rss=,cputime=", "-p", ",".join(map(str, pids))], capture_output=True, text=True)
    out = {}
    for line in r.stdout.splitlines():
        f = line.split()
        if len(f) == 4:
            out[int(f[0])] = (float(f[1]), float(f[2]) / 1024, _cputime(f[3]))
    return out

def _cputime(s):
    """[[dd-]hh:]mm:ss.ss -> seconds."""
    days = 0
    if "-" in s:
        d, s = s.split("-"); days = int(d)
    parts = [float(x) for x in s.split(":")]
    while len(parts) < 3:
        parts.insert(0, 0.0)
    return days * 86400 + parts[0] * 3600 + parts[1] * 60 + parts[2]

class _Sock(http.client.HTTPConnection):
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); self.sock.connect("/var/run/docker.sock")

def _container_stats(names):
    """container name -> (cumulative cpu ns, memory MB without the page cache), one shot over the Docker socket (~20 ms each)."""
    out = {}
    for name in names:
        try:
            c = _Sock("localhost"); c.request("GET", f"/containers/{name}/stats?stream=false&one-shot=true"); d = json.loads(c.getresponse().read()); c.close()
            ms = d.get("memory_stats", {}); mem = ms.get("usage", 0) - ms.get("stats", {}).get("inactive_file", 0)
            out[name] = (d["cpu_stats"]["cpu_usage"]["total_usage"], mem / 2**20)
        except Exception:
            pass
    return out

def _iostat_busy():
    """The machine's busy share (user + sys, percent of all cores) over a one-second interval, from iostat's second line."""
    r = subprocess.run(["iostat", "-w", "1", "-c", "2"], capture_output=True, text=True)
    lines = [l for l in r.stdout.splitlines() if l.strip() and not l.lstrip().startswith(("disk", "KB"))]
    if not lines:
        return None
    f = lines[-1].split()
    return float(f[-6]) + float(f[-5])        # us sy id 1m 5m 15m

HOST_BUCKETS = (("host_vm", ("Virtualization.VirtualMachine",)),            # the virtual machine the containers run in
                ("host_docker", ("com.docker", "Docker.app")),               # Docker Desktop's host-side processes: the port forwarding among them
                ("host_java", ("java",)))                                     # the workers, seen from the host (the per-pid figures are exact)

def _host_cpu():
    """CPU share of one core by host-process bucket, every process ps lists (its decaying average over the last minute; the
    kernel's own time is not a process here, so the machine's busy share from iostat exceeds the sum of the buckets by it)."""
    r = subprocess.run(["ps", "-A", "-o", "%cpu=,comm="], capture_output=True, text=True)
    out = {k: 0.0 for k, _ in HOST_BUCKETS}; out["host_other"] = 0.0
    for l in r.stdout.splitlines():
        cpu, comm = l.split(None, 1) if " " in l.strip() else (l, "")
        for k, needles in HOST_BUCKETS:
            if any(n in comm for n in needles):
                out[k] += float(cpu); break
        else:
            out["host_other"] += float(cpu)
    return {k: round(v, 1) for k, v in out.items()}

class ResourceMonitor(threading.Thread):
    def __init__(self, workers, sample_s=2.0, containers=("kafka", "deadhand-osrm-1")):
        super().__init__(daemon=True)
        self.workers, self.sample_s, self.containers = dict(workers), sample_s, containers
        self.samples, self.cpu_seconds, self._halt = [], {}, threading.Event()
        self.ncpu = os.cpu_count(); self._cont = {}; self.container_ns = {}

    def run(self):
        while not self._halt.is_set():
            t = time.time(); row = {"t": round(t, 1)}
            ps = _ps(self.workers.values())
            for w, pid in self.workers.items():
                if pid in ps:
                    row[f"jvm_{w}_cpu"], row[f"jvm_{w}_mb"], self.cpu_seconds[w] = ps[pid][0], round(ps[pid][1], 1), ps[pid][2]
            me = _ps([os.getpid()]).get(os.getpid())
            if me:
                row["launcher_cpu"] = me[0]
            for name, (ns, mb) in _container_stats(self.containers).items():
                prev = self._cont.get(name)
                if prev:
                    row[f"{name}_cpu"] = round((ns - prev[0]) / max(1e-9, (t - prev[1])) / 1e7, 2)     # share of one core since the last sample
                self._cont[name] = (ns, t); row[f"{name}_mb"] = round(mb, 1)
                first = self.container_ns.setdefault(name, [ns, ns]); first[1] = ns
            row.update(_host_cpu())
            busy = _iostat_busy()
            if busy is not None:
                row["machine_busy_pct"] = busy
            self.samples.append(row)
            self._halt.wait(max(0.0, self.sample_s - (time.time() - t)))

    def stop(self):
        self._halt.set(); self.join(timeout=30)

    def summary(self, wall_s):
        return {"sample_s": self.sample_s, "samples": len(self.samples), "ncpu": self.ncpu, "wall_s": round(wall_s, 1),
                "container_cpu_seconds": {n: round((v[1] - v[0]) / 1e9, 1) for n, v in self.container_ns.items()},
                "jvm_cpu_seconds": {w: round(s, 1) for w, s in sorted(self.cpu_seconds.items())},
                "series": summarize(self.samples, wall_s, self.ncpu), "raw": self.samples}

def summarize(samples, wall_s, ncpu):
    """Per sampled series: mean, median, p95, max, and the core-seconds it stands for over the run (CPU shares of one core,
    or the machine's busy share times its cores)."""
    series = {}
    for k in sorted({k for s in samples for k in s} - {"t"}):
        v = sorted(s[k] for s in samples if k in s)
        if not v:
            continue
        d = {"n": len(v), "mean": round(sum(v) / len(v), 2), "median": v[len(v) // 2], "p95": v[min(len(v) - 1, -(-95 * (len(v) - 1) // 100))], "max": max(v)}
        if k.endswith("_cpu") or k.startswith("host_"):
            d["core_seconds"] = round(sum(v) / len(v) / 100 * wall_s, 1)
        if k == "machine_busy_pct":
            d["core_seconds"] = round(sum(v) / len(v) / 100 * ncpu * wall_s, 1)
        series[k] = d
    return series
