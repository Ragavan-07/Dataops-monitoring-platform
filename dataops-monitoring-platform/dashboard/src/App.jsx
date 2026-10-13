import { useCallback, useEffect, useState } from "react";
import { getJSON } from "./api.js";
import { formatNumber } from "./format.js";
import Lineage from "./components/Lineage.jsx";
import JobsTable from "./components/JobsTable.jsx";
import QualityPanel from "./components/QualityPanel.jsx";
import BenchmarkPanel from "./components/BenchmarkPanel.jsx";

const REFRESH_MS = 30_000;

export default function App() {
  const [summary, setSummary] = useState(null);
  const [freshness, setFreshness] = useState([]);
  const [jobs, setJobs] = useState([]);
  const [quality, setQuality] = useState([]);
  const [jobFilter, setJobFilter] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [lastLoaded, setLastLoaded] = useState(null);

  const load = useCallback(async () => {
    try {
      const jobsUrl = jobFilter ? `/api/jobs?status=${jobFilter}&limit=50` : "/api/jobs?limit=50";
      const [s, f, j, q] = await Promise.all([
        getJSON("/api/summary"),
        getJSON("/api/freshness"),
        getJSON(jobsUrl),
        getJSON("/api/quality"),
      ]);
      setSummary(s);
      setFreshness(f);
      setJobs(j);
      setQuality(q);
      setError("");
      setLastLoaded(new Date());
    } catch (err) {
      setError(err.message || "Could not reach the monitoring API.");
    } finally {
      setLoading(false);
    }
  }, [jobFilter]);

  useEffect(() => {
    load();
    const timer = setInterval(load, REFRESH_MS);
    return () => clearInterval(timer);
  }, [load]);

  const jobsDay = summary ? summary.jobs_last_24h : null;
  const stale = summary ? summary.datasets_stale : 0;

  return (
    <div className="page">
      <header className="masthead">
        <div>
          <h1>E-commerce events pipeline</h1>
          <p className="masthead__sub">
            {loading
              ? "Loading pipeline status…"
              : stale > 0
                ? `${stale} of ${summary.datasets_total} datasets are past their freshness SLA`
                : "All datasets are within their freshness SLA"}
          </p>
        </div>
        <div className="masthead__actions">
          {lastLoaded && <span className="muted">Updated {lastLoaded.toLocaleTimeString()}</span>}
          <button type="button" className="button" onClick={load}>
            Refresh now
          </button>
        </div>
      </header>

      {error && (
        <div className="banner" role="alert">
          {error} Check that the api container is running: <code>docker compose ps</code>
        </div>
      )}

      <Lineage freshness={freshness} />

      {jobsDay && (
        <dl className="stats">
          <div>
            <dt>Task runs, last 24 h</dt>
            <dd>{formatNumber(jobsDay.total)}</dd>
          </div>
          <div className={jobsDay.failed > 0 ? "stat--bad" : ""}>
            <dt>Failed</dt>
            <dd>{formatNumber(jobsDay.failed)}</dd>
          </div>
          <div>
            <dt>Running</dt>
            <dd>{formatNumber(jobsDay.running)}</dd>
          </div>
          <div>
            <dt>Rule violations, latest run</dt>
            <dd>{formatNumber(summary.latest_quality ? summary.latest_quality.violations : 0)}</dd>
          </div>
        </dl>
      )}

      <div className="grid">
        <QualityPanel rules={quality} />
        <BenchmarkPanel benchmark={summary ? summary.latest_benchmark : null} />
        <JobsTable jobs={jobs} filter={jobFilter} onFilterChange={setJobFilter} />
      </div>
    </div>
  );
}
