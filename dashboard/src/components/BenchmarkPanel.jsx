import { formatDuration, formatNumber } from "../format.js";

export default function BenchmarkPanel({ benchmark }) {
  const longest = benchmark
    ? Math.max(benchmark.baseline_seconds, benchmark.optimized_seconds, 0.001)
    : 1;
  return (
    <section className="panel">
      <div className="panel__head">
        <h2>Runtime benchmark</h2>
        {benchmark && <span className="panel__note">{formatNumber(benchmark.num_events)} events</span>}
      </div>
      {!benchmark ? (
        <p className="empty">
          No benchmark yet. Run <code>python -m pipeline.benchmark --run-date YYYY-MM-DD</code> in the
          scheduler container.
        </p>
      ) : (
        <>
          <p className="big-number">
            {benchmark.improvement_pct.toFixed(1)}%<span> faster after tuning, partitioning and caching</span>
          </p>
          <ul className="bars">
            {[
              ["Before", benchmark.baseline_seconds, "bars__fill bars__fill--muted"],
              ["After", benchmark.optimized_seconds, "bars__fill"],
            ].map(([label, seconds, cls]) => (
              <li key={label}>
                <div className="bars__label">
                  <span>{label}</span>
                  <span className="num">{formatDuration(seconds)}</span>
                </div>
                <div className="bars__track">
                  <div
                    className={cls}
                    style={{ width: `${(seconds / longest) * 100}%` }}
                  />
                </div>
              </li>
            ))}
          </ul>
        </>
      )}
    </section>
  );
}
