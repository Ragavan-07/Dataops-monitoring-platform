import { formatNumber } from "../format.js";

export default function QualityPanel({ rules }) {
  const max = Math.max(1, ...rules.map((r) => r.violation_count));
  const runDate = rules.length ? rules[0].run_date : null;
  return (
    <section className="panel">
      <div className="panel__head">
        <h2>Quality violations</h2>
        {runDate && <span className="panel__note">Run date {runDate}</span>}
      </div>
      {rules.length === 0 ? (
        <p className="empty">No quality checks recorded yet. They appear after validate_events runs.</p>
      ) : (
        <ul className="bars">
          {rules.map((rule) => (
            <li key={rule.rule_name}>
              <div className="bars__label">
                <span>{rule.rule_name.replaceAll("_", " ")}</span>
                <span className="num">
                  {formatNumber(rule.violation_count)}
                  <span className="muted"> ({rule.violation_pct ?? 0}%)</span>
                </span>
              </div>
              <div className="bars__track">
                <div
                  className="bars__fill"
                  style={{ width: `${(rule.violation_count / max) * 100}%` }}
                />
              </div>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
