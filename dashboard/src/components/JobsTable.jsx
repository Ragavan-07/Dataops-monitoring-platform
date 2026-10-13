import { formatDuration, formatTime } from "../format.js";

const FILTERS = [
  { value: "", label: "All runs" },
  { value: "failed", label: "Failed" },
  { value: "running", label: "Running" },
];

export default function JobsTable({ jobs, filter, onFilterChange }) {
  return (
    <section className="panel panel--wide">
      <div className="panel__head">
        <h2>Task runs</h2>
        <div className="segmented" role="group" aria-label="Filter task runs">
          {FILTERS.map((f) => (
            <button
              key={f.label}
              type="button"
              className={filter === f.value ? "is-active" : ""}
              aria-pressed={filter === f.value}
              onClick={() => onFilterChange(f.value)}
            >
              {f.label}
            </button>
          ))}
        </div>
      </div>
      {jobs.length === 0 ? (
        <p className="empty">
          No task runs here yet. Trigger the ecommerce_events_pipeline DAG in Airflow to create some.
        </p>
      ) : (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Task</th>
                <th>Run</th>
                <th>Try</th>
                <th>Status</th>
                <th>Started</th>
                <th className="num">Duration</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => (
                <tr key={job.id}>
                  <td>
                    <strong>{job.task_id}</strong>
                    {job.error_message && <div className="error-text">{job.error_message}</div>}
                  </td>
                  <td className="mono-ish">{job.run_id}</td>
                  <td>{job.try_number}</td>
                  <td>
                    <span className={`pill pill--${job.status}`}>{job.status.replaceAll("_", " ")}</span>
                  </td>
                  <td>{formatTime(job.start_time)}</td>
                  <td className="num">{formatDuration(job.duration_seconds)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
