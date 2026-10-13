import { formatAge, formatNumber } from "../format.js";

// The pipeline's stages, left to right, and the datasets each stage produces.
const STAGES = [
  { name: "Ingested", datasets: ["raw_events"] },
  { name: "Validated", datasets: ["validated_events", "quarantine_events"] },
  { name: "Curated", datasets: ["daily_sales", "conversion_funnel", "top_products"] },
];

const LABELS = {
  raw_events: "Raw events",
  validated_events: "Validated events",
  quarantine_events: "Quarantined rows",
  daily_sales: "Daily sales",
  conversion_funnel: "Conversion funnel",
  top_products: "Top products",
};

function DatasetNode({ name, info }) {
  const status = info ? info.status : "missing";
  return (
    <div className={`dataset dataset--${status}`}>
      <span className="dataset__dot" aria-hidden="true" />
      <div>
        <div className="dataset__name">{LABELS[name] || name}</div>
        <div className="dataset__meta">
          {info
            ? `${formatNumber(info.record_count)} ${info.record_count === 1 ? "row" : "rows"}, updated ${formatAge(info.minutes_since_update)}`
            : "Not written yet"}
        </div>
      </div>
      <span className="dataset__status">{status === "missing" ? "no data" : status}</span>
    </div>
  );
}

export default function Lineage({ freshness }) {
  const byName = Object.fromEntries(freshness.map((d) => [d.dataset, d]));
  return (
    <section className="lineage" aria-label="Data freshness by pipeline stage">
      {STAGES.map((stage) => (
        <div className="stage" key={stage.name}>
          <h3 className="stage__name">{stage.name}</h3>
          {stage.datasets.map((name) => (
            <DatasetNode key={name} name={name} info={byName[name]} />
          ))}
        </div>
      ))}
    </section>
  );
}
