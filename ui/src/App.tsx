import { useEffect, useMemo, useState } from "react";

type SummaryResponse = {
  summary: {
    count: number;
    minBlock: number | null;
    maxBlock: number | null;
  };
};

type OpsResponse = {
  rows: Array<{ eventName: string; count: number }>;
};

const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:4310";

async function fetchJson<T>(url: string, signal: AbortSignal): Promise<T> {
  const response = await fetch(url, { signal });
  if (!response.ok) {
    throw new Error(`Request failed (${response.status})`);
  }
  return response.json() as Promise<T>;
}

function formatNumber(value: number | null | undefined): string {
  if (value === null || value === undefined) return "—";
  return value.toLocaleString();
}

function formatRange(min: number | null | undefined, max: number | null | undefined): string {
  if (min === null || min === undefined || max === null || max === undefined) return "—";
  return `${min.toLocaleString()} → ${max.toLocaleString()}`;
}

function App() {
  const [summary, setSummary] = useState<SummaryResponse["summary"] | null>(null);
  const [ops, setOps] = useState<OpsResponse["rows"]>([]);
  const [status, setStatus] = useState<"loading" | "ready" | "error">("loading");
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);

  useEffect(() => {
    const controller = new AbortController();
    const cacheBust = refreshKey;

    const load = async () => {
      setStatus("loading");
      setError(null);
      try {
        const [summaryResponse, opsResponse] = await Promise.all([
          fetchJson<SummaryResponse>(
            `${API_BASE}/stats/summary?cacheBust=${cacheBust}`,
            controller.signal,
          ),
          fetchJson<OpsResponse>(`${API_BASE}/stats/ops?cacheBust=${cacheBust}`, controller.signal),
        ]);

        if (controller.signal.aborted) return;

        setSummary(summaryResponse.summary ?? null);
        setOps(opsResponse.rows ?? []);
        setLastUpdated(new Date().toLocaleString());
        setStatus("ready");
      } catch (err) {
        if (controller.signal.aborted) return;
        setError(err instanceof Error ? err.message : "Failed to load data.");
        setStatus("error");
      }
    };

    load();
    return () => controller.abort();
  }, [refreshKey]);

  const topOps = useMemo(() => ops.slice(0, 10), [ops]);
  const maxOps = useMemo(() => {
    return topOps.reduce((max, row) => Math.max(max, row.count), 1);
  }, [topOps]);

  const summaryCards = [
    {
      label: "Total events",
      value: formatNumber(summary?.count),
      detail: "All FHEVMExecutor events ingested.",
    },
    {
      label: "Block range",
      value: formatRange(summary?.minBlock, summary?.maxBlock),
      detail: "Coverage window in the local DB.",
    },
    {
      label: "Ops tracked",
      value: formatNumber(ops.length),
      detail: "Distinct event signatures captured.",
    },
  ];

  const statusBadge =
    status === "error"
      ? "border-red-200 bg-red-50 text-red-700"
      : status === "loading"
        ? "border-amber-200 bg-amber-50 text-amber-700"
        : "border-emerald-200 bg-emerald-50 text-emerald-700";

  return (
    <div className="min-h-screen">
      <div className="mx-auto max-w-6xl px-6 py-12">
        <div className="grid gap-8 lg:grid-cols-[1.1fr_0.9fr]">
          <section className="glass-panel rounded-[28px] p-8 fade-in-up">
            <p className="muted-text text-xs uppercase tracking-[0.4em]">fhevm stats</p>
            <h1 className="font-display mt-4 text-4xl md:text-5xl">Encrypted ops, clear view.</h1>
            <p className="muted-text mt-4 max-w-xl text-base">
              Lightweight telemetry for FHEVMExecutor activity. Raw events, no rollups — just the
              facts.
            </p>

            <div className="mt-6 flex flex-wrap items-center gap-3 text-xs uppercase tracking-[0.2em]">
              <span className={`rounded-full border px-3 py-1 ${statusBadge}`}>
                {status === "error" ? "API error" : status === "loading" ? "Syncing" : "Live"}
              </span>
              <span className="rounded-full border border-black/10 bg-white/70 px-3 py-1 text-black/70">
                Source {API_BASE.replace(/^https?:\/\//, "")}
              </span>
            </div>

            <div className="mt-6 flex flex-wrap items-center gap-4">
              <button
                type="button"
                onClick={() => setRefreshKey((value) => value + 1)}
                disabled={status === "loading"}
                className="glow-ring rounded-full border border-black/10 bg-white/80 px-5 py-2 text-sm uppercase tracking-[0.18em] text-black/80 transition hover:-translate-y-0.5 hover:bg-white disabled:cursor-not-allowed disabled:opacity-60"
              >
                {status === "loading" ? "Refreshing..." : "Refresh data"}
              </button>
              <span className="muted-text text-xs uppercase tracking-[0.18em]">
                {lastUpdated ? `Updated ${lastUpdated}` : "Awaiting first sync"}
              </span>
            </div>

            {error ? (
              <p className="mt-4 text-sm text-red-600">
                {error} — check the API server at {API_BASE}.
              </p>
            ) : null}
          </section>

          <section className="grid gap-4">
            {summaryCards.map((card, index) => (
              <div
                key={card.label}
                className="glass-panel rounded-2xl p-6 fade-in-up"
                style={{ animationDelay: `${120 + index * 80}ms` }}
              >
                <p className="muted-text text-xs uppercase tracking-[0.32em]">{card.label}</p>
                <p className="font-code mt-3 text-2xl md:text-3xl">{card.value}</p>
                <p className="muted-text mt-3 text-sm">{card.detail}</p>
              </div>
            ))}
          </section>
        </div>

        <section className="glass-panel mt-10 rounded-[28px] p-8 fade-in-up">
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div>
              <p className="muted-text text-xs uppercase tracking-[0.3em]">Ops distribution</p>
              <h2 className="font-display mt-3 text-2xl">Top events by volume</h2>
            </div>
            <p className="muted-text text-xs uppercase tracking-[0.2em]">
              Showing {topOps.length || 0} of {ops.length}
            </p>
          </div>

          <div className="mt-8 grid gap-4">
            {topOps.length === 0 ? (
              <p className="muted-text text-sm">
                No events yet. Run backfill or stream to populate the DB.
              </p>
            ) : (
              topOps.map((row, index) => (
                <div
                  key={row.eventName}
                  className="grid items-center gap-4 md:grid-cols-[140px_1fr_90px]"
                  style={{
                    animation: "fadeInUp 700ms ease both",
                    animationDelay: `${index * 60}ms`,
                  }}
                >
                  <span className="text-sm font-semibold">{row.eventName}</span>
                  <div className="h-2 rounded-full bg-black/10">
                    <div
                      className="h-2 rounded-full"
                      style={{
                        width: `${Math.max((row.count / maxOps) * 100, 6)}%`,
                        background: "linear-gradient(90deg, var(--accent), var(--accent-strong))",
                      }}
                    />
                  </div>
                  <span className="font-code text-right text-sm">{row.count.toLocaleString()}</span>
                </div>
              ))
            )}
          </div>
        </section>

        <footer className="muted-text mt-6 text-xs uppercase tracking-[0.2em]">
          Lean mode: raw events only, rollups deferred until queries slow down.
        </footer>
      </div>
    </div>
  );
}

export default App;
