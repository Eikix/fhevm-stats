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

type NetworkOption = {
  id: "mainnet" | "sepolia";
  label: string;
  chainId: number;
};

type PieSegment = {
  label: string;
  count: number;
  color: string;
};

const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:4310";
const NETWORKS: NetworkOption[] = [
  { id: "mainnet", label: "Mainnet", chainId: 1 },
  { id: "sepolia", label: "Sepolia", chainId: 11155111 },
];
const PIE_COLORS = ["#0f766e", "#0b4f4a", "#f59e0b", "#b45309", "#0ea5e9", "#1d4ed8", "#ef4444"];
const PIE_LIMIT = 6;

async function fetchJson<T>(url: string, signal: AbortSignal): Promise<T> {
  const response = await fetch(url, { signal });
  if (!response.ok) {
    throw new Error(`Request failed (${response.status})`);
  }
  return response.json() as Promise<T>;
}

function buildQuery(chainId: number, cacheBust: number): string {
  const params = new URLSearchParams();
  params.set("chainId", chainId.toString());
  params.set("cacheBust", cacheBust.toString());
  return params.toString();
}

function formatNumber(value: number | null | undefined): string {
  if (value === null || value === undefined) return "—";
  return value.toLocaleString();
}

function formatRange(min: number | null | undefined, max: number | null | undefined): string {
  if (min === null || min === undefined || max === null || max === undefined) return "—";
  return `${min.toLocaleString()} → ${max.toLocaleString()}`;
}

function formatPercent(value: number): string {
  if (!Number.isFinite(value)) return "—";
  const rounded = value >= 10 ? Math.round(value) : Number(value.toFixed(1));
  return `${rounded}%`;
}

function App() {
  const [summary, setSummary] = useState<SummaryResponse["summary"] | null>(null);
  const [ops, setOps] = useState<OpsResponse["rows"]>([]);
  const [status, setStatus] = useState<"loading" | "ready" | "error">("loading");
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const [networkId, setNetworkId] = useState<NetworkOption["id"]>("mainnet");

  const activeNetwork = NETWORKS.find((network) => network.id === networkId) ?? NETWORKS[0];
  const chainId = activeNetwork.chainId;

  useEffect(() => {
    const controller = new AbortController();
    const cacheBust = refreshKey;
    const query = buildQuery(chainId, cacheBust);

    const load = async () => {
      setStatus("loading");
      setError(null);
      try {
        const [summaryResponse, opsResponse] = await Promise.all([
          fetchJson<SummaryResponse>(`${API_BASE}/stats/summary?${query}`, controller.signal),
          fetchJson<OpsResponse>(`${API_BASE}/stats/ops?${query}`, controller.signal),
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
  }, [chainId, refreshKey]);

  const topOps = useMemo(() => ops.slice(0, 10), [ops]);
  const maxOps = useMemo(() => {
    return topOps.reduce((max, row) => Math.max(max, row.count), 1);
  }, [topOps]);
  const totalOps = useMemo(() => {
    return ops.reduce((sum, row) => sum + row.count, 0);
  }, [ops]);

  const pieSegments = useMemo(() => {
    if (totalOps <= 0) return [] as PieSegment[];
    const primary = ops.slice(0, PIE_LIMIT);
    const used = primary.reduce((sum, row) => sum + row.count, 0);
    const segments = primary.map((row, index) => ({
      label: row.eventName,
      count: row.count,
      color: PIE_COLORS[index % PIE_COLORS.length],
    }));
    const otherCount = totalOps - used;
    if (otherCount > 0) {
      segments.push({
        label: "Other",
        count: otherCount,
        color: PIE_COLORS[segments.length % PIE_COLORS.length],
      });
    }
    return segments;
  }, [ops, totalOps]);

  const pieGradient = useMemo(() => {
    if (pieSegments.length === 0 || totalOps <= 0) {
      return "conic-gradient(rgba(15, 118, 110, 0.2) 0% 100%)";
    }
    let acc = 0;
    const stops = pieSegments.map((segment) => {
      const start = (acc / totalOps) * 100;
      acc += segment.count;
      const end = (acc / totalOps) * 100;
      return `${segment.color} ${start}% ${end}%`;
    });
    return `conic-gradient(${stops.join(", ")})`;
  }, [pieSegments, totalOps]);

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
              Lightweight telemetry for FHEVMExecutor activity. Raw events first, bucketed rollups
              when it counts.
            </p>

            <div className="mt-6 flex flex-wrap items-center gap-3 text-xs uppercase tracking-[0.2em]">
              <span className={`rounded-full border px-3 py-1 ${statusBadge}`}>
                {status === "error" ? "API error" : status === "loading" ? "Syncing" : "Live"}
              </span>
              <span className="rounded-full border border-black/10 bg-white/70 px-3 py-1 text-black/70">
                Network {activeNetwork.label}
              </span>
              <span className="rounded-full border border-black/10 bg-white/70 px-3 py-1 text-black/70">
                Source {API_BASE.replace(/^https?:\/\//, "")}
              </span>
            </div>

            <div className="mt-6 flex flex-wrap items-center gap-4">
              <div className="flex items-center gap-1 rounded-full border border-black/10 bg-white/70 p-1 text-xs uppercase tracking-[0.18em]">
                {NETWORKS.map((network) => {
                  const isActive = network.id === activeNetwork.id;
                  return (
                    <button
                      key={network.id}
                      type="button"
                      onClick={() => setNetworkId(network.id)}
                      className={`rounded-full px-4 py-2 transition ${
                        isActive
                          ? "bg-black/90 text-white shadow"
                          : "text-black/70 hover:bg-black/5"
                      }`}
                    >
                      {network.label}
                    </button>
                  );
                })}
              </div>

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

          <div className="mt-8 grid gap-8 lg:grid-cols-[320px_1fr]">
            <div className="flex flex-col items-center gap-6">
              <div
                className="relative flex h-56 w-56 items-center justify-center rounded-full border border-black/10"
                style={{ background: pieGradient }}
              >
                <div className="absolute flex h-32 w-32 flex-col items-center justify-center rounded-full border border-black/10 bg-white/85 text-center">
                  <span className="muted-text text-[11px] uppercase tracking-[0.3em]">Total</span>
                  <span className="font-code text-xl">{formatNumber(totalOps)}</span>
                </div>
              </div>

              <div className="w-full space-y-3 text-sm">
                {pieSegments.length === 0 ? (
                  <p className="muted-text text-sm">
                    No events yet. Run backfill or stream to populate the DB.
                  </p>
                ) : (
                  pieSegments.map((segment) => (
                    <div key={segment.label} className="flex items-center justify-between gap-3">
                      <div className="flex items-center gap-3">
                        <span
                          className="h-2.5 w-2.5 rounded-full"
                          style={{ background: segment.color }}
                        />
                        <span className="text-sm font-semibold">{segment.label}</span>
                      </div>
                      <span className="font-code text-xs">
                        {formatPercent((segment.count / totalOps) * 100)}
                      </span>
                    </div>
                  ))
                )}
              </div>
            </div>

            <div className="grid gap-4">
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
                    <span className="font-code text-right text-sm">
                      {row.count.toLocaleString()}
                    </span>
                  </div>
                ))
              )}
            </div>
          </div>
        </section>

        <footer className="muted-text mt-6 text-xs uppercase tracking-[0.2em]">
          Raw events remain the source of truth. Bucketed rollups kick in when performance demands
          it.
        </footer>
      </div>
    </div>
  );
}

export default App;
