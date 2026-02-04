import dagre from "@dagrejs/dagre";
import {
  type FormEvent,
  type MouseEvent,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

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

type IngestionResponse = {
  chainId: number;
  events: {
    maxBlock: number | null;
    lastEventAt: string | null;
    count: number;
  };
  checkpoint: {
    lastBlock: number | null;
    updatedAt: string | null;
  };
};

type DfgStats = {
  opCounts: Record<string, number>;
  inputKinds: Record<string, Record<string, number>>;
  operandPairs: Record<string, Record<string, number>>;
  typeCounts: Record<string, Record<string, Record<string, number>>>;
};

type DfgTxSummary = {
  txHash: string;
  blockNumber: number;
  nodeCount: number;
  edgeCount: number;
  depth: number;
  signatureHash: string | null;
  stats: DfgStats | null;
};

type DfgTxsResponse = {
  rows: DfgTxSummary[];
  total?: number;
};

type DfgSignatureRow = {
  signatureHash: string;
  txCount: number;
  avgNodes: number;
  avgEdges: number;
};

type DfgSignaturesResponse = {
  rows: DfgSignatureRow[];
  total?: number;
  txTotal?: number;
};

type DfgStatsResponse = {
  chainId: number;
  dfg: {
    total: number;
    avgNodes: number | null;
    avgEdges: number | null;
    avgDepth: number | null;
    minNodes: number | null;
    maxNodes: number | null;
    minEdges: number | null;
    maxEdges: number | null;
    minDepth: number | null;
    maxDepth: number | null;
    signatureCount: number;
  };
  totalTxs: number;
  coverage: number;
  maxDepsBlock: number | null;
  deps: {
    totalTxs: number;
    dependentTxs: number;
    independentTxs: number;
    avgUpstreamTxs: number;
    avgUpstreamHandles: number;
    maxUpstreamTxs: number;
    maxUpstreamHandles: number;
    parallelismRatio: number;
    maxChainDepth: number;
    maxTotalDepth: number;
    chainDepthDistribution: Record<number, number>;
    totalDepthDistribution: Record<number, number>;
    depthMode?: "inter" | "total";
    horizon?: {
      startBlock: number;
      endBlock: number;
      blockCount: number;
    };
  } | null;
};

type DfgRollup = {
  opCounts: Record<string, number>;
  inputKinds: Record<string, Record<string, number>>;
  operandPairs: Record<string, Record<string, number>>;
  typeCounts: Record<string, Record<string, Record<string, number>>>;
};

type DfgRollupResponse = {
  chainId: number;
  dfgTxCount: number;
  updatedAt: string;
  stats: DfgRollup | null;
};

type ViewBox = { x: number; y: number; width: number; height: number };

type DfgInputInfo = {
  role: string;
  kind: string;
  handle?: string;
  type?: number | null;
};

type DfgTypeInfo = {
  inputs?: DfgInputInfo[];
  output?: {
    handle?: string | null;
    kind?: string;
    type?: number | null;
  };
};

type DfgNode = {
  nodeId: number;
  op: string;
  outputHandle: string | null;
  inputCount: number;
  scalarFlag: number | null;
  typeInfo: DfgTypeInfo | null;
};

type DfgEdge = {
  fromNodeId: number;
  toNodeId: number;
  inputHandle: string;
};

type DfgInput = {
  handle: string;
  kind: string;
};

type DfgTxResponse = {
  tx: DfgTxSummary;
  nodes: DfgNode[];
  edges: DfgEdge[];
  inputs: DfgInput[];
  cutEdges?: CutEdge[];
  lookbackBlocks?: number;
};

type OpTypeRow = {
  eventName: string;
  typeValue: string;
  count: number;
};

type OpTypesResponse = {
  rows: OpTypeRow[];
  totals?: Array<{ eventName: string; count: number }>;
};

type WindowStatsResponse = {
  chainId: number;
  lookbackBlocks: number;
  signatureHash?: string;
  stats: {
    totalTxs: number;
    dependentTxs: number;
    independentTxs: number;
    parallelismRatio: number;
    maxTruncatedDepth: number;
    avgTruncatedDepth: number;
    maxCombinedDepth: number;
    avgCombinedDepth: number;
    avgIntraDepth: number;
    truncatedDepthDistribution: Record<number, number>;
    combinedDepthDistribution: Record<
      number,
      { count: number; avgIntra: number }
    >;
  };
  topDepthTxs: Array<{
    txHash: string;
    blockNumber: number;
    truncatedDepth: number;
    intraTxDepth: number;
    combinedDepth: number;
    fullChainDepth: number;
  }>;
};

type CutEdge = {
  handle: string;
  producerTxHash: string;
  producerBlock: number;
  windowStart: number;
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
const PIE_COLORS = [
  "#0f766e",
  "#0b4f4a",
  "#f59e0b",
  "#b45309",
  "#0ea5e9",
  "#1d4ed8",
  "#ef4444",
];
const PIE_LIMIT = 6;
const OP_TYPE_ROLES: Array<{ id: "result" | "lhs" | "rhs"; label: string }> = [
  { id: "result", label: "Result" },
  { id: "lhs", label: "LHS" },
  { id: "rhs", label: "RHS + scalar" },
];
const FHE_TYPE_NAMES = [
  "Bool",
  "Uint4",
  "Uint8",
  "Uint16",
  "Uint32",
  "Uint64",
  "Uint128",
  "Uint160",
  "Uint256",
  "Uint512",
  "Uint1024",
  "Uint2048",
  "Uint2",
  "Uint6",
  "Uint10",
  "Uint12",
  "Uint14",
  "Int2",
  "Int4",
  "Int6",
  "Int8",
  "Int10",
  "Int12",
  "Int14",
  "Int16",
  "Int32",
  "Int64",
  "Int128",
  "Int160",
  "Int256",
  "AsciiString",
  "Int512",
  "Int1024",
  "Int2048",
  "Uint24",
  "Uint40",
  "Uint48",
  "Uint56",
  "Uint72",
  "Uint80",
  "Uint88",
  "Uint96",
  "Uint104",
  "Uint112",
  "Uint120",
  "Uint136",
  "Uint144",
  "Uint152",
  "Uint168",
  "Uint176",
  "Uint184",
  "Uint192",
  "Uint200",
  "Uint208",
  "Uint216",
  "Uint224",
  "Uint232",
  "Uint240",
  "Uint248",
  "Int24",
  "Int40",
  "Int48",
  "Int56",
  "Int72",
  "Int80",
  "Int88",
  "Int96",
  "Int104",
  "Int112",
  "Int120",
  "Int136",
  "Int144",
  "Int152",
  "Int168",
  "Int176",
  "Int184",
  "Int192",
  "Int200",
  "Int208",
  "Int216",
  "Int224",
  "Int232",
  "Int240",
  "Int248",
];

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

function formatRange(
  min: number | null | undefined,
  max: number | null | undefined,
): string {
  if (min === null || min === undefined || max === null || max === undefined)
    return "—";
  return `${min.toLocaleString()} → ${max.toLocaleString()}`;
}

function formatFheType(value: number | null | undefined): string {
  if (value === null || value === undefined) return "—";
  const name = FHE_TYPE_NAMES[value];
  if (!name) return `t${value}`;
  return `e${name.charAt(0).toLowerCase()}${name.slice(1)}`;
}

function resolveEdgeType(
  edge: DfgEdge,
  nodeById: Map<number, DfgNode>,
): number | null {
  const toNode = nodeById.get(edge.toNodeId);
  const inputType = toNode?.typeInfo?.inputs?.find(
    (input) => input.handle === edge.inputHandle,
  )?.type;
  if (inputType !== null && inputType !== undefined) return inputType;
  const fromNode = nodeById.get(edge.fromNodeId);
  return fromNode?.typeInfo?.output?.type ?? null;
}

function formatOpTypeValue(value: string): string {
  if (value === "scalar") return "scalar";
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return value;
  return formatFheType(parsed);
}

function shortenHandle(
  value: string | null | undefined,
  head = 6,
  tail = 4,
): string {
  if (!value) return "—";
  if (value.length <= head + tail + 2) return value;
  return `${value.slice(0, head + 2)}...${value.slice(-tail)}`;
}

function formatPercent(value: number): string {
  if (!Number.isFinite(value)) return "—";
  if (value > 0 && value < 0.1) return "<0.1%";
  const rounded = value >= 10 ? Math.round(value) : Number(value.toFixed(1));
  return `${rounded}%`;
}

function formatDecimal(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || !Number.isFinite(value))
    return "—";
  return Number(value.toFixed(digits)).toLocaleString();
}

function normalizeTimestamp(value: string): string {
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(value)) {
    return `${value.replace(" ", "T")}Z`;
  }
  return value;
}

function formatRelativeTime(value: string | null | undefined): string {
  if (!value) return "unknown";
  const timestamp = new Date(normalizeTimestamp(value)).getTime();
  if (Number.isNaN(timestamp)) return "unknown";
  const deltaSeconds = Math.floor((Date.now() - timestamp) / 1000);
  if (deltaSeconds < 30) return "just now";
  if (deltaSeconds < 90) return "1m ago";
  if (deltaSeconds < 3600) return `${Math.floor(deltaSeconds / 60)}m ago`;
  if (deltaSeconds < 5400) return "1h ago";
  if (deltaSeconds < 86400) return `${Math.floor(deltaSeconds / 3600)}h ago`;
  return `${Math.floor(deltaSeconds / 86400)}d ago`;
}

function App() {
  const [summary, setSummary] = useState<SummaryResponse["summary"] | null>(
    null,
  );
  const [ops, setOps] = useState<OpsResponse["rows"]>([]);
  const [status, setStatus] = useState<"loading" | "ready" | "error">(
    "loading",
  );
  const [error, setError] = useState<string | null>(null);
  const [opsStatus, setOpsStatus] = useState<
    "idle" | "loading" | "ready" | "error"
  >("idle");
  const [opsError, setOpsError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const [ingestion, setIngestion] = useState<IngestionResponse | null>(null);
  const [ingestionStatus, setIngestionStatus] = useState<
    "loading" | "ready" | "error"
  >("loading");
  const [ingestionError, setIngestionError] = useState<string | null>(null);
  const [networkId, setNetworkId] = useState<NetworkOption["id"]>("mainnet");
  const [dfgTxs, setDfgTxs] = useState<DfgTxSummary[]>([]);
  const [dfgTotal, setDfgTotal] = useState(0);
  const [dfgStatus, setDfgStatus] = useState<
    "idle" | "loading" | "ready" | "error"
  >("idle");
  const [dfgSelection, setDfgSelection] = useState<string | null>(null);
  const dfgSelectionRef = useRef<string | null>(null);
  useEffect(() => {
    dfgSelectionRef.current = dfgSelection;
  }, [dfgSelection]);
  const [dfgQuery, setDfgQuery] = useState("");
  const [dfgDetail, setDfgDetail] = useState<DfgTxResponse | null>(null);
  const [dfgDetailStatus, setDfgDetailStatus] = useState<
    "idle" | "loading" | "ready" | "error"
  >("idle");
  const [dfgDetailError, setDfgDetailError] = useState<string | null>(null);
  const [dfgSignatureSelection, setDfgSignatureSelection] = useState<
    string | null
  >(null);
  const [dfgSignatures, setDfgSignatures] = useState<DfgSignatureRow[]>([]);
  const [dfgSignatureTotal, setDfgSignatureTotal] = useState(0);
  const [dfgSignatureTxTotal, setDfgSignatureTxTotal] = useState(0);
  const [dfgSignatureStatus, setDfgSignatureStatus] = useState<
    "idle" | "loading" | "ready" | "error"
  >("idle");
  const [dfgSignatureError, setDfgSignatureError] = useState<string | null>(
    null,
  );
  const [dfgSignatureMinNodes, setDfgSignatureMinNodes] = useState(2);
  const [dfgSignatureMinEdges, setDfgSignatureMinEdges] = useState(1);
  const [dfgCaller, setDfgCaller] = useState(() => {
    try {
      return localStorage.getItem("dfgCaller") ?? "";
    } catch {
      return "";
    }
  });
  const defaultCallerAppliedRef = useRef(false);
  const dfgResetRef = useRef<{ chainId: number; caller: string } | null>(null);
  const [dfgRangeMode, setDfgRangeMode] = useState<"window" | "range">(() => {
    try {
      const value = localStorage.getItem("dfgRangeMode");
      return value === "range" ? "range" : "window";
    } catch {
      return "window";
    }
  });
  const [dfgStartBlock, setDfgStartBlock] = useState(() => {
    try {
      return localStorage.getItem("dfgStartBlock") ?? "";
    } catch {
      return "";
    }
  });
  const [dfgEndBlock, setDfgEndBlock] = useState(() => {
    try {
      return localStorage.getItem("dfgEndBlock") ?? "";
    } catch {
      return "";
    }
  });
  const defaultRangeAppliedRef = useRef(false);
  const [dfgStats, setDfgStats] = useState<DfgStatsResponse | null>(null);
  const [dfgStatsStatus, setDfgStatsStatus] = useState<
    "idle" | "loading" | "ready" | "error"
  >("idle");
  const [dfgStatsError, setDfgStatsError] = useState<string | null>(null);

  // Rolling window settings (V5: truncated depth visualization)
  const [windowLookback, setWindowLookback] = useState(50);
  const [windowStats, setWindowStats] = useState<WindowStatsResponse | null>(
    null,
  );
  const [windowStatus, setWindowStatus] = useState<
    "idle" | "loading" | "ready" | "error"
  >("idle");
  const [windowError, setWindowError] = useState<string | null>(null);
  const [dfgLookback, setDfgLookback] = useState<number | null>(null);

  const [dfgRollup, setDfgRollup] = useState<DfgRollupResponse | null>(null);
  const [dfgRollupStatus, setDfgRollupStatus] = useState<
    "idle" | "loading" | "ready" | "error"
  >("idle");
  const [dfgRollupError, setDfgRollupError] = useState<string | null>(null);
  const [dfgGraphView] = useState(true);
  const [dfgViewBox, setDfgViewBox] = useState<ViewBox | null>(null);
  const [dfgDragging, setDfgDragging] = useState(false);
  const dfgViewerRef = useRef<HTMLDivElement | null>(null);
  const dfgDragRef = useRef<{
    startX: number;
    startY: number;
    viewBox: ViewBox;
  } | null>(null);
  const dfgSvgRef = useRef<SVGSVGElement | null>(null);
  const [opTypeRole, setOpTypeRole] = useState<"result" | "lhs" | "rhs">(
    "result",
  );
  const [opTypes, setOpTypes] = useState<OpTypeRow[]>([]);
  const [opTypeStatus, setOpTypeStatus] = useState<
    "idle" | "loading" | "ready" | "error"
  >("idle");
  const [opTypeError, setOpTypeError] = useState<string | null>(null);
  const [opTypeFilter, setOpTypeFilter] = useState("");
  const [opTypeTotals, setOpTypeTotals] = useState<Record<string, number>>({});

  const activeNetwork =
    NETWORKS.find((network) => network.id === networkId) ?? NETWORKS[0];
  const chainId = activeNetwork.chainId;

  useEffect(() => {
    try {
      localStorage.setItem("dfgCaller", dfgCaller);
      localStorage.setItem("dfgRangeMode", dfgRangeMode);
      localStorage.setItem("dfgStartBlock", dfgStartBlock);
      localStorage.setItem("dfgEndBlock", dfgEndBlock);
    } catch {
      // ignore
    }
  }, [dfgCaller, dfgRangeMode, dfgStartBlock, dfgEndBlock]);

  useEffect(() => {
    if (chainId !== 11155111) return;
    if (defaultCallerAppliedRef.current) return;
    defaultCallerAppliedRef.current = true;
    try {
      const stored = localStorage.getItem("dfgCaller");
      if (!stored) {
        setDfgCaller("0x9fdd4b67c241779dca4d2eaf3d5946fb699f5d7a");
      }
    } catch {
      // ignore
    }
  }, [chainId]);

  useEffect(() => {
    if (chainId !== 11155111) return;
    if (
      dfgCaller.trim().toLowerCase() !==
      "0x9fdd4b67c241779dca4d2eaf3d5946fb699f5d7a"
    )
      return;
    if (defaultRangeAppliedRef.current) return;
    if (summary?.maxBlock == null) return;
    // Avoid applying a Sepolia preset based on stale Mainnet summary data during the network switch.
    if (summary.maxBlock > 20_000_000) return;
    defaultRangeAppliedRef.current = true;

    // Roughly 17 hours ≈ ~5100 blocks (12s). Give a little buffer.
    const end = summary.maxBlock;
    const start = Math.max(0, end - 5200);
    setDfgRangeMode("range");
    setDfgStartBlock(String(start));
    setDfgEndBlock(String(end));
  }, [chainId, dfgCaller, summary?.maxBlock]);

  useEffect(() => {
    const prev = dfgResetRef.current;
    if (prev && prev.chainId === chainId && prev.caller === dfgCaller) return;
    dfgResetRef.current = { chainId, caller: dfgCaller };

    setDfgSignatureSelection(null);
    setDfgSelection(null);
    setDfgQuery("");
  }, [chainId, dfgCaller]);

  useEffect(() => {
    const controller = new AbortController();
    const cacheBust = refreshKey;
    const query = buildQuery(chainId, cacheBust);

    const loadSummary = async () => {
      setStatus("loading");
      setError(null);
      try {
        const summaryResponse = await fetchJson<SummaryResponse>(
          `${API_BASE}/stats/summary?${query}`,
          controller.signal,
        );

        if (controller.signal.aborted) return;
        setSummary(summaryResponse.summary ?? null);
        setLastUpdated(new Date().toLocaleString());
        setStatus("ready");
      } catch (err) {
        if (controller.signal.aborted) return;
        setError(err instanceof Error ? err.message : "Failed to load data.");
        setStatus("error");
      }
    };

    const loadOps = async () => {
      setOpsStatus("loading");
      setOpsError(null);
      try {
        const opsResponse = await fetchJson<OpsResponse>(
          `${API_BASE}/stats/ops?${query}`,
          controller.signal,
        );
        if (controller.signal.aborted) return;
        setOps(opsResponse.rows ?? []);
        setOpsStatus("ready");
      } catch (err) {
        if (controller.signal.aborted) return;
        setOpsError(err instanceof Error ? err.message : "Failed to load ops.");
        setOpsStatus("error");
      }
    };

    loadSummary();
    loadOps();
    return () => controller.abort();
  }, [chainId, refreshKey]);

  useEffect(() => {
    const controller = new AbortController();
    const params = new URLSearchParams();
    params.set("chainId", chainId.toString());
    params.set("cacheBust", refreshKey.toString());

    setIngestionStatus("loading");
    setIngestionError(null);

    fetchJson<IngestionResponse>(
      `${API_BASE}/stats/ingestion?${params.toString()}`,
      controller.signal,
    )
      .then((data) => {
        setIngestion(data ?? null);
        setIngestionStatus("ready");
      })
      .catch((err) => {
        if (err instanceof Error && err.name === "AbortError") return;
        setIngestionError(
          err instanceof Error
            ? err.message
            : "Failed to load ingestion status.",
        );
        setIngestionStatus("error");
      });

    return () => controller.abort();
  }, [chainId, refreshKey]);

  useEffect(() => {
    const controller = new AbortController();
    const cacheBust = refreshKey;
    const params = new URLSearchParams();
    params.set("chainId", chainId.toString());
    params.set("role", opTypeRole);
    if (opTypeRole === "rhs") {
      params.set("includeScalar", "1");
    }
    params.set("cacheBust", cacheBust.toString());

    const load = async () => {
      setOpTypeStatus("loading");
      setOpTypeError(null);
      try {
        const response = await fetchJson<OpTypesResponse>(
          `${API_BASE}/stats/op-types?${params.toString()}`,
          controller.signal,
        );

        if (controller.signal.aborted) return;
        setOpTypes(response.rows ?? []);
        const totals = response.totals ?? [];
        const totalMap: Record<string, number> = {};
        for (const row of totals) {
          totalMap[row.eventName] = row.count;
        }
        setOpTypeTotals(totalMap);
        setOpTypeStatus("ready");
      } catch (err) {
        if (controller.signal.aborted) return;
        setOpTypeError(
          err instanceof Error ? err.message : "Failed to load op types.",
        );
        setOpTypeStatus("error");
      }
    };

    load();
    return () => controller.abort();
  }, [chainId, refreshKey, opTypeRole]);

  useEffect(() => {
    const controller = new AbortController();
    const cacheBust = refreshKey;
    const params = new URLSearchParams();
    params.set("chainId", chainId.toString());
    params.set("limit", "16");
    if (dfgSignatureSelection) {
      params.set("signatureHash", dfgSignatureSelection);
    }
    if (dfgCaller.trim()) {
      params.set("caller", dfgCaller.trim().toLowerCase());
    }
    params.set("cacheBust", cacheBust.toString());

    if (dfgRangeMode === "window") {
      if (summary?.maxBlock != null) {
        const endBlock = summary.maxBlock;
        const startBlock = Math.max(0, endBlock - windowLookback + 1);
        params.set("startBlock", startBlock.toString());
        params.set("endBlock", endBlock.toString());
      }
    } else {
      const start = Number(dfgStartBlock);
      const end = Number(dfgEndBlock);
      if (Number.isFinite(start) && start >= 0) {
        params.set("startBlock", String(Math.floor(start)));
      }
      if (Number.isFinite(end) && end >= 0) {
        params.set("endBlock", String(Math.floor(end)));
      }
    }

    const load = async () => {
      setDfgStatus("loading");
      try {
        const response = await fetchJson<DfgTxsResponse>(
          `${API_BASE}/dfg/txs?${params.toString()}`,
          controller.signal,
        );

        if (controller.signal.aborted) return;
        const rows = response.rows ?? [];
        setDfgTxs(rows);
        setDfgTotal(response.total ?? rows.length);
        setDfgStatus("ready");
        if (rows.length > 0) {
          const current = dfgSelectionRef.current;
          const exists = current
            ? rows.some((row) => row.txHash === current)
            : false;
          if (!exists) {
            setDfgSelection(rows[0].txHash);
            setDfgQuery(rows[0].txHash);
          }
        }
      } catch {
        if (controller.signal.aborted) return;
        setDfgStatus("error");
      }
    };

    load();
    return () => controller.abort();
  }, [
    chainId,
    refreshKey,
    dfgSignatureSelection,
    dfgCaller,
    dfgRangeMode,
    dfgStartBlock,
    dfgEndBlock,
    windowLookback,
    summary?.maxBlock,
  ]);

  useEffect(() => {
    const controller = new AbortController();
    const cacheBust = refreshKey;
    const params = new URLSearchParams();
    params.set("chainId", chainId.toString());
    params.set("limit", "10");
    params.set("minNodes", dfgSignatureMinNodes.toString());
    params.set("minEdges", dfgSignatureMinEdges.toString());
    if (dfgCaller.trim()) {
      params.set("caller", dfgCaller.trim().toLowerCase());
    }
    params.set("cacheBust", cacheBust.toString());

    if (dfgRangeMode === "window") {
      // Apply window-based block range filtering if we have block info
      if (summary?.maxBlock != null) {
        const endBlock = summary.maxBlock;
        const startBlock = Math.max(0, endBlock - windowLookback + 1);
        params.set("startBlock", startBlock.toString());
        params.set("endBlock", endBlock.toString());
      }
    } else {
      const start = Number(dfgStartBlock);
      const end = Number(dfgEndBlock);
      if (Number.isFinite(start) && start >= 0) {
        params.set("startBlock", String(Math.floor(start)));
      }
      if (Number.isFinite(end) && end >= 0) {
        params.set("endBlock", String(Math.floor(end)));
      }
    }

    const load = async () => {
      setDfgSignatureStatus("loading");
      setDfgSignatureError(null);
      try {
        const response = await fetchJson<DfgSignaturesResponse>(
          `${API_BASE}/dfg/signatures?${params.toString()}`,
          controller.signal,
        );

        if (controller.signal.aborted) return;
        setDfgSignatures(response.rows ?? []);
        setDfgSignatureTotal(response.total ?? 0);
        setDfgSignatureTxTotal(response.txTotal ?? 0);
        setDfgSignatureStatus("ready");
      } catch (err) {
        if (controller.signal.aborted) return;
        setDfgSignatureError(
          err instanceof Error ? err.message : "Failed to load signatures.",
        );
        setDfgSignatureStatus("error");
      }
    };

    load();
    return () => controller.abort();
  }, [
    chainId,
    refreshKey,
    dfgSignatureMinNodes,
    dfgSignatureMinEdges,
    dfgCaller,
    dfgRangeMode,
    dfgStartBlock,
    dfgEndBlock,
    windowLookback,
    summary?.maxBlock,
  ]);

  useEffect(() => {
    const controller = new AbortController();
    const cacheBust = refreshKey;
    const params = new URLSearchParams();
    params.set("chainId", chainId.toString());
    params.set("cacheBust", cacheBust.toString());

    const load = async () => {
      setDfgStatsStatus("loading");
      setDfgStatsError(null);
      try {
        const response = await fetchJson<DfgStatsResponse>(
          `${API_BASE}/dfg/stats?${params.toString()}`,
          controller.signal,
        );

        if (controller.signal.aborted) return;
        setDfgStats(response);
        setDfgStatsStatus("ready");
      } catch (err) {
        if (controller.signal.aborted) return;
        setDfgStatsError(
          err instanceof Error ? err.message : "Failed to load DFG stats.",
        );
        setDfgStatsStatus("error");
      }
    };

    load();
    return () => controller.abort();
  }, [chainId, refreshKey]);

  // Load rolling window stats when lookback/chain changes (after DFG stats ready)
  useEffect(() => {
    if (dfgStatsStatus !== "ready") return;

    const controller = new AbortController();
    const params = new URLSearchParams();
    params.set("chainId", chainId.toString());
    params.set("lookbackBlocks", windowLookback.toString());
    params.set("topLimit", "10");
    params.set("cacheBust", refreshKey.toString());

    setWindowStatus("loading");
    setWindowError(null);
    fetchJson<WindowStatsResponse>(
      `${API_BASE}/dfg/stats/window?${params.toString()}`,
      controller.signal,
    )
      .then((response) => {
        setWindowStats(response);
        setWindowStatus("ready");
      })
      .catch((err) => {
        if (err instanceof Error && err.name === "AbortError") return;
        setWindowError(
          err instanceof Error ? err.message : "Failed to load window stats.",
        );
        setWindowStatus("error");
      });

    return () => controller.abort();
  }, [windowLookback, dfgStatsStatus, chainId, refreshKey]);

  useEffect(() => {
    const controller = new AbortController();
    const cacheBust = refreshKey;
    const params = new URLSearchParams();
    params.set("chainId", chainId.toString());
    params.set("cacheBust", cacheBust.toString());

    const load = async () => {
      setDfgRollupStatus("loading");
      setDfgRollupError(null);
      try {
        const response = await fetchJson<DfgRollupResponse>(
          `${API_BASE}/dfg/rollup?${params.toString()}`,
          controller.signal,
        );

        if (controller.signal.aborted) return;
        setDfgRollup(response);
        setDfgRollupStatus("ready");
      } catch (err) {
        if (controller.signal.aborted) return;
        setDfgRollupError(
          err instanceof Error ? err.message : "Failed to load DFG rollup.",
        );
        setDfgRollupStatus("error");
      }
    };

    load();
    return () => controller.abort();
  }, [chainId, refreshKey]);

  useEffect(() => {
    if (!dfgSelection) {
      setDfgDetail(null);
      return;
    }
    const controller = new AbortController();
    const cacheBust = refreshKey;
    const params = new URLSearchParams();
    params.set("chainId", chainId.toString());
    params.set("txHash", dfgSelection);
    params.set("cacheBust", cacheBust.toString());
    if (dfgLookback !== null) {
      params.set("lookbackBlocks", dfgLookback.toString());
    }

    const load = async () => {
      setDfgDetailStatus("loading");
      setDfgDetailError(null);
      try {
        const response = await fetchJson<DfgTxResponse>(
          `${API_BASE}/dfg/tx?${params.toString()}`,
          controller.signal,
        );

        if (controller.signal.aborted) return;
        setDfgDetail(response);
        setDfgDetailStatus("ready");
      } catch (err) {
        if (controller.signal.aborted) return;
        setDfgDetailError(
          err instanceof Error ? err.message : "Failed to load DFG details.",
        );
        setDfgDetailStatus("error");
      }
    };

    load();
    return () => controller.abort();
  }, [chainId, refreshKey, dfgSelection, dfgLookback]);

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

  const opTypeRows = useMemo(() => {
    const rows = [...opTypes];
    rows.sort((a, b) => {
      if (a.eventName === b.eventName) return b.count - a.count;
      return a.eventName.localeCompare(b.eventName);
    });
    const needle = opTypeFilter.trim().toLowerCase();
    if (!needle) return rows;
    return rows.filter((row) => row.eventName.toLowerCase().includes(needle));
  }, [opTypes, opTypeFilter]);

  const dfgSelected = dfgDetail?.tx ?? null;
  const dfgNodes = useMemo(() => dfgDetail?.nodes ?? [], [dfgDetail]);
  const dfgEdges = useMemo(() => dfgDetail?.edges ?? [], [dfgDetail]);
  const dfgInputs = useMemo(() => dfgDetail?.inputs ?? [], [dfgDetail]);
  const dfgTxStats = dfgSelected?.stats ?? null;
  const dfgEdgeTypes = useMemo(() => {
    if (dfgEdges.length === 0 || dfgNodes.length === 0) return [];
    const nodeById = new Map<number, DfgNode>();
    for (const node of dfgNodes) nodeById.set(node.nodeId, node);
    return dfgEdges.map((edge) => {
      const type = resolveEdgeType(edge, nodeById);
      return type === null || type === undefined ? null : formatFheType(type);
    });
  }, [dfgEdges, dfgNodes]);

  const dfgOpRows = useMemo(() => {
    if (!dfgTxStats?.opCounts) return [];
    return Object.entries(dfgTxStats.opCounts)
      .map(([op, count]) => ({ op, count }))
      .sort((a, b) => b.count - a.count);
  }, [dfgTxStats]);

  const dfgInputRows = useMemo(() => {
    if (!dfgTxStats?.inputKinds) return [];
    return Object.entries(dfgTxStats.inputKinds)
      .map(([op, counts]) => {
        const ciphertext = counts.ciphertext ?? 0;
        const trivial = counts.trivial ?? 0;
        const external = counts.external ?? 0;
        const scalar = counts.scalar ?? 0;
        return {
          op,
          ciphertext,
          trivial,
          external,
          scalar,
          total: ciphertext + trivial + external + scalar,
        };
      })
      .sort((a, b) => b.total - a.total);
  }, [dfgTxStats]);

  const dfgRollupStats = dfgRollup?.stats ?? null;
  const dfgRollupOpRows = useMemo(() => {
    if (!dfgRollupStats?.opCounts) return [];
    return Object.entries(dfgRollupStats.opCounts)
      .map(([op, count]) => ({ op, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 8);
  }, [dfgRollupStats]);

  const dfgRollupTotalNodes = useMemo(() => {
    if (!dfgRollupStats?.opCounts) return 0;
    return Object.values(dfgRollupStats.opCounts).reduce(
      (sum, value) => sum + value,
      0,
    );
  }, [dfgRollupStats]);

  const dfgRollupInputTotals = useMemo(() => {
    const totals = { ciphertext: 0, trivial: 0, external: 0, scalar: 0 };
    if (!dfgRollupStats?.inputKinds) return totals;
    for (const counts of Object.values(dfgRollupStats.inputKinds)) {
      totals.ciphertext += counts.ciphertext ?? 0;
      totals.trivial += counts.trivial ?? 0;
      totals.external += counts.external ?? 0;
      totals.scalar += counts.scalar ?? 0;
    }
    return totals;
  }, [dfgRollupStats]);

  const dfgRollupOperandPairs = useMemo(() => {
    const totals: Record<string, number> = {};
    if (!dfgRollupStats?.operandPairs) return [];
    for (const counts of Object.values(dfgRollupStats.operandPairs)) {
      for (const [pair, count] of Object.entries(counts)) {
        totals[pair] = (totals[pair] ?? 0) + count;
      }
    }
    return Object.entries(totals)
      .map(([pair, count]) => ({ pair, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 6);
  }, [dfgRollupStats]);

  const dfgGraph = useMemo(() => {
    if (!dfgGraphView || dfgNodes.length === 0) return null;
    const graph = new dagre.graphlib.Graph({ multigraph: true });
    graph.setGraph({
      rankdir: "LR",
      nodesep: 28,
      ranksep: 80,
      marginx: 20,
      marginy: 20,
    });
    graph.setDefaultEdgeLabel(() => ({}));

    const nodeWidth = 140;
    const nodeHeight = 44;
    const nodeById = new Map<number, DfgNode>();
    for (const node of dfgNodes) nodeById.set(node.nodeId, node);
    const edgeLabels = new Map<string, string | null>();

    for (const node of dfgNodes) {
      graph.setNode(String(node.nodeId), {
        width: nodeWidth,
        height: nodeHeight,
        label: node.op,
      });
    }
    dfgEdges.forEach((edge, index) => {
      const type = resolveEdgeType(edge, nodeById);
      edgeLabels.set(
        `e${index}`,
        type === null || type === undefined ? null : formatFheType(type),
      );
      graph.setEdge(
        {
          v: String(edge.fromNodeId),
          w: String(edge.toNodeId),
          name: `e${index}`,
        },
        {},
      );
    });

    dagre.layout(graph);

    const nodes = graph.nodes().map((id) => {
      const data = graph.node(id);
      return {
        id,
        x: data.x,
        y: data.y,
        width: data.width,
        height: data.height,
        label: dfgNodes.find((node) => String(node.nodeId) === id)?.op ?? id,
      };
    });

    const edges = graph.edges().map((edge) => {
      const data = graph.edge(edge);
      const points = data?.points ?? [];
      const path = points
        .map((point, idx) => `${idx === 0 ? "M" : "L"}${point.x},${point.y}`)
        .join(" ");
      const mid = points[Math.floor(points.length / 2)];
      return {
        id: `${edge.v}-${edge.w}-${edge.name ?? ""}`,
        path,
        label: edgeLabels.get(edge.name ?? "") ?? null,
        labelX: mid?.x ?? 0,
        labelY: mid?.y ?? 0,
      };
    });

    const bounds = nodes.reduce(
      (acc, node) => {
        acc.minX = Math.min(acc.minX, node.x - node.width / 2);
        acc.maxX = Math.max(acc.maxX, node.x + node.width / 2);
        acc.minY = Math.min(acc.minY, node.y - node.height / 2);
        acc.maxY = Math.max(acc.maxY, node.y + node.height / 2);
        return acc;
      },
      { minX: Infinity, maxX: -Infinity, minY: Infinity, maxY: -Infinity },
    );

    if (!Number.isFinite(bounds.minX)) return null;

    const padding = 30;
    const width = bounds.maxX - bounds.minX + padding * 2;
    const height = bounds.maxY - bounds.minY + padding * 2;
    const viewBox: ViewBox = {
      x: bounds.minX - padding,
      y: bounds.minY - padding,
      width,
      height,
    };

    return { nodes, edges, viewBox };
  }, [dfgGraphView, dfgNodes, dfgEdges]);

  useEffect(() => {
    if (!dfgGraph) {
      setDfgViewBox(null);
      return;
    }
    setDfgViewBox(dfgGraph.viewBox);
  }, [dfgGraph]);

  // Store current values in refs for the native wheel event listener
  const dfgGraphRef = useRef(dfgGraph);
  const dfgViewBoxRef = useRef(dfgViewBox);
  useEffect(() => {
    dfgGraphRef.current = dfgGraph;
  }, [dfgGraph]);
  useEffect(() => {
    dfgViewBoxRef.current = dfgViewBox;
  }, [dfgViewBox]);

  // Attach non-passive wheel listener to prevent page scroll while zooming
  useEffect(() => {
    if (!dfgGraph) return;
    const svg = dfgSvgRef.current;
    if (!svg) return;

    const handleWheel = (event: WheelEvent) => {
      const graph = dfgGraphRef.current;
      const viewBox = dfgViewBoxRef.current;
      if (!graph || !viewBox) return;
      event.preventDefault();
      const rect = svg.getBoundingClientRect();
      const pointX =
        viewBox.x + ((event.clientX - rect.left) / rect.width) * viewBox.width;
      const pointY =
        viewBox.y + ((event.clientY - rect.top) / rect.height) * viewBox.height;
      const scale = event.deltaY > 0 ? 1.12 : 0.9;

      const base = graph.viewBox;
      const nextWidth = viewBox.width * scale;
      const nextHeight = viewBox.height * scale;
      const minWidth = base.width * 0.15;
      const minHeight = base.height * 0.15;
      const maxWidth = base.width * 5;
      const maxHeight = base.height * 5;
      const clampedWidth = Math.min(Math.max(nextWidth, minWidth), maxWidth);
      const clampedHeight = Math.min(
        Math.max(nextHeight, minHeight),
        maxHeight,
      );
      const ratioX = clampedWidth / viewBox.width;
      const ratioY = clampedHeight / viewBox.height;

      const nextX = pointX - (pointX - viewBox.x) * ratioX;
      const nextY = pointY - (pointY - viewBox.y) * ratioY;
      setDfgViewBox({
        x: nextX,
        y: nextY,
        width: clampedWidth,
        height: clampedHeight,
      });
    };

    svg.addEventListener("wheel", handleWheel, { passive: false });
    return () => svg.removeEventListener("wheel", handleWheel);
  }, [dfgGraph]);

  const handleGraphMouseDown = (event: MouseEvent<SVGSVGElement>) => {
    if (!dfgViewBox) return;
    if (event.button !== 0) return;
    event.preventDefault();
    dfgDragRef.current = {
      startX: event.clientX,
      startY: event.clientY,
      viewBox: { ...dfgViewBox },
    };
    setDfgDragging(true);
  };

  const handleGraphMouseMove = (event: MouseEvent<SVGSVGElement>) => {
    if (!dfgDragRef.current) return;
    const rect = event.currentTarget.getBoundingClientRect();
    const dx = event.clientX - dfgDragRef.current.startX;
    const dy = event.clientY - dfgDragRef.current.startY;
    const moveX = (dx / rect.width) * dfgDragRef.current.viewBox.width;
    const moveY = (dy / rect.height) * dfgDragRef.current.viewBox.height;
    setDfgViewBox({
      x: dfgDragRef.current.viewBox.x - moveX,
      y: dfgDragRef.current.viewBox.y - moveY,
      width: dfgDragRef.current.viewBox.width,
      height: dfgDragRef.current.viewBox.height,
    });
  };

  const handleGraphMouseUp = () => {
    dfgDragRef.current = null;
    setDfgDragging(false);
  };

  const resetGraphView = () => {
    if (!dfgGraph) return;
    setDfgViewBox(dfgGraph.viewBox);
  };

  const statusBadge =
    status === "error"
      ? "border-red-200 bg-red-50 text-red-700"
      : status === "loading"
        ? "border-amber-200 bg-amber-50 text-amber-700"
        : "border-emerald-200 bg-emerald-50 text-emerald-700";

  const ingestionAge = useMemo(() => {
    if (!ingestion?.events.lastEventAt || ingestionStatus !== "ready")
      return null;
    return (
      Date.now() -
      new Date(normalizeTimestamp(ingestion.events.lastEventAt)).getTime()
    );
  }, [ingestion?.events.lastEventAt, ingestionStatus]);
  const ingestionStale =
    ingestionAge === null
      ? true
      : ingestionAge > 5 * 60 * 1000 || (ingestion?.events.count ?? 0) === 0;
  const ingestionBadge =
    ingestionStatus === "error"
      ? "border-red-200 bg-red-50 text-red-700"
      : ingestionStatus === "loading"
        ? "border-amber-200 bg-amber-50 text-amber-700"
        : ingestionStale
          ? "border-amber-200 bg-amber-50 text-amber-700"
          : "border-emerald-200 bg-emerald-50 text-emerald-700";

  const handleDfgSearch = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const next = dfgQuery.trim();
    if (!next) return;
    setDfgSelection(next);
  };

  return (
    <div className="min-h-screen">
      <div className="mx-auto max-w-6xl px-6 py-12">
        <div className="grid gap-8 lg:grid-cols-[1.1fr_0.9fr]">
          <section className="glass-panel rounded-[28px] p-8 fade-in-up">
            <p className="muted-text text-xs uppercase tracking-[0.4em]">
              fhevm stats
            </p>
            <h1 className="font-display mt-4 text-4xl md:text-5xl">
              Encrypted ops, clear view.
            </h1>
            <p className="muted-text mt-4 max-w-xl text-base">
              Lightweight telemetry for FHEVMExecutor activity. Raw events
              first, bucketed rollups when it counts.
            </p>

            <div className="mt-6 flex flex-wrap items-center gap-3 text-xs uppercase tracking-[0.2em]">
              <span className={`rounded-full border px-3 py-1 ${statusBadge}`}>
                {status === "error"
                  ? "API error"
                  : status === "loading"
                    ? "Syncing"
                    : "Live"}
              </span>
              <span
                className={`rounded-full border px-3 py-1 ${ingestionBadge}`}
              >
                {ingestionStatus === "error"
                  ? "Ingestion error"
                  : ingestionStatus === "loading"
                    ? "Ingestion…"
                    : ingestionStale
                      ? "Ingestion stale"
                      : "Ingestion live"}
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
              {ingestionStatus === "ready" ? (
                <span className="muted-text text-xs uppercase tracking-[0.18em]">
                  Last event {formatRelativeTime(ingestion?.events.lastEventAt)}{" "}
                  · block {formatNumber(ingestion?.events.maxBlock)}
                </span>
              ) : ingestionStatus === "error" ? (
                <span className="muted-text text-xs uppercase tracking-[0.18em]">
                  {ingestionError ?? "Ingestion status unavailable"}
                </span>
              ) : null}
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
                <p className="muted-text text-xs uppercase tracking-[0.32em]">
                  {card.label}
                </p>
                <p className="font-code mt-3 text-2xl md:text-3xl">
                  {card.value}
                </p>
                <p className="muted-text mt-3 text-sm">{card.detail}</p>
              </div>
            ))}
          </section>
        </div>

        <section className="glass-panel mt-10 rounded-[28px] p-8 fade-in-up">
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div>
              <p className="muted-text group relative inline-flex items-center gap-1 text-xs uppercase tracking-[0.3em]">
                Ops distribution
                <span className="cursor-help text-black/30 hover:text-black/50">
                  ⓘ
                </span>
                <span className="pointer-events-none absolute left-0 top-full z-10 mt-1 w-64 rounded bg-black/80 px-2 py-1.5 text-[10px] normal-case tracking-normal text-white opacity-0 transition-opacity group-hover:opacity-100">
                  Raw count of ALL FHE events emitted on-chain from the
                  fhe_events table. This is the source of truth for on-chain
                  activity.
                </span>
              </p>
              <h2 className="font-display mt-3 text-2xl">
                Top events by volume
              </h2>
            </div>
            <p className="muted-text text-xs uppercase tracking-[0.2em]">
              {opsStatus === "loading"
                ? "Loading"
                : opsStatus === "error"
                  ? "Error"
                  : `Showing ${topOps.length || 0} of ${ops.length}`}
            </p>
          </div>

          <div className="mt-8 grid gap-8 lg:grid-cols-[320px_1fr]">
            <div className="flex flex-col items-center gap-6">
              <div
                className="relative flex h-56 w-56 items-center justify-center rounded-full border border-black/10"
                style={{ background: pieGradient }}
              >
                <div className="absolute flex h-32 w-32 flex-col items-center justify-center rounded-full border border-black/10 bg-white/85 text-center">
                  <span className="muted-text text-[11px] uppercase tracking-[0.3em]">
                    Total
                  </span>
                  <span className="font-code text-xl">
                    {opsStatus === "loading" ? "—" : formatNumber(totalOps)}
                  </span>
                </div>
              </div>

              <div className="w-full space-y-3 text-sm">
                {opsStatus === "loading" ? (
                  <p className="muted-text text-sm">
                    Loading event distribution…
                  </p>
                ) : opsStatus === "error" ? (
                  <p className="muted-text text-sm">
                    {opsError ?? "Failed to load ops distribution."}
                  </p>
                ) : pieSegments.length === 0 ? (
                  <p className="muted-text text-sm">
                    No events yet. Run backfill or stream to populate the DB.
                  </p>
                ) : (
                  pieSegments.map((segment) => (
                    <div
                      key={segment.label}
                      className="flex items-center justify-between gap-3"
                    >
                      <div className="flex items-center gap-3">
                        <span
                          className="h-2.5 w-2.5 rounded-full"
                          style={{ background: segment.color }}
                        />
                        <span className="text-sm font-semibold">
                          {segment.label}
                        </span>
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
              {opsStatus === "loading" ? (
                <p className="muted-text text-sm">Loading events…</p>
              ) : opsStatus === "error" ? (
                <p className="muted-text text-sm">
                  {opsError ?? "Failed to load events."}
                </p>
              ) : topOps.length === 0 ? (
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
                    <span className="text-sm font-semibold">
                      {row.eventName}
                    </span>
                    <div className="h-2 rounded-full bg-black/10">
                      <div
                        className="h-2 rounded-full"
                        style={{
                          width: `${Math.max((row.count / maxOps) * 100, 0.5)}%`,
                          background:
                            "linear-gradient(90deg, var(--accent), var(--accent-strong))",
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

        <section className="glass-panel mt-10 rounded-[28px] p-8 fade-in-up">
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div>
              <p className="muted-text group relative inline-flex items-center gap-1 text-xs uppercase tracking-[0.3em]">
                Op types
                <span className="cursor-help text-black/30 hover:text-black/50">
                  ⓘ
                </span>
                <span className="pointer-events-none absolute left-0 top-full z-10 mt-1 w-72 rounded bg-black/80 px-2 py-1.5 text-[10px] normal-case tracking-normal text-white opacity-0 transition-opacity group-hover:opacity-100">
                  FHE type distribution by operand role:
                  <br />
                  <b>Result</b>: output type produced by the op.
                  <br />
                  <b>LHS</b>: left operand type (first input).
                  <br />
                  <b>RHS</b>: right operand type (second input), includes scalar
                  (plaintext) values.
                </span>
              </p>
              <h2 className="font-display mt-3 text-2xl">Type usage per op</h2>
            </div>
            <p className="muted-text text-xs uppercase tracking-[0.2em]">
              {opTypeStatus === "loading"
                ? "Loading"
                : opTypeStatus === "error"
                  ? "Error"
                  : `${formatNumber(opTypeRows.length)} rows`}
            </p>
          </div>
          <p className="muted-text mt-3 max-w-2xl text-sm">
            Types are decoded from handle metadata.
          </p>

          <div className="mt-6 flex flex-wrap items-center gap-3 text-xs uppercase tracking-[0.2em]">
            <div className="flex items-center gap-1 rounded-full border border-black/10 bg-white/70 p-1">
              {OP_TYPE_ROLES.map((role) => {
                const isActive = role.id === opTypeRole;
                return (
                  <button
                    key={role.id}
                    type="button"
                    onClick={() => setOpTypeRole(role.id)}
                    className={`rounded-full px-4 py-2 transition ${
                      isActive
                        ? "bg-black/90 text-white shadow"
                        : "text-black/70 hover:bg-black/5"
                    }`}
                  >
                    {role.label}
                  </button>
                );
              })}
            </div>
            <span className="muted-text text-xs uppercase tracking-[0.18em]">
              Role: {opTypeRole}
            </span>
            <input
              value={opTypeFilter}
              onChange={(event) => setOpTypeFilter(event.target.value)}
              placeholder="Filter op"
              className="rounded-full border border-black/10 bg-white/80 px-4 py-2 text-xs uppercase tracking-[0.18em]"
            />
          </div>

          <div className="mt-6 max-h-[420px] overflow-auto rounded-2xl border border-black/10 bg-white/70 p-4">
            {opTypeStatus === "loading" ? (
              <p className="muted-text text-sm">Loading op types...</p>
            ) : opTypeStatus === "error" ? (
              <p className="muted-text text-sm">
                {opTypeError ?? "Failed to load op types."}
              </p>
            ) : opTypeRows.length === 0 ? (
              <p className="muted-text text-sm">No type data available.</p>
            ) : (
              <table className="w-full text-sm">
                <thead className="text-left text-[11px] uppercase tracking-[0.2em] text-black/60">
                  <tr>
                    <th className="pb-2">Op</th>
                    <th className="pb-2">Type</th>
                    <th className="pb-2 text-right">Count</th>
                    <th className="pb-2 text-right">Share</th>
                  </tr>
                </thead>
                <tbody className="text-black/80">
                  {opTypeRows.map((row, index) => {
                    const total = opTypeTotals[row.eventName];
                    const share =
                      total && total > 0
                        ? formatPercent((row.count / total) * 100)
                        : "—";
                    return (
                      <tr key={`${row.eventName}-${row.typeValue}-${index}`}>
                        <td className="py-1">{row.eventName}</td>
                        <td className="py-1 font-code">
                          {formatOpTypeValue(row.typeValue)}
                        </td>
                        <td className="py-1 text-right font-code">
                          {formatNumber(row.count)}
                        </td>
                        <td className="py-1 text-right font-code">{share}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </div>
        </section>

        <section className="glass-panel mt-10 rounded-[28px] p-8 fade-in-up">
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div>
              <p className="muted-text text-xs uppercase tracking-[0.3em]">
                DFG stats
              </p>
              <h2 className="font-display mt-3 text-2xl">Graph summary</h2>
            </div>
            <p className="muted-text text-xs uppercase tracking-[0.2em]">
              {dfgStatsStatus === "loading"
                ? "Loading"
                : dfgStatsStatus === "error"
                  ? "Error"
                  : "Ready"}
            </p>
          </div>
          <p className="muted-text mt-3 max-w-2xl text-sm">
            Snapshot of DFG coverage, sizes, and shape variety for the selected
            network.
          </p>

          {dfgStatsStatus === "loading" ? (
            <p className="muted-text text-sm">Loading DFG stats...</p>
          ) : dfgStatsStatus === "error" ? (
            <p className="muted-text text-sm">
              {dfgStatsError ?? "Failed to load DFG stats."}
            </p>
          ) : !dfgStats ? (
            <p className="muted-text text-sm">No DFG stats available.</p>
          ) : (
            <div className="mt-6 grid gap-4 md:grid-cols-2 lg:grid-cols-4">
              {[
                {
                  label: "DFG txs",
                  value: formatNumber(dfgStats.dfg.total),
                  detail: `${formatPercent(dfgStats.coverage * 100)} coverage`,
                },
                {
                  label: "Signatures",
                  value: formatNumber(dfgStats.dfg.signatureCount),
                  detail: "Distinct structures",
                },
                {
                  label: "Avg nodes",
                  value: formatDecimal(dfgStats.dfg.avgNodes),
                  detail: `${formatNumber(dfgStats.dfg.minNodes)}–${formatNumber(
                    dfgStats.dfg.maxNodes,
                  )}`,
                },
                {
                  label: "Avg depth",
                  value: formatDecimal(dfgStats.dfg.avgDepth),
                  detail: `${formatNumber(dfgStats.dfg.minDepth)}–${formatNumber(
                    dfgStats.dfg.maxDepth,
                  )}`,
                },
              ].map((card) => (
                <div
                  key={card.label}
                  className="rounded-2xl border border-black/10 bg-white/70 p-4"
                >
                  <p className="muted-text text-[11px] uppercase tracking-[0.3em]">
                    {card.label}
                  </p>
                  <p className="font-code mt-2 text-lg">{card.value}</p>
                  <p className="muted-text mt-2 text-xs uppercase tracking-[0.18em]">
                    {card.detail}
                  </p>
                </div>
              ))}
            </div>
          )}

          {dfgStats && dfgStatsStatus === "ready" ? (
            <div className="mt-6 rounded-2xl border border-black/10 bg-white/70 p-4">
              <p className="muted-text text-xs uppercase tracking-[0.3em]">
                Nodes / edges
              </p>
              <div className="mt-3 grid gap-3 md:grid-cols-3">
                {[
                  {
                    label: "Avg edges",
                    value: formatDecimal(dfgStats.dfg.avgEdges),
                    detail: `${formatNumber(dfgStats.dfg.minEdges)}–${formatNumber(
                      dfgStats.dfg.maxEdges,
                    )}`,
                  },
                  {
                    label: "Edges per node",
                    value:
                      dfgStats.dfg.avgNodes &&
                      dfgStats.dfg.avgNodes > 0 &&
                      dfgStats.dfg.avgEdges
                        ? formatDecimal(
                            dfgStats.dfg.avgEdges / dfgStats.dfg.avgNodes,
                            2,
                          )
                        : "—",
                    detail: "Avg ratio",
                  },
                  {
                    label: "Total txs",
                    value: formatNumber(dfgStats.totalTxs),
                    detail: "All FHE txs",
                  },
                ].map((card) => (
                  <div
                    key={card.label}
                    className="rounded-2xl border border-black/5 bg-white/70 p-4"
                  >
                    <p className="muted-text text-[11px] uppercase tracking-[0.3em]">
                      {card.label}
                    </p>
                    <p className="font-code mt-2 text-lg">{card.value}</p>
                    <p className="muted-text mt-2 text-xs uppercase tracking-[0.18em]">
                      {card.detail}
                    </p>
                  </div>
                ))}
              </div>
            </div>
          ) : null}

          {/* Rolling Window Dependency Visualization */}
          {dfgStatsStatus === "ready" ? (
            <div className="mt-6 rounded-2xl border border-black/10 bg-white/70 p-4">
              {/* Header */}
              <p className="muted-text group relative inline-flex items-center gap-1 text-xs uppercase tracking-[0.3em] mb-4">
                Rolling window dependencies
                <span className="cursor-help text-black/30 hover:text-black/50">
                  ⓘ
                </span>
                <span className="pointer-events-none absolute left-0 top-full z-10 mt-1 w-72 rounded bg-black/80 px-2 py-1.5 text-[10px] normal-case tracking-normal text-white opacity-0 transition-opacity group-hover:opacity-100">
                  Compute dependency depth within a lookback window (N blocks).
                  Dependencies outside the window are truncated. Matches batch
                  processing model where only recent blocks matter.
                </span>
              </p>

              {/* Window selector */}
              <div className="flex flex-wrap items-center gap-3 mb-4">
                <span className="text-xs text-black/60">Lookback window:</span>
                <div className="flex gap-1 rounded-lg bg-black/5 p-0.5">
                  {[1, 3, 5, 10, 20, 50, 100, 200].map((size) => (
                    <button
                      key={size}
                      type="button"
                      onClick={() => setWindowLookback(size)}
                      className={`rounded px-3 py-1 text-[11px] transition ${windowLookback === size ? "bg-white shadow" : "hover:bg-white/50"}`}
                    >
                      {size}
                    </button>
                  ))}
                </div>
                {windowStatus === "loading" && (
                  <span className="text-xs text-black/50">Computing...</span>
                )}
                {windowStatus === "error" && (
                  <span className="text-xs text-red-600">
                    {windowError ?? "Failed to load."}
                  </span>
                )}
              </div>

              {/* Window stats summary */}
              {windowStats && windowStatus === "ready" && (
                <>
                  <div className="rounded-xl border border-black/5 bg-teal-50/50 p-4 mb-4">
                    <p className="text-xs text-black/60 mb-3">
                      Depth stats for {windowStats.lookbackBlocks}-block window
                      ({formatNumber(windowStats.stats.totalTxs)} txs)
                    </p>
                    <div className="grid gap-4 md:grid-cols-5">
                      <div>
                        <p className="muted-text text-[10px] uppercase tracking-[0.2em]">
                          Max Σtx_depth
                        </p>
                        <p className="font-code text-xl mt-1">
                          {formatNumber(windowStats.stats.maxCombinedDepth)}
                        </p>
                        <p className="text-[10px] text-black/50">
                          critical path
                        </p>
                      </div>
                      <div>
                        <p className="muted-text text-[10px] uppercase tracking-[0.2em]">
                          Avg Σtx_depth
                        </p>
                        <p className="font-code text-xl mt-1">
                          {formatDecimal(windowStats.stats.avgCombinedDepth, 1)}
                        </p>
                        <p className="text-[10px] text-black/50">
                          avg tx depth:{" "}
                          {formatDecimal(windowStats.stats.avgIntraDepth, 1)}
                        </p>
                      </div>
                      <div>
                        <p className="muted-text text-[10px] uppercase tracking-[0.2em]">
                          Max chain
                        </p>
                        <p className="font-code text-xl mt-1">
                          {formatNumber(windowStats.stats.maxTruncatedDepth)}
                        </p>
                        <p className="text-[10px] text-black/50">
                          longest tx chain
                        </p>
                      </div>
                      <div>
                        <p className="muted-text text-[10px] uppercase tracking-[0.2em]">
                          Parallelism
                        </p>
                        <p className="font-code text-xl mt-1">
                          {formatPercent(
                            windowStats.stats.parallelismRatio * 100,
                          )}
                        </p>
                        <p className="text-[10px] text-black/50">
                          {formatNumber(windowStats.stats.independentTxs)} /{" "}
                          {formatNumber(windowStats.stats.totalTxs)}
                        </p>
                      </div>
                      <div>
                        <p className="muted-text text-[10px] uppercase tracking-[0.2em]">
                          Dependent txs
                        </p>
                        <p className="font-code text-xl mt-1">
                          {formatNumber(windowStats.stats.dependentTxs)}
                        </p>
                        <p className="text-[10px] text-black/50">
                          {formatPercent(
                            (windowStats.stats.dependentTxs /
                              Math.max(windowStats.stats.totalTxs, 1)) *
                              100,
                          )}
                        </p>
                      </div>
                    </div>
                  </div>

                  {/* Σtx_depth distribution histogram */}
                  <div className="rounded-xl border border-black/5 bg-white p-4 mb-4">
                    <div className="flex items-center justify-between mb-3">
                      <p className="muted-text text-[10px] uppercase tracking-[0.2em]">
                        Σtx_depth distribution
                      </p>
                      <div className="flex items-center gap-3 text-[9px]">
                        <span className="flex items-center gap-1">
                          <span className="w-2 h-2 rounded-sm bg-teal-600" />
                          Upstream chain
                        </span>
                        <span className="flex items-center gap-1">
                          <span className="w-2 h-2 rounded-sm bg-teal-300" />
                          This tx
                        </span>
                      </div>
                    </div>
                    {Object.keys(windowStats.stats.combinedDepthDistribution)
                      .length === 0 ? (
                      <p className="muted-text text-sm">
                        No depth distribution data. All transactions may be
                        independent (depth 0).
                      </p>
                    ) : (
                      <div className="flex gap-2">
                        {/* Y axis */}
                        {(() => {
                          const dist =
                            windowStats.stats.combinedDepthDistribution;
                          const maxCount = Math.max(
                            ...Object.values(dist).map((d) => d.count),
                          );
                          return (
                            <div
                              className="flex flex-col justify-between text-[9px] text-black/50 text-right pr-1"
                              style={{ height: 80 }}
                            >
                              <span>{formatNumber(maxCount)}</span>
                              <span>
                                {formatNumber(Math.round(maxCount / 2))}
                              </span>
                              <span>0</span>
                            </div>
                          );
                        })()}
                        {/* Stacked bars */}
                        <div className="flex-1 flex items-end gap-px">
                          {(() => {
                            const dist =
                              windowStats.stats.combinedDepthDistribution;
                            const maxDepth = Math.max(
                              ...Object.keys(dist).map(Number),
                            );
                            const maxCount = Math.max(
                              ...Object.values(dist).map((d) => d.count),
                            );
                            const bars = [];
                            for (let d = 0; d <= Math.min(maxDepth, 20); d++) {
                              const bucket = dist[d];
                              const count = bucket?.count ?? 0;
                              const avgIntra = bucket?.avgIntra ?? 0;
                              const totalHeightPx =
                                maxCount > 0
                                  ? Math.max((count / maxCount) * 80, 2)
                                  : 2;
                              // Split bar proportionally
                              const intraPortion = d > 0 ? avgIntra / d : 0;
                              const intraHeightPx =
                                totalHeightPx * intraPortion;
                              const interHeightPx =
                                totalHeightPx - intraHeightPx;
                              bars.push(
                                <div
                                  key={d}
                                  className="flex-1 flex flex-col items-center justify-end"
                                >
                                  <div className="w-full min-w-[4px] flex flex-col justify-end">
                                    {/* This tx's intra (top, lighter) */}
                                    {intraHeightPx > 0 && (
                                      <div
                                        className="w-full bg-teal-300 rounded-t"
                                        style={{ height: intraHeightPx }}
                                      />
                                    )}
                                    {/* Upstream chain's intra (bottom, darker) */}
                                    {interHeightPx > 0 && (
                                      <div
                                        className={`w-full bg-teal-600 ${intraHeightPx === 0 ? "rounded-t" : ""}`}
                                        style={{ height: interHeightPx }}
                                      />
                                    )}
                                  </div>
                                  <span className="text-[9px] text-black/50 mt-1">
                                    {d}
                                  </span>
                                </div>,
                              );
                            }
                            if (maxDepth > 20) {
                              bars.push(
                                <div
                                  key="overflow"
                                  className="flex-1 flex flex-col items-center justify-end"
                                >
                                  <span className="text-[9px] text-black/30">
                                    ...
                                  </span>
                                  <span className="text-[9px] text-black/50 mt-1">
                                    {maxDepth}
                                  </span>
                                </div>,
                              );
                            }
                            return bars;
                          })()}
                        </div>
                      </div>
                    )}
                  </div>

                  {/* Top depth transactions */}
                  {windowStats.topDepthTxs.length > 0 && (
                    <div className="rounded-xl border border-black/5 bg-white p-4">
                      <div className="flex items-center justify-between mb-3">
                        <p className="muted-text text-[10px] uppercase tracking-[0.2em]">
                          Top depth transactions
                        </p>
                        <button
                          type="button"
                          onClick={() => {
                            setDfgLookback(windowStats.lookbackBlocks);
                            dfgViewerRef.current?.scrollIntoView({
                              behavior: "smooth",
                              block: "start",
                            });
                          }}
                          className="text-[10px] px-2 py-0.5 border border-teal-600 text-teal-600 rounded hover:bg-teal-50"
                        >
                          Enable cut edges in viewer
                        </button>
                      </div>
                      <div className="space-y-1">
                        {windowStats.topDepthTxs.map((tx) => (
                          <div
                            key={tx.txHash}
                            className="flex items-center justify-between text-xs bg-black/[0.02] rounded px-2 py-1.5"
                          >
                            <button
                              type="button"
                              onClick={() => {
                                setDfgSelection(tx.txHash);
                                setDfgQuery(tx.txHash);
                                setDfgLookback(windowStats.lookbackBlocks);
                                dfgViewerRef.current?.scrollIntoView({
                                  behavior: "smooth",
                                  block: "start",
                                });
                              }}
                              className="font-mono text-teal-700 hover:underline text-left"
                            >
                              {shortenHandle(tx.txHash, 10, 6)}
                            </button>
                            <div className="flex items-center gap-4 text-black/50">
                              <span>block {formatNumber(tx.blockNumber)}</span>
                              <span className="font-mono text-[10px]">
                                Σtx_depth:{" "}
                                <span className="text-teal-700 text-xs">
                                  {tx.combinedDepth}
                                </span>
                                <span className="text-black/40 mx-2">•</span>
                                chain: {tx.truncatedDepth} tx
                                <span className="text-black/40 mx-2">•</span>
                                this tx depth: {tx.intraTxDepth}
                              </span>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </>
              )}
            </div>
          ) : null}
        </section>

        <section className="glass-panel mt-10 rounded-[28px] p-8 fade-in-up">
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div>
              <p className="muted-text text-xs uppercase tracking-[0.3em]">
                DFG rollup
              </p>
              <h2 className="font-display mt-3 text-2xl">Aggregate usage</h2>
            </div>
            <p className="muted-text text-xs uppercase tracking-[0.2em]">
              {dfgRollupStatus === "loading"
                ? "Loading"
                : dfgRollupStatus === "error"
                  ? "Error"
                  : dfgRollup
                    ? `Updated ${new Date(dfgRollup.updatedAt).toLocaleString()}`
                    : "Ready"}
            </p>
          </div>
          <p className="muted-text mt-3 max-w-2xl text-sm">
            Summed across all DFGs. Run `bun run dfg:rollup` after rebuilding
            graphs.
          </p>

          {dfgRollupStatus === "loading" ? (
            <p className="muted-text text-sm">Loading DFG rollup...</p>
          ) : dfgRollupStatus === "error" ? (
            <p className="muted-text text-sm">
              {dfgRollupError ?? "Failed to load DFG rollup."}
            </p>
          ) : !dfgRollupStats ? (
            <p className="muted-text text-sm">No rollup data yet.</p>
          ) : (
            <div className="mt-6 grid gap-6 lg:grid-cols-[1.1fr_0.9fr]">
              <div className="rounded-2xl border border-black/10 bg-white/70 p-4">
                <div className="flex items-center justify-between gap-4">
                  <p className="muted-text group relative inline-flex items-center gap-1 text-xs uppercase tracking-[0.3em]">
                    Top ops
                    <span className="cursor-help text-black/30 hover:text-black/50">
                      ⓘ
                    </span>
                    <span className="pointer-events-none absolute left-0 top-full z-10 mt-1 w-64 rounded bg-black/80 px-2 py-1.5 text-[10px] normal-case tracking-normal text-white opacity-0 transition-opacity group-hover:opacity-100">
                      FHE operations from reconstructed DFG graphs. Similar to
                      "Top events" but only counts ops that are part of
                      transaction dataflow graphs.
                    </span>
                  </p>
                  <span className="font-code text-xs">
                    {formatNumber(dfgRollupTotalNodes)} nodes
                  </span>
                </div>
                <div className="mt-3 space-y-2">
                  {dfgRollupOpRows.map((row) => {
                    const share =
                      dfgRollupTotalNodes > 0
                        ? formatPercent((row.count / dfgRollupTotalNodes) * 100)
                        : "—";
                    return (
                      <div
                        key={row.op}
                        className="flex items-center justify-between text-sm"
                      >
                        <span>{row.op}</span>
                        <span className="font-code text-xs">
                          {formatNumber(row.count)} · {share}
                        </span>
                      </div>
                    );
                  })}
                </div>
              </div>

              <div className="grid gap-4">
                <div className="rounded-2xl border border-black/10 bg-white/70 p-4">
                  <p className="muted-text group relative inline-flex items-center gap-1 text-xs uppercase tracking-[0.3em]">
                    Input kinds
                    <span className="cursor-help text-black/30 hover:text-black/50">
                      ⓘ
                    </span>
                    <span className="pointer-events-none absolute left-0 top-full z-10 mt-1 w-72 rounded bg-black/80 px-2 py-1.5 text-[10px] normal-case tracking-normal text-white opacity-0 transition-opacity group-hover:opacity-100">
                      <b>Ciphertext</b>: handle from same tx (internal).
                      <br />
                      <b>Trivial</b>: trivial encryption of a constant.
                      <br />
                      <b>External</b>: handle from different tx (cross-tx dep).
                      <br />
                      <b>Scalar</b>: plaintext value, not encrypted.
                    </span>
                  </p>
                  <div className="mt-3 space-y-2 text-sm">
                    {[
                      {
                        label: "Ciphertext",
                        value: dfgRollupInputTotals.ciphertext,
                      },
                      { label: "Trivial", value: dfgRollupInputTotals.trivial },
                      {
                        label: "External",
                        value: dfgRollupInputTotals.external,
                      },
                      { label: "Scalar", value: dfgRollupInputTotals.scalar },
                    ].map((row) => (
                      <div
                        key={row.label}
                        className="flex items-center justify-between"
                      >
                        <span>{row.label}</span>
                        <span className="font-code text-xs">
                          {formatNumber(row.value)}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>

                <div className="rounded-2xl border border-black/10 bg-white/70 p-4">
                  <p className="muted-text group relative inline-flex items-center gap-1 text-xs uppercase tracking-[0.3em]">
                    Operand pairs
                    <span className="cursor-help text-black/30 hover:text-black/50">
                      ⓘ
                    </span>
                    <span className="pointer-events-none absolute left-0 top-full z-10 mt-1 w-72 rounded bg-black/80 px-2 py-1.5 text-[10px] normal-case tracking-normal text-white opacity-0 transition-opacity group-hover:opacity-100">
                      For binary ops (add, mul, etc.), shows LHS × RHS input
                      kind combinations. E.g., "ciphertext + scalar" = encrypted
                      LHS with plaintext RHS. Scalar ops are cheaper than
                      ciphertext × ciphertext.
                    </span>
                  </p>
                  <div className="mt-3 space-y-2 text-sm">
                    {dfgRollupOperandPairs.length === 0 ? (
                      <p className="muted-text text-sm">
                        No operand pairs recorded.
                      </p>
                    ) : (
                      dfgRollupOperandPairs.map((row) => (
                        <div
                          key={row.pair}
                          className="flex items-center justify-between"
                        >
                          <span>{row.pair}</span>
                          <span className="font-code text-xs">
                            {formatNumber(row.count)}
                          </span>
                        </div>
                      ))
                    )}
                  </div>
                </div>
              </div>
            </div>
          )}
        </section>

        <section className="glass-panel mt-10 rounded-[28px] p-8 fade-in-up">
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div>
              <p className="muted-text text-xs uppercase tracking-[0.3em]">
                Signatures
              </p>
              <h2 className="font-display mt-3 text-2xl">Top DFG patterns</h2>
            </div>
            <div className="flex flex-wrap items-center gap-3">
              <p className="muted-text text-xs uppercase tracking-[0.2em]">
                {dfgSignatureStatus === "loading"
                  ? "Loading"
                  : dfgSignatureStatus === "error"
                    ? "Error"
                    : `Showing ${formatNumber(dfgSignatures.length)} of ${formatNumber(
                        dfgSignatureTotal,
                      )}`}
              </p>
              <div className="flex w-full min-w-0 flex-wrap items-center gap-x-2 gap-y-1 rounded-2xl border border-black/10 bg-white/80 px-3 py-2 text-[10px] uppercase tracking-[0.2em] text-black/70 sm:w-auto">
                <span>Min nodes</span>
                <input
                  type="number"
                  min={0}
                  value={dfgSignatureMinNodes}
                  onChange={(event) => {
                    const next = Number(event.target.value);
                    setDfgSignatureMinNodes(
                      Number.isFinite(next) ? Math.max(0, next) : 0,
                    );
                  }}
                  className="w-14 shrink-0 rounded-full border border-black/10 bg-white px-2 py-1 text-center text-[10px] uppercase tracking-[0.2em]"
                />
                <span>Min edges</span>
                <input
                  type="number"
                  min={0}
                  value={dfgSignatureMinEdges}
                  onChange={(event) => {
                    const next = Number(event.target.value);
                    setDfgSignatureMinEdges(
                      Number.isFinite(next) ? Math.max(0, next) : 0,
                    );
                  }}
                  className="w-14 shrink-0 rounded-full border border-black/10 bg-white px-2 py-1 text-center text-[10px] uppercase tracking-[0.2em]"
                />
                <span className="hidden sm:inline">Scope</span>
                <button
                  type="button"
                  onClick={() => setDfgRangeMode("window")}
                  className={`rounded-full border border-black/10 px-2 py-0.5 text-[9px] uppercase tracking-[0.2em] transition ${
                    dfgRangeMode === "window"
                      ? "bg-black/80 text-white"
                      : "bg-white/80 text-black/60 hover:bg-white"
                  }`}
                >
                  window
                </button>
                <button
                  type="button"
                  onClick={() => setDfgRangeMode("range")}
                  className={`rounded-full border border-black/10 px-2 py-0.5 text-[9px] uppercase tracking-[0.2em] transition ${
                    dfgRangeMode === "range"
                      ? "bg-black/80 text-white"
                      : "bg-white/80 text-black/60 hover:bg-white"
                  }`}
                >
                  range
                </button>
                {dfgRangeMode === "range" && (
                  <>
                    <span className="hidden sm:inline">Start</span>
                    <input
                      type="number"
                      min={0}
                      value={dfgStartBlock}
                      onChange={(event) => setDfgStartBlock(event.target.value)}
                      className="w-24 shrink-0 rounded-full border border-black/10 bg-white px-3 py-1 text-center text-[10px] uppercase tracking-[0.2em] sm:w-28"
                    />
                    <span className="hidden sm:inline">End</span>
                    <input
                      type="number"
                      min={0}
                      value={dfgEndBlock}
                      onChange={(event) => setDfgEndBlock(event.target.value)}
                      className="w-24 shrink-0 rounded-full border border-black/10 bg-white px-3 py-1 text-center text-[10px] uppercase tracking-[0.2em] sm:w-28"
                    />
                    <button
                      type="button"
                      onClick={() => {
                        if (summary?.maxBlock != null)
                          setDfgEndBlock(String(summary.maxBlock));
                      }}
                      className="rounded-full border border-black/10 bg-white/80 px-2 py-0.5 text-[9px] uppercase tracking-[0.2em] text-black/60 transition hover:bg-white"
                      title="Set end block to latest block in the local DB"
                    >
                      max
                    </button>
                  </>
                )}
                <span className="hidden sm:inline">Caller</span>
                <input
                  value={dfgCaller}
                  onChange={(event) => setDfgCaller(event.target.value)}
                  placeholder="0x…"
                  className="w-full min-w-[10rem] flex-1 rounded-full border border-black/10 bg-white px-3 py-1 text-[10px] uppercase tracking-[0.2em] sm:w-56 sm:flex-none"
                />
                <button
                  type="button"
                  onClick={() =>
                    setDfgCaller("0x9fdd4b67c241779dca4d2eaf3d5946fb699f5d7a")
                  }
                  className="rounded-full border border-black/10 bg-white/80 px-2 py-0.5 text-[9px] uppercase tracking-[0.2em] text-black/60 transition hover:bg-white"
                  title="Preset: attacker entrypoint (args_json.caller)"
                >
                  preset
                </button>
                <button
                  type="button"
                  onClick={() => setDfgCaller("")}
                  className="rounded-full border border-black/10 bg-white/80 px-2 py-0.5 text-[9px] uppercase tracking-[0.2em] text-black/60 transition hover:bg-white"
                >
                  clear
                </button>
              </div>
            </div>
          </div>
          <p className="muted-text mt-3 max-w-2xl text-sm">
            Signatures hash the ordered ops + edges so repeated graph structures
            can be counted without heavy mining.
          </p>

          <div className="mt-6 max-h-[360px] overflow-auto rounded-2xl border border-black/10 bg-white/70 p-4">
            {dfgSignatureStatus === "loading" ? (
              <p className="muted-text text-sm">Loading signatures...</p>
            ) : dfgSignatureStatus === "error" ? (
              <p className="muted-text text-sm">
                {dfgSignatureError ?? "Failed to load signatures."}
              </p>
            ) : dfgSignatures.length === 0 ? (
              <p className="muted-text text-sm">No signatures available yet.</p>
            ) : (
              <table className="w-full text-sm">
                <thead className="text-left text-[11px] uppercase tracking-[0.2em] text-black/60">
                  <tr>
                    <th className="pb-2">Signature</th>
                    <th className="pb-2 text-right">Txs</th>
                    <th className="pb-2 text-right">Share</th>
                    <th className="pb-2 text-right">Avg nodes</th>
                    <th className="pb-2 text-right">Avg edges</th>
                  </tr>
                </thead>
                <tbody className="text-black/80">
                  {dfgSignatures.map((row, index) => {
                    const baseTotal =
                      dfgSignatureTxTotal > 0
                        ? dfgSignatureTxTotal
                        : (dfgStats?.dfg.total ?? dfgTotal);
                    const share =
                      baseTotal > 0
                        ? formatPercent((row.txCount / baseTotal) * 100)
                        : "—";
                    return (
                      <tr
                        key={`${row.signatureHash}-${index}`}
                        title="Click to load a sample DFG for this signature"
                        className="cursor-pointer transition hover:bg-black/5"
                        onClick={() => {
                          setDfgSignatureSelection(row.signatureHash);
                          setDfgSelection(null);
                          dfgViewerRef.current?.scrollIntoView({
                            behavior: "smooth",
                            block: "start",
                          });
                        }}
                      >
                        <td className="py-1">
                          <span className="flex items-center gap-2 font-code text-xs text-black/80">
                            {shortenHandle(row.signatureHash, 10, 8)}
                            <span className="rounded-full border border-black/10 bg-white/80 px-2 py-0.5 text-[9px] uppercase tracking-[0.2em] text-black/60">
                              View
                            </span>
                          </span>
                        </td>
                        <td className="py-1 text-right font-code">
                          {formatNumber(row.txCount)}
                        </td>
                        <td className="py-1 text-right font-code">{share}</td>
                        <td className="py-1 text-right font-code">
                          {formatNumber(Math.round(row.avgNodes))}
                        </td>
                        <td className="py-1 text-right font-code">
                          {formatNumber(Math.round(row.avgEdges))}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </div>
        </section>

        <section
          ref={dfgViewerRef}
          className="glass-panel mt-10 rounded-[28px] p-8 fade-in-up"
        >
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div>
              <p className="muted-text text-xs uppercase tracking-[0.3em]">
                DFG viewer
              </p>
              <h2 className="font-display mt-3 text-2xl">Tx dataflow graphs</h2>
            </div>
            <div className="flex flex-wrap items-center gap-3">
              <p className="muted-text text-xs uppercase tracking-[0.2em]">
                Showing {formatNumber(dfgTxs.length)} of{" "}
                {formatNumber(dfgTotal)} ·{" "}
                {dfgStatus === "loading"
                  ? "Loading"
                  : dfgStatus === "error"
                    ? "Error"
                    : "Ready"}
              </p>
              {dfgLookback !== null && (
                <div className="flex items-center gap-2 rounded-full border border-violet-200 bg-violet-50 px-3 py-1 text-[10px] uppercase tracking-[0.2em] text-violet-700">
                  <span>Window: {dfgLookback} blocks</span>
                  <button
                    type="button"
                    onClick={() => setDfgLookback(null)}
                    className="ml-1 text-violet-500 hover:text-violet-700"
                  >
                    ×
                  </button>
                </div>
              )}
            </div>
          </div>
          <p className="muted-text mt-3 max-w-2xl text-sm">
            DFGs are reconstructed from L1 logs per transaction. External inputs
            are handles not produced inside the same tx.
            {dfgLookback !== null && (
              <span className="text-violet-600">
                {" "}
                Dashed edges show dependencies outside the {dfgLookback}-block
                window.
              </span>
            )}
          </p>

          <div className="mt-6 flex flex-wrap items-center gap-4">
            {dfgSignatureSelection ? (
              <div className="flex items-center gap-2 rounded-2xl border border-black/10 bg-white/70 px-4 py-2 text-xs uppercase tracking-[0.2em]">
                <span className="text-black/70">
                  Signature {shortenHandle(dfgSignatureSelection, 8, 6)} ·{" "}
                  {formatNumber(dfgTotal)} matches
                </span>
                <button
                  type="button"
                  onClick={() => setDfgSignatureSelection(null)}
                  className="rounded-full border border-black/10 bg-white px-3 py-1 text-[10px] uppercase tracking-[0.2em] text-black/70 transition hover:bg-black/5"
                >
                  Clear
                </button>
              </div>
            ) : null}
            <form
              onSubmit={handleDfgSearch}
              className="flex items-center gap-2"
            >
              <input
                value={dfgQuery}
                onChange={(event) => setDfgQuery(event.target.value)}
                placeholder="Search tx hash"
                className="w-64 rounded-full border border-black/10 bg-white/80 px-4 py-2 text-sm"
              />
              <button
                type="submit"
                className="rounded-full border border-black/10 bg-black/90 px-4 py-2 text-xs uppercase tracking-[0.2em] text-white transition hover:-translate-y-0.5"
              >
                Load
              </button>
            </form>
          </div>

          <div className="mt-6 space-y-6">
            {dfgDetailStatus === "loading" ? (
              <p className="muted-text text-sm">Loading DFG details...</p>
            ) : dfgDetailStatus === "error" ? (
              <p className="muted-text text-sm">
                {dfgDetailError ?? "Failed to load DFG details."}
              </p>
            ) : !dfgSelected ? (
              <p className="muted-text text-sm">Select a tx to view its DFG.</p>
            ) : (
              <>
                <div className="grid gap-3 md:grid-cols-3">
                  {[
                    {
                      label: "Block",
                      value: formatNumber(dfgSelected.blockNumber),
                    },
                    {
                      label: "Nodes",
                      value: formatNumber(dfgSelected.nodeCount),
                    },
                    {
                      label: "Edges",
                      value: formatNumber(dfgSelected.edgeCount),
                    },
                  ].map((card) => (
                    <div
                      key={card.label}
                      className="rounded-2xl border border-black/10 bg-white/70 p-4"
                    >
                      <p className="muted-text text-[11px] uppercase tracking-[0.3em]">
                        {card.label}
                      </p>
                      <p className="font-code mt-2 text-lg">{card.value}</p>
                    </div>
                  ))}
                </div>

                <div className="rounded-2xl border border-black/10 bg-white/70 p-4">
                  <div className="flex items-center justify-between gap-4">
                    <p className="muted-text text-xs uppercase tracking-[0.3em]">
                      Graph view
                    </p>
                    <button
                      type="button"
                      onClick={resetGraphView}
                      className="rounded-full border border-black/10 bg-white px-3 py-1 text-[10px] uppercase tracking-[0.2em] text-black/70 transition hover:bg-black/5"
                    >
                      Reset view
                    </button>
                  </div>
                  <p className="muted-text mt-2 text-xs">
                    Scroll to zoom, drag to pan. Layout generated with dagre.
                  </p>
                  {dfgGraph ? (
                    <div className="mt-4 h-[360px] w-full overflow-auto rounded-2xl border border-black/10 bg-white">
                      <svg
                        viewBox={
                          dfgViewBox
                            ? `${dfgViewBox.x} ${dfgViewBox.y} ${dfgViewBox.width} ${dfgViewBox.height}`
                            : `${dfgGraph.viewBox.x} ${dfgGraph.viewBox.y} ${dfgGraph.viewBox.width} ${dfgGraph.viewBox.height}`
                        }
                        ref={dfgSvgRef}
                        className={`h-full w-full ${dfgDragging ? "cursor-grabbing" : "cursor-grab"}`}
                        onMouseDown={handleGraphMouseDown}
                        onMouseMove={handleGraphMouseMove}
                        onMouseUp={handleGraphMouseUp}
                        onMouseLeave={handleGraphMouseUp}
                        role="img"
                        aria-label="Data flow graph visualization"
                      >
                        <g>
                          {dfgGraph.edges.map((edge) => (
                            <g key={edge.id}>
                              <path
                                d={edge.path}
                                fill="none"
                                stroke="rgba(15, 23, 42, 0.25)"
                                strokeWidth="1.5"
                              />
                              {edge.label ? (
                                <text
                                  x={edge.labelX}
                                  y={edge.labelY}
                                  textAnchor="middle"
                                  dominantBaseline="central"
                                  fontSize="9"
                                  fill="rgba(15, 23, 42, 0.8)"
                                  paintOrder="stroke"
                                  stroke="white"
                                  strokeWidth="3"
                                >
                                  {edge.label}
                                </text>
                              ) : null}
                            </g>
                          ))}
                        </g>
                        <g>
                          {dfgGraph.nodes.map((node) => {
                            // Check if this node consumes any cut-edge handle
                            const dfgNode = dfgNodes.find(
                              (n) => String(n.nodeId) === node.id,
                            );
                            const hasCutEdge =
                              dfgDetail?.cutEdges?.some((ce) =>
                                dfgNode?.typeInfo?.inputs?.some(
                                  (input) =>
                                    input.kind === "external" &&
                                    input.handle === ce.handle,
                                ),
                              ) ?? false;

                            return (
                              <g
                                key={node.id}
                                transform={`translate(${node.x}, ${node.y})`}
                              >
                                <rect
                                  x={-node.width / 2}
                                  y={-node.height / 2}
                                  width={node.width}
                                  height={node.height}
                                  rx={12}
                                  fill="white"
                                  stroke={
                                    hasCutEdge
                                      ? "#7c3aed"
                                      : "rgba(15, 23, 42, 0.2)"
                                  }
                                  strokeWidth={hasCutEdge ? 2 : 1}
                                />
                                {hasCutEdge && (
                                  <circle
                                    cx={-node.width / 2 - 4}
                                    cy={0}
                                    r={4}
                                    fill="#7c3aed"
                                  />
                                )}
                                <text
                                  textAnchor="middle"
                                  dominantBaseline="middle"
                                  fontSize="11"
                                  fill="rgba(15, 23, 42, 0.85)"
                                >
                                  {node.label}
                                </text>
                              </g>
                            );
                          })}
                        </g>
                      </svg>
                    </div>
                  ) : (
                    <p className="muted-text mt-4 text-sm">
                      Graph data unavailable.
                    </p>
                  )}
                </div>

                <div className="grid gap-6 lg:grid-cols-2">
                  <div className="rounded-2xl border border-black/10 bg-white/70 p-4">
                    <p className="muted-text text-xs uppercase tracking-[0.3em]">
                      Op counts
                    </p>
                    <div className="mt-3 max-h-52 overflow-auto">
                      {dfgOpRows.length === 0 ? (
                        <p className="muted-text text-sm">
                          No op counts recorded.
                        </p>
                      ) : (
                        <table className="w-full text-sm">
                          <thead className="text-left text-[11px] uppercase tracking-[0.2em] text-black/60">
                            <tr>
                              <th className="pb-2">Op</th>
                              <th className="pb-2 text-right">Count</th>
                            </tr>
                          </thead>
                          <tbody className="text-black/80">
                            {dfgOpRows.map((row) => (
                              <tr key={row.op}>
                                <td className="py-1">{row.op}</td>
                                <td className="py-1 text-right font-code">
                                  {formatNumber(row.count)}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      )}
                    </div>
                  </div>

                  <div className="rounded-2xl border border-black/10 bg-white/70 p-4">
                    <p className="muted-text text-xs uppercase tracking-[0.3em]">
                      Input kinds
                    </p>
                    <div className="mt-3 max-h-52 overflow-auto">
                      {dfgInputRows.length === 0 ? (
                        <p className="muted-text text-sm">
                          No input breakdown available.
                        </p>
                      ) : (
                        <table className="w-full text-sm">
                          <thead className="text-left text-[11px] uppercase tracking-[0.2em] text-black/60">
                            <tr>
                              <th className="pb-2">Op</th>
                              <th className="pb-2 text-right">CT</th>
                              <th className="pb-2 text-right">Trivial</th>
                              <th className="pb-2 text-right">External</th>
                              <th className="pb-2 text-right">Scalar</th>
                            </tr>
                          </thead>
                          <tbody className="text-black/80">
                            {dfgInputRows.map((row) => (
                              <tr key={row.op}>
                                <td className="py-1">{row.op}</td>
                                <td className="py-1 text-right font-code">
                                  {formatNumber(row.ciphertext)}
                                </td>
                                <td className="py-1 text-right font-code">
                                  {formatNumber(row.trivial)}
                                </td>
                                <td className="py-1 text-right font-code">
                                  {formatNumber(row.external)}
                                </td>
                                <td className="py-1 text-right font-code">
                                  {formatNumber(row.scalar)}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      )}
                    </div>
                  </div>
                </div>

                <div className="rounded-2xl border border-black/10 bg-white/70 p-4">
                  <div className="flex items-center justify-between gap-4">
                    <p className="muted-text text-xs uppercase tracking-[0.3em]">
                      Nodes
                    </p>
                    <span className="font-code text-xs">
                      {formatNumber(dfgNodes.length)}
                    </span>
                  </div>
                  <div className="mt-3 max-h-64 overflow-auto">
                    {dfgNodes.length === 0 ? (
                      <p className="muted-text text-sm">No nodes recorded.</p>
                    ) : (
                      <table className="w-full text-sm">
                        <thead className="text-left text-[11px] uppercase tracking-[0.2em] text-black/60">
                          <tr>
                            <th className="pb-2">ID</th>
                            <th className="pb-2">Op</th>
                            <th className="pb-2">Inputs</th>
                            <th className="pb-2">Output</th>
                          </tr>
                        </thead>
                        <tbody className="text-black/80">
                          {dfgNodes.map((node) => {
                            const inputs = node.typeInfo?.inputs ?? [];
                            const inputText =
                              inputs.length === 0
                                ? "—"
                                : inputs
                                    .map((input) => {
                                      const typeLabel =
                                        input.type !== undefined &&
                                        input.type !== null
                                          ? ` ${formatFheType(input.type)}`
                                          : "";
                                      return `${input.role}:${input.kind}${typeLabel}`;
                                    })
                                    .join(", ");
                            const outputType = node.typeInfo?.output?.type;
                            const outputKind = node.typeInfo?.output?.kind;
                            const outputLabel = [
                              shortenHandle(node.outputHandle),
                              outputKind,
                              outputType !== undefined && outputType !== null
                                ? formatFheType(outputType)
                                : null,
                            ]
                              .filter(Boolean)
                              .join(" ");
                            return (
                              <tr key={node.nodeId}>
                                <td className="py-1 font-code">
                                  {node.nodeId}
                                </td>
                                <td className="py-1">{node.op}</td>
                                <td className="py-1 text-xs text-black/70">
                                  {inputText}
                                </td>
                                <td className="py-1 font-code text-xs">
                                  {outputLabel}
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    )}
                  </div>
                </div>

                <div className="rounded-2xl border border-black/10 bg-white/70 p-4">
                  <div className="flex items-center justify-between gap-4">
                    <p className="muted-text text-xs uppercase tracking-[0.3em]">
                      Edges
                    </p>
                    <span className="font-code text-xs">
                      {formatNumber(dfgEdges.length)}
                    </span>
                  </div>
                  <div className="mt-3 max-h-52 overflow-auto">
                    {dfgEdges.length === 0 ? (
                      <p className="muted-text text-sm">No edges recorded.</p>
                    ) : (
                      <table className="w-full text-sm">
                        <thead className="text-left text-[11px] uppercase tracking-[0.2em] text-black/60">
                          <tr>
                            <th className="pb-2">From</th>
                            <th className="pb-2">To</th>
                            <th className="pb-2">Handle</th>
                            <th className="pb-2">Type</th>
                          </tr>
                        </thead>
                        <tbody className="text-black/80">
                          {dfgEdges.map((edge, index) => (
                            <tr
                              key={`${edge.fromNodeId}-${edge.toNodeId}-${index}`}
                            >
                              <td className="py-1 font-code">
                                {edge.fromNodeId}
                              </td>
                              <td className="py-1 font-code">
                                {edge.toNodeId}
                              </td>
                              <td className="py-1 font-code text-xs">
                                {shortenHandle(edge.inputHandle)}
                              </td>
                              <td className="py-1 font-code text-xs">
                                {dfgEdgeTypes[index] ?? "—"}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    )}
                  </div>
                </div>

                <div className="rounded-2xl border border-black/10 bg-white/70 p-4">
                  <div className="flex items-center justify-between gap-4">
                    <p className="muted-text text-xs uppercase tracking-[0.3em]">
                      External inputs
                    </p>
                    <div className="flex items-center gap-2">
                      {dfgDetail?.cutEdges && dfgDetail.cutEdges.length > 0 && (
                        <span className="text-[10px] text-violet-600 uppercase tracking-wider">
                          {dfgDetail.cutEdges.length} outside window
                        </span>
                      )}
                      <span className="font-code text-xs">
                        {formatNumber(dfgInputs.length)}
                      </span>
                    </div>
                  </div>
                  <div className="mt-3 max-h-40 overflow-auto">
                    {dfgInputs.length === 0 ? (
                      <p className="muted-text text-sm">No external inputs.</p>
                    ) : (
                      <div className="space-y-2">
                        {dfgInputs.map((input) => {
                          const cutEdge = dfgDetail?.cutEdges?.find(
                            (ce) => ce.handle === input.handle,
                          );
                          const isCut = Boolean(cutEdge);
                          return (
                            <div
                              key={input.handle}
                              className={`text-xs font-code ${isCut ? "text-violet-600" : "text-black/80"}`}
                            >
                              {shortenHandle(input.handle, 10, 8)} ·{" "}
                              {input.kind}
                              {isCut && cutEdge && (
                                <span className="ml-2 text-[10px] text-violet-500">
                                  ← block {formatNumber(cutEdge.producerBlock)}
                                </span>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </div>
                </div>
              </>
            )}
          </div>
        </section>

        <footer className="muted-text mt-6 text-xs uppercase tracking-[0.2em]">
          Raw events remain the source of truth. Bucketed rollups kick in when
          performance demands it.
        </footer>
      </div>
    </div>
  );
}

export default App;
