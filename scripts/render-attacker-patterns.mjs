import fs from "node:fs/promises";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

const dbPath = process.env.DB_PATH ?? "data/fhevm_stats.sqlite";
const chainId = Number(process.env.CHAIN_ID ?? 11155111);

async function findLatestReport() {
  const dir = process.env.REPORT_DIR ?? "/tmp";
  const files = await fs.readdir(dir);
  const candidates = files
    .filter((f) => f.startsWith("attacker_dfg_patterns_") && f.endsWith(".json"))
    .map((f) => path.join(dir, f));
  if (candidates.length === 0) return null;

  let best = candidates[0];
  let bestMtime = (await fs.stat(best)).mtimeMs;
  for (const file of candidates.slice(1)) {
    const mtime = (await fs.stat(file)).mtimeMs;
    if (mtime > bestMtime) {
      bestMtime = mtime;
      best = file;
    }
  }
  return best;
}

const reportPath = process.env.REPORT_PATH ?? (await findLatestReport());
if (!reportPath) {
  throw new Error(
    "No report found. Set REPORT_PATH=/tmp/attacker_dfg_patterns_...json or run scripts/analyze-attacker.mjs first.",
  );
}

const report = JSON.parse(await fs.readFile(reportPath, "utf8"));
const topSignatures = report.topSignatures ?? [];
if (!Array.isArray(topSignatures) || topSignatures.length === 0) {
  throw new Error(`No topSignatures in report (${reportPath}).`);
}

const db = new DatabaseSync(dbPath, { readonly: true });

const loadNodes = db.prepare(
  `SELECT node_id AS nodeId, op
   FROM dfg_nodes
   WHERE chain_id = ? AND tx_hash = ?
   ORDER BY node_id`,
);
const loadEdges = db.prepare(
  `SELECT from_node_id AS fromNodeId, to_node_id AS toNodeId
   FROM dfg_edges
   WHERE chain_id = ? AND tx_hash = ?
   ORDER BY from_node_id, to_node_id`,
);

function canonicalize(nodes, edges) {
  const idToIndex = new Map();
  const visNodes = nodes.map((n, i) => {
    idToIndex.set(Number(n.nodeId), i);
    return { id: i, label: `${i}:${n.op}` };
  });

  const visEdges = [];
  for (const e of edges) {
    const from = idToIndex.get(Number(e.fromNodeId));
    const to = idToIndex.get(Number(e.toNodeId));
    if (from === undefined || to === undefined) continue;
    visEdges.push({ from, to, arrows: "to" });
  }
  return { nodes: visNodes, edges: visEdges };
}

const patterns = [];
for (const row of topSignatures) {
  const signature = String(row.signature);
  const txs = Number(row.txs);
  const sampleTxHash = row.samples?.[0]?.txHash ? String(row.samples[0].txHash) : null;
  if (!sampleTxHash) continue;

  const nodes = loadNodes.all(chainId, sampleTxHash);
  const edges = loadEdges.all(chainId, sampleTxHash);
  const graph = canonicalize(nodes, edges);

  patterns.push({
    signature,
    txs,
    nodeCount: graph.nodes.length,
    edgeCount: graph.edges.length,
    depthMax: row.depthStats?.max ?? null,
    sampleTxHash,
    graph,
  });
}

db.close();

const outPath = process.env.OUT_PATH ?? "/tmp/attacker_dfg_gallery.html";
const html = `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Attacker DFG Patterns</title>
    <style>
      :root { color-scheme: dark; }
      body { margin: 0; font-family: ui-sans-serif, system-ui, -apple-system; background: #0b0c10; color: #e7e9ee; }
      .wrap { display: grid; grid-template-columns: 420px 1fr; height: 100vh; }
      .left { padding: 16px; border-right: 1px solid #23252f; overflow: auto; }
      .right { padding: 16px; overflow: hidden; display: grid; grid-template-rows: auto 1fr; gap: 12px; }
      h1 { font-size: 16px; margin: 0 0 8px; }
      .meta { font-size: 12px; color: #aab0c0; margin-bottom: 12px; }
      .list { display: grid; gap: 8px; }
      .item { padding: 10px; border: 1px solid #23252f; border-radius: 10px; background: #11131a; cursor: pointer; }
      .item.active { outline: 2px solid #3b82f6; border-color: transparent; }
      .row { display: flex; justify-content: space-between; gap: 10px; }
      .sig { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
      .pill { font-size: 12px; background: #1b1f2a; border: 1px solid #23252f; padding: 2px 8px; border-radius: 999px; color: #cbd3e5; }
      a { color: #93c5fd; text-decoration: none; }
      a:hover { text-decoration: underline; }
      #network { width: 100%; height: calc(100vh - 110px); border: 1px solid #23252f; border-radius: 12px; background: #0f1117; }
      .topbar { display: flex; justify-content: space-between; align-items: center; gap: 10px; flex-wrap: wrap; }
      .topbar .sigfull { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; color: #cbd3e5; }
      .stats { font-size: 12px; color: #aab0c0; }
    </style>
    <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
  </head>
  <body>
    <div class="wrap">
      <div class="left">
        <h1>Attacker DFG patterns (top ${patterns.length})</h1>
        <div class="meta">
          Report: <span class="sig">${path.basename(reportPath)}</span><br/>
          DB: <span class="sig">${dbPath}</span><br/>
          Range: blocks ${report.range?.startBlock ?? "?"}..${report.range?.endBlock ?? "?"}<br/>
          Caller: <span class="sig">${report.caller ?? "?"}</span><br/>
          Attacker txs: ${report.attackerTxCount ?? "?"}
        </div>
        <div id="list" class="list"></div>
      </div>
      <div class="right">
        <div class="topbar">
          <div>
            <div class="sigfull" id="sig"></div>
            <div class="stats" id="stats"></div>
          </div>
          <div class="stats">
            Sample tx: <a id="tx" target="_blank" rel="noreferrer"></a>
          </div>
        </div>
        <div id="network"></div>
      </div>
    </div>
    <script>
      const patterns = ${JSON.stringify(patterns)};
      const list = document.getElementById('list');
      const sigEl = document.getElementById('sig');
      const statsEl = document.getElementById('stats');
      const txEl = document.getElementById('tx');
      const container = document.getElementById('network');
      let network = null;

      function renderIndex(active) {
        list.innerHTML = '';
        patterns.forEach((p, idx) => {
          const div = document.createElement('div');
          div.className = 'item' + (idx === active ? ' active' : '');
          div.onclick = () => select(idx);
          div.innerHTML = \`
            <div class=\"row\">
              <div class=\"pill\">\${p.txs} txs</div>
              <div class=\"pill\">\${p.nodeCount}n / \${p.edgeCount}e</div>
              <div class=\"pill\">depth≤\${p.depthMax ?? '?'}</div>
            </div>
            <div class=\"sig\" title=\"\${p.signature}\">\${p.signature}</div>
          \`;
          list.appendChild(div);
        });
      }

      function select(idx) {
        const p = patterns[idx];
        renderIndex(idx);
        sigEl.textContent = 'signature: ' + p.signature;
        statsEl.textContent = \`txs=\${p.txs} nodes=\${p.nodeCount} edges=\${p.edgeCount} depthMax=\${p.depthMax ?? '?'}\`;
        txEl.textContent = p.sampleTxHash;
        txEl.href = \`http://localhost:4310/dfg/tx?chainId=${chainId}&txHash=\${p.sampleTxHash}\`;

        const data = {
          nodes: new vis.DataSet(p.graph.nodes),
          edges: new vis.DataSet(p.graph.edges),
        };
        const options = {
          layout: { hierarchical: { enabled: true, direction: 'LR', sortMethod: 'directed', levelSeparation: 220, nodeSpacing: 180 } },
          physics: false,
          nodes: { shape: 'box', font: { face: 'monospace', size: 12, color: '#e7e9ee' }, color: { background: '#11131a', border: '#3b82f6' } },
          edges: { color: { color: '#4b5563' }, arrows: { to: { enabled: true, scaleFactor: 0.6 } } },
          interaction: { hover: true, navigationButtons: true, keyboard: true },
        };
        network?.destroy();
        network = new vis.Network(container, data, options);
        network.fit({ animation: false });
      }

      renderIndex(0);
      select(0);
    </script>
  </body>
</html>`;

await fs.writeFile(outPath, html);
console.log(JSON.stringify({ wrote: outPath, reportPath, patterns: patterns.length }, null, 2));

