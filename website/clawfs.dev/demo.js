// demo.js — Interactive demo controller for ClawFS website
// Loads ClawFS WASM (compiled from clawfs-wasm-demo/) when present and falls
// back to a local simulator when the generated bundle is not available.

function estimateCompression(data) {
  const bytes = new TextEncoder().encode(data).length;
  if (bytes === 0) {
    return { original_bytes: 0, stored_bytes: 0, ratio: 1, codec: "Raw" };
  }

  const unique = new Set(data).size;
  const repetition = Math.max(0, 1 - unique / Math.min(data.length || 1, 64));
  const looksStructured = /[\{\}\[\]\":,_\n]/.test(data);
  const compressibility = Math.min(0.45, repetition * 0.35 + (looksStructured ? 0.18 : 0));
  const stored = Math.max(32, Math.round(bytes * (1 - compressibility)));
  const codec = stored < bytes ? "LZ4" : "Raw";

  return {
    original_bytes: bytes,
    stored_bytes: codec === "LZ4" ? stored : bytes,
    ratio: codec === "LZ4" ? stored / bytes : 1,
    codec,
  };
}

function fallbackComputeStorage(content) {
  return JSON.stringify(estimateCompression(content));
}

class FallbackFsDemo {
  constructor() {
    this.files = [];
    this.checkpoints = [];
    this._generation = 0;
  }

  add_file(path, content, now_ms) {
    const stats = estimateCompression(content);
    this.files = this.files.filter((f) => f.path !== path);
    this.files.push({
      path,
      original_bytes: stats.original_bytes,
      stored_bytes: stats.stored_bytes,
      codec: stats.codec,
      mtime_ms: now_ms,
    });
    return stats.stored_bytes;
  }

  add_file_sized(path, original_bytes, compressible, now_ms) {
    const stored_bytes = compressible ? Math.round(original_bytes * 0.55) : original_bytes;
    this.files = this.files.filter((f) => f.path !== path);
    this.files.push({
      path,
      original_bytes,
      stored_bytes,
      codec: compressible ? "LZ4" : "Raw",
      mtime_ms: now_ms,
    });
  }

  checkpoint(label) {
    this._generation += 1;
    this.checkpoints.push({
      generation: this._generation,
      label,
      files: this.files.map((f) => ({ ...f })),
    });
    return this._generation;
  }

  restore(gen) {
    const cp = this.checkpoints.find((item) => item.generation === gen);
    if (cp) this.files = cp.files.map((f) => ({ ...f }));
  }

  clear_files() {
    this.files = [];
  }

  reset() {
    this.files = [];
    this.checkpoints = [];
    this._generation = 0;
  }

  get_tree_json() {
    return JSON.stringify(this.files);
  }

  get_checkpoints_json() {
    return JSON.stringify(
      this.checkpoints.map((cp) => ({
        generation: cp.generation,
        label: cp.label,
        file_count: cp.files.length,
        total_original: cp.files.reduce((sum, f) => sum + f.original_bytes, 0),
        total_stored: cp.files.reduce((sum, f) => sum + f.stored_bytes, 0),
      }))
    );
  }

  get_totals_json() {
    const total_original = this.files.reduce((sum, f) => sum + f.original_bytes, 0);
    const total_stored = this.files.reduce((sum, f) => sum + f.stored_bytes, 0);
    return JSON.stringify({
      file_count: this.files.length,
      total_original,
      total_stored,
      ratio: total_original === 0 ? 1 : total_stored / total_original,
    });
  }

  generation() {
    return this._generation;
  }

  free() {}
}

let compute_storage = fallbackComputeStorage;
let FsDemo = FallbackFsDemo;

// ─── WASM bootstrap ──────────────────────────────────────────────────────────
let wasmReady = false;

async function loadWasm() {
  const status = document.getElementById("wasm-status");
  try {
    status.style.display = "block";
    const moduleUrl = "./wasm/clawfs_wasm_demo.js";
    const probe = await fetch(moduleUrl, { method: "GET" });
    const contentType = probe.headers.get("content-type") || "";
    if (!probe.ok || !/(javascript|ecmascript|wasm)/i.test(contentType)) {
      throw new Error(`missing or invalid module asset (${probe.status || "unknown"} ${contentType || "no content-type"})`);
    }
    const wasm = await import(moduleUrl);
    await wasm.default();
    compute_storage = wasm.compute_storage;
    FsDemo = wasm.FsDemo;
    wasmReady = true;
    status.style.display = "none";
    initDemos();
  } catch (e) {
    console.warn("ClawFS demo falling back to JS simulator:", e);
    wasmReady = false;
    status.innerHTML =
      `<span style="color:var(--ink-soft)">Using built-in simulator. Build <code>website/clawfs.dev/wasm/</code> to enable the Rust/WASM demo bundle.</span>`;
    window.setTimeout(() => {
      status.style.display = "none";
    }, 1600);
    initDemos();
  }
}

loadWasm();

// ─── Tab switching ────────────────────────────────────────────────────────────
document.querySelectorAll(".tab-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
    document.querySelectorAll(".tab-panel").forEach((p) => p.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById(btn.dataset.tab).classList.add("active");
  });
});

// ─── Helpers ─────────────────────────────────────────────────────────────────
function fmtBytes(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

function fmtTime(seconds) {
  if (seconds < 60) return `${seconds}s`;
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return s ? `${m}m ${s}s` : `${m}m`;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function appendLine(termEl, text, cls = "") {
  const d = document.createElement("div");
  d.className = "line" + (cls ? " " + cls : "");
  d.textContent = text;
  termEl.appendChild(d);
  termEl.scrollTop = termEl.scrollHeight;
}

function clearTerm(termEl) {
  termEl.innerHTML = "";
}

function renderStatBox(val, lbl, valClass = "") {
  return `<div class="stat-box"><div class="val ${valClass}">${val}</div><div class="lbl">${lbl}</div></div>`;
}

function renderFileTree(files, emptyMsg = "No files.") {
  if (!files || files.length === 0) {
    return `<div style="color:var(--ink-soft);font-size:.8rem;padding:.5rem">${emptyMsg}</div>`;
  }
  const dirs = {};
  files.forEach((f) => {
    const parts = f.path.split("/");
    const dir = parts.length > 1 ? parts[0] : ".";
    if (!dirs[dir]) dirs[dir] = [];
    dirs[dir].push(f);
  });
  let html = "";
  Object.keys(dirs)
    .sort()
    .forEach((dir) => {
      if (dir !== ".") {
        html += `<div class="entry"><span class="fdir">📁 ${dir}/</span></div>`;
      }
      dirs[dir].forEach((f) => {
        const fname = dir === "." ? f.path : f.path.slice(dir.length + 1);
        const savedPct =
          f.original_bytes > 0
            ? Math.round((1 - f.stored_bytes / f.original_bytes) * 100)
            : 0;
        const codecBadge =
          f.codec !== "Raw" ? `<span class="fcodec">${f.codec} −${savedPct}%</span>` : "";
        html += `<div class="entry">
          <span class="fname">&nbsp;&nbsp;${fname}${codecBadge}</span>
          <span class="fsize">${fmtBytes(f.original_bytes)}</span>
        </div>`;
      });
    });
  return html;
}

// ─── INIT ─────────────────────────────────────────────────────────────────────
function initDemos() {
  initColdStart();
  initRedTeamAudit();
  initPipeline();
}

// ══════════════════════════════════════════════════════════════════════════════
// TAB 1: Cold Start Tax
// ══════════════════════════════════════════════════════════════════════════════

const ARCHETYPES = {
  ml: {
    label: "Python ML",
    steps: [
      { label: "git clone ml-research-repo", time: 14, type: "dim" },
      { label: "pip install -r requirements.txt (torch, transformers, numpy…)", time: 142, type: "warn" },
      { label: "Downloading model weights from HuggingFace (4.7 GB)…", time: 312, type: "warn" },
      { label: "Building native extensions (sentencepiece, tokenizers)…", time: 38, type: "warn" },
      { label: "Warming embedding cache…", time: 24, type: "dim" },
      { label: "✓ Agent environment ready", time: 0, type: "ok" },
    ],
    warmSteps: [
      { label: "Mounting ClawFS volume vol-ml-workspace (gen 14)…", time: 0, type: "dim" },
      { label: "✓ Volume mounted at /workspace (1.2 s)", time: 0, type: "ok" },
      { label: "✓ .venv/ present — skipping pip install", time: 0, type: "ok" },
      { label: "✓ model weights cached — skipping download", time: 0, type: "ok" },
      { label: "✓ Agent environment ready", time: 0, type: "ok" },
    ],
    totalColdSecs: 530,
    warmSecs: 2,
    files: [
      { path: ".venv/lib", label: ".venv/ (Python packages)", bytes: 1_800_000_000, compressible: true },
      { path: ".cache/huggingface/hub", label: ".cache/huggingface/ (model weights)", bytes: 4_700_000_000, compressible: false },
      { path: "repo/src", label: "repo/ (source code)", bytes: 82_000_000, compressible: true },
      { path: "repo/data", label: "data/ (training samples)", bytes: 340_000_000, compressible: true },
    ],
  },
  node: {
    label: "Node.js App",
    steps: [
      { label: "git clone agent-api-service", time: 8, type: "dim" },
      { label: "npm ci (847 packages)…", time: 94, type: "warn" },
      { label: "Building TypeScript (tsc)…", time: 22, type: "warn" },
      { label: "Generating OpenAPI client stubs…", time: 11, type: "warn" },
      { label: "Seeding local dev database…", time: 18, type: "warn" },
      { label: "✓ Agent environment ready", time: 0, type: "ok" },
    ],
    warmSteps: [
      { label: "Mounting ClawFS volume vol-node-workspace (gen 8)…", time: 0, type: "dim" },
      { label: "✓ Volume mounted at /workspace (0.9 s)", time: 0, type: "ok" },
      { label: "✓ node_modules/ present — skipping npm ci", time: 0, type: "ok" },
      { label: "✓ dist/ present — skipping tsc build", time: 0, type: "ok" },
      { label: "✓ Agent environment ready", time: 0, type: "ok" },
    ],
    totalColdSecs: 153,
    warmSecs: 1,
    files: [
      { path: "node_modules/.bin", label: "node_modules/ (npm packages)", bytes: 420_000_000, compressible: true },
      { path: "dist/src", label: "dist/ (compiled output)", bytes: 18_000_000, compressible: true },
      { path: "repo/src", label: "src/ (source code)", bytes: 24_000_000, compressible: true },
      { path: ".cache/stubs", label: ".cache/ (generated stubs)", bytes: 12_000_000, compressible: true },
    ],
  },
  rust: {
    label: "Rust Project",
    steps: [
      { label: "git clone rust-agent-toolkit", time: 10, type: "dim" },
      { label: "cargo build --release (312 crates)…", time: 284, type: "warn" },
      { label: "Building proc-macro crates…", time: 42, type: "warn" },
      { label: "Linking final binary…", time: 8, type: "warn" },
      { label: "✓ Agent environment ready", time: 0, type: "ok" },
    ],
    warmSteps: [
      { label: "Mounting ClawFS volume vol-rust-workspace (gen 22)…", time: 0, type: "dim" },
      { label: "✓ Volume mounted at /workspace (1.1 s)", time: 0, type: "ok" },
      { label: "✓ target/ present — cargo build skipped (fingerprints current)", time: 0, type: "ok" },
      { label: "✓ Agent environment ready", time: 0, type: "ok" },
    ],
    totalColdSecs: 344,
    warmSecs: 1,
    files: [
      { path: "target/release/deps", label: "target/ (compiled artifacts)", bytes: 2_400_000_000, compressible: true },
      { path: "~/.cargo/registry", label: "~/.cargo/registry/ (crate cache)", bytes: 680_000_000, compressible: true },
      { path: "repo/src", label: "src/ (source code)", bytes: 38_000_000, compressible: true },
    ],
  },
};

function initColdStart() {
  let currentArch = "ml";
  let running = false;
  let animHandle = null;

  const archBtns = document.querySelectorAll(".arch-btn");
  archBtns.forEach((btn) => {
    btn.addEventListener("click", () => {
      if (running) return;
      archBtns.forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      currentArch = btn.dataset.arch;
      resetCold();
    });
  });

  document.getElementById("btn-cold-start").addEventListener("click", () => {
    if (running) return;
    runColdDemo(ARCHETYPES[currentArch]);
  });

  document.getElementById("btn-cold-reset").addEventListener("click", resetCold);

  function resetCold() {
    if (animHandle) clearTimeout(animHandle);
    running = false;
    clearTerm(document.getElementById("cold-term"));
    clearTerm(document.getElementById("warm-term"));
    document.getElementById("cold-stats").innerHTML = "";
    document.getElementById("time-saved-banner").style.display = "none";
    document.getElementById("cold-filetree").innerHTML =
      `<div style="color:var(--ink-soft);font-size:.8rem;padding:.5rem">Run the simulation to see what gets cached…</div>`;
    document.getElementById("cold-storage-stats").innerHTML = "";
    document.getElementById("btn-cold-start").disabled = false;
    document.getElementById("btn-cold-reset").disabled = true;
  }

  async function runColdDemo(arch) {
    running = true;
    document.getElementById("btn-cold-start").disabled = true;
    document.getElementById("btn-cold-reset").disabled = false;

    const coldTerm = document.getElementById("cold-term");
    const warmTerm = document.getElementById("warm-term");
    clearTerm(coldTerm);
    clearTerm(warmTerm);

    appendLine(coldTerm, `$ # Starting fresh worker — ${arch.label}`, "prompt");
    appendLine(warmTerm, `$ # Starting fresh worker — ${arch.label} (with ClawFS)`, "prompt");
    await sleep(200);

    // Animate cold start steps
    let coldElapsed = 0;
    for (const step of arch.steps) {
      appendLine(coldTerm, `$ ${step.label}`, step.type);
      if (step.time > 0) {
        appendLine(coldTerm, `  ⏳ ${fmtTime(step.time)}…`, "dim");
        coldElapsed += step.time;
      }
      await sleep(280);
    }

    appendLine(coldTerm, ``, "");
    appendLine(coldTerm, `Total bootstrap time: ${fmtTime(arch.totalColdSecs)}`, "err");
    appendLine(coldTerm, `Agent starts working at t+${fmtTime(arch.totalColdSecs)}`, "dim");

    document.getElementById("cold-stats").innerHTML =
      renderStatBox(fmtTime(arch.totalColdSecs), "bootstrap time", "red") +
      renderStatBox("every run", "repeated cost", "red");

    // Animate warm start steps
    await sleep(300);
    appendLine(warmTerm, `$ # ClawFS volume already exists from previous run`, "dim");
    await sleep(150);

    for (const step of arch.warmSteps) {
      appendLine(warmTerm, `$ ${step.label}`, step.type);
      await sleep(180);
    }

    appendLine(warmTerm, ``, "");
    appendLine(warmTerm, `Total bootstrap time: ${fmtTime(arch.warmSecs)}`, "ok");
    appendLine(warmTerm, `Agent starts working at t+${fmtTime(arch.warmSecs)}`, "ok");

    // Show time saved
    const saved = arch.totalColdSecs - arch.warmSecs;
    const banner = document.getElementById("time-saved-banner");
    banner.style.display = "block";
    document.getElementById("time-saved-val").textContent = fmtTime(saved);

    // Compute file tree using WASM
    const fsDemo = new FsDemo();
    const now = Date.now();
    const storageStats = [];

    for (const f of arch.files) {
      fsDemo.add_file_sized(f.path, f.bytes, f.compressible, now);
      const sampleContent = f.compressible
        ? "x".repeat(Math.min(f.bytes, 4096))
        : new Array(64).fill(0).map(() => Math.random().toString(36)).join("");
      const stats = JSON.parse(compute_storage(sampleContent));
      storageStats.push({ ...f, ...stats });
    }

    const treeData = JSON.parse(fsDemo.get_tree_json());
    document.getElementById("cold-filetree").innerHTML = renderFileTree(treeData);

    const totals = JSON.parse(fsDemo.get_totals_json());
    document.getElementById("cold-storage-stats").innerHTML =
      renderStatBox(fmtBytes(totals.total_original), "original size") +
      renderStatBox(fmtBytes(totals.total_stored), "stored on ClawFS") +
      renderStatBox(
        Math.round((1 - totals.ratio) * 100) + "%",
        "space saved by LZ4",
        "green"
      );

    fsDemo.free();
    running = false;
  }
}

// ══════════════════════════════════════════════════════════════════════════════
// TAB 2: Recurring Red Team Audit
// ══════════════════════════════════════════════════════════════════════════════

const REDTEAM_RUNS = [
  {
    label: "Baseline audit · week 1",
    domain: "https://acmebank.example",
    started_at: "2026-03-01T02:00:00Z",
    findings: [
      { id: "RT-101", severity: "high", title: "Admin portal exposed in robots.txt", path: "/robots.txt" },
      { id: "RT-102", severity: "medium", title: "Missing content-security-policy header", path: "/login" },
      { id: "RT-103", severity: "medium", title: "Password policy page missing MFA language", path: "/legal/password-policy" },
      { id: "RT-104", severity: "low", title: "Staging JS bundle references internal hostname", path: "/assets/app.js" },
    ],
    artifacts: [
      { path: "snapshots/week-1/crawl/index.json", bytes: 540_000, compressible: true },
      { path: "snapshots/week-1/headers/login.json", bytes: 18_000, compressible: true },
      { path: "findings/week-1/findings.json", bytes: 24_000, compressible: true },
      { path: "notes/week-1/compliance.md", bytes: 11_000, compressible: true },
      { path: "diffs/week-1/baseline-summary.md", bytes: 7_000, compressible: true },
    ],
    steps: [
      "$ clawfs mount vol-acme-redteam /workspace",
      "$ python audit_agent.py --target https://acmebank.example --profile weekly",
      "  → crawl 842 URLs and normalize route inventory",
      "  → capture response headers for auth, legal, and account surfaces",
      "  → diff exposed paths against last approved scope (none yet)",
      "  → write findings/week-1/findings.json",
      "  → write notes/week-1/compliance.md",
    ],
    diffSummary:
      "Baseline stored. The agent now has a reusable route inventory, header captures, and a compliance notebook to diff against future deploys.",
    stats: {
      routes: 842,
      findings: 4,
      compliance: "2 gaps noted",
      changed: "baseline",
    },
    progressLabel: "Baseline audit completed · 5 artifacts committed to the volume",
  },
  {
    label: "Recurring audit · week 2 deploy",
    domain: "https://acmebank.example",
    started_at: "2026-03-08T02:00:00Z",
    findings: [
      { id: "RT-101", severity: "resolved", title: "robots.txt exposure removed", path: "/robots.txt" },
      { id: "RT-205", severity: "critical", title: "New /debug/graphql endpoint exposed without auth", path: "/debug/graphql" },
      { id: "RT-206", severity: "high", title: "Session cookie lost SameSite=strict after deploy", path: "/login" },
      { id: "RT-207", severity: "medium", title: "Privacy page changed; SOC2 retention statement removed", path: "/legal/privacy" },
    ],
    artifacts: [
      { path: "snapshots/week-2/crawl/index.json", bytes: 566_000, compressible: true },
      { path: "snapshots/week-2/headers/login.json", bytes: 19_000, compressible: true },
      { path: "findings/week-2/findings.json", bytes: 29_000, compressible: true },
      { path: "diffs/week-2/route-diff.json", bytes: 8_000, compressible: true },
      { path: "reports/week-2/executive-summary.md", bytes: 13_000, compressible: true },
    ],
    steps: [
      "$ clawfs mount vol-acme-redteam /workspace",
      "$ python audit_agent.py --target https://acmebank.example --profile weekly --resume-history",
      "  → load previous crawl graph and finding ledger from /workspace",
      "  → detect 37 new routes and 12 changed headers since week 1",
      "  → flag /debug/graphql as net-new internet-exposed surface",
      "  → compare privacy/legal copy against stored compliance notes",
      "  → write diffs/week-2/route-diff.json and reports/week-2/executive-summary.md",
    ],
    diffSummary:
      "Week 2 diff surfaced one critical new attack surface, one cookie regression, and one compliance drift issue. The agent also marked the old robots.txt finding as resolved because the baseline history was still mounted.",
    stats: {
      routes: "842 → 879",
      findings: "3 open / 1 resolved",
      compliance: "retention statement drift",
      changed: "+37 routes",
    },
    progressLabel: "Recurring audit completed · old vs new deploy diffed from persistent history",
  },
];

function initRedTeamAudit() {
  let fsDemo = new FsDemo();
  let runIndex = 0;
  let running = false;

  const term = document.getElementById("redteam-term");
  const tree = document.getElementById("redteam-filetree");
  const stats = document.getElementById("redteam-stats");
  const progress = document.getElementById("redteam-progress");
  const progressLabel = document.getElementById("redteam-progress-label");
  const diffSummary = document.getElementById("redteam-diff-summary");
  const genBadge = document.getElementById("redteam-gen-badge");
  const btnBaseline = document.getElementById("btn-redteam-baseline");
  const btnRerun = document.getElementById("btn-redteam-rerun");
  const btnReset = document.getElementById("btn-redteam-reset");

  function renderTree() {
    tree.innerHTML = renderFileTree(JSON.parse(fsDemo.get_tree_json()), "No scans stored yet…");
  }

  function renderStats(run) {
    const totals = JSON.parse(fsDemo.get_totals_json());
    stats.innerHTML =
      renderStatBox(String(run.stats.routes), "routes tracked") +
      renderStatBox(String(run.stats.findings), "finding status") +
      renderStatBox(String(run.stats.compliance), "compliance drift") +
      renderStatBox(fmtBytes(totals.total_stored), "history stored", "green");
  }

  async function runAudit(run) {
    if (running) return;
    running = true;
    btnBaseline.disabled = true;
    btnRerun.disabled = true;
    progress.style.width = "8%";

    appendLine(term, `$ # ${run.label}`, "prompt");
    await sleep(120);

    for (const [idx, step] of run.steps.entries()) {
      const cls =
        step.startsWith("$") ? "prompt" :
        step.includes("flag") ? "err" :
        step.includes("write") ? "ok" : "dim";
      appendLine(term, step, cls);
      progress.style.width = `${Math.round(((idx + 1) / run.steps.length) * 72) + 8}%`;
      await sleep(180);
    }

    const now = Date.parse(run.started_at);
    for (const artifact of run.artifacts) {
      fsDemo.add_file_sized(artifact.path, artifact.bytes, artifact.compressible, now);
    }

    const findingsDoc = JSON.stringify({
      target: run.domain,
      audited_at: run.started_at,
      findings: run.findings,
    });
    fsDemo.add_file(`history/${run.label.toLowerCase().replace(/[^a-z0-9]+/g, "-")}.json`, findingsDoc, now + 5000);
    const gen = fsDemo.checkpoint(run.label);

    renderTree();
    renderStats(run);
    diffSummary.textContent = run.diffSummary;
    progress.style.width = "100%";
    progressLabel.textContent = run.progressLabel;
    genBadge.textContent = `gen ${gen} · ${JSON.parse(fsDemo.get_totals_json()).file_count} audit artifacts`;

    appendLine(term, `✓ Saved audit history to /workspace at gen ${gen}`, "ok");
    appendLine(term, `✓ Findings remain queryable for the next weekly scan`, "accent");
    appendLine(term, "", "");

    runIndex += 1;
    btnBaseline.disabled = true;
    btnRerun.disabled = runIndex >= REDTEAM_RUNS.length;
    running = false;
  }

  function resetRedTeam() {
    fsDemo.free();
    fsDemo = new FsDemo();
    runIndex = 0;
    running = false;
    clearTerm(term);
    tree.innerHTML = `<div style="color:var(--ink-soft);font-size:.8rem;padding:.5rem">No scans stored yet…</div>`;
    stats.innerHTML = "";
    progress.style.width = "0%";
    progressLabel.textContent = "No scans yet";
    diffSummary.textContent = "Run the baseline audit to build the first site snapshot and compliance notebook.";
    genBadge.textContent = "baseline only";
    btnBaseline.disabled = false;
    btnRerun.disabled = true;
  }

  btnBaseline.addEventListener("click", () => runAudit(REDTEAM_RUNS[0]));
  btnRerun.addEventListener("click", () => {
    if (runIndex < REDTEAM_RUNS.length) runAudit(REDTEAM_RUNS[runIndex]);
  });
  btnReset.addEventListener("click", resetRedTeam);
}

// ══════════════════════════════════════════════════════════════════════════════
// TAB 3: Agent Pipeline
// ══════════════════════════════════════════════════════════════════════════════

const LOG_SAMPLES = {
  nginx: {
    name: "nginx-access-2025-03-16.log",
    content: `192.168.1.10 - - [16/Mar/2025:03:14:01 +0000] "GET /api/v1/agents HTTP/1.1" 200 1284 "-" "ClawFS-Agent/1.0"
192.168.1.11 - - [16/Mar/2025:03:14:02 +0000] "POST /api/v1/volumes HTTP/1.1" 201 842 "-" "ClawFS-Agent/1.0"
192.168.1.12 - - [16/Mar/2025:03:14:03 +0000] "GET /api/v1/volumes/vol-abc HTTP/1.1" 200 3421 "-" "Mozilla/5.0"
192.168.1.10 - - [16/Mar/2025:03:14:05 +0000] "GET /api/v1/agents HTTP/1.1" 500 98 "-" "ClawFS-Agent/1.0"
192.168.1.13 - - [16/Mar/2025:03:14:06 +0000] "DELETE /api/v1/volumes/vol-xyz HTTP/1.1" 404 62 "-" "curl/8.1"
192.168.1.10 - - [16/Mar/2025:03:14:08 +0000] "GET /healthz HTTP/1.1" 200 12 "-" "-"
192.168.1.14 - - [16/Mar/2025:03:14:09 +0000] "POST /api/v1/checkpoint HTTP/1.1" 500 110 "-" "ClawFS-Agent/1.0"
192.168.1.11 - - [16/Mar/2025:03:14:10 +0000] "GET /api/v1/volumes HTTP/1.1" 200 9832 "-" "ClawFS-Agent/1.0"`,
    errors: 2, total: 8, p99_ms: 142,
  },
  app: {
    name: "app-errors-2025-03-16.log",
    content: `2025-03-16T03:14:01Z ERROR [agent-runner] Volume mount failed: timeout after 30s vol=vol-abc
2025-03-16T03:14:02Z WARN  [checkpoint-svc] Checkpoint stalled: object store latency p99=8420ms
2025-03-16T03:14:04Z ERROR [metadata-sync] Shard write conflict generation=14 client=worker-07
2025-03-16T03:14:07Z INFO  [agent-runner] Agent resumed from gen=13 files=47 bytes=2341882920
2025-03-16T03:14:08Z WARN  [segment-flush] Flush queue depth=842 — backpressure active
2025-03-16T03:14:09Z ERROR [journal] Journal replay error: segment 0x3f2a missing range 0-65536
2025-03-16T03:14:11Z INFO  [agent-runner] Checkpoint completed gen=14 duration=1.2s
2025-03-16T03:14:12Z WARN  [metadata-sync] Delta log merge took 4200ms — shard_id=7`,
    errors: 3, total: 8, p99_ms: 8420,
  },
  agent: {
    name: "agent-trace-2025-03-16.log",
    content: `[03:14:00] agent-planner START task="analyze quarterly report" model=claude-opus-4-6
[03:14:01] agent-planner TOOL filesystem.list_dir path=/workspace/reports
[03:14:01] agent-planner TOOL filesystem.read_file path=/workspace/reports/q4_2024.csv
[03:14:03] agent-planner TOOL filesystem.write_file path=/workspace/plan.md bytes=4821
[03:14:04] agent-executor START task="execute plan" parent=agent-planner
[03:14:04] agent-executor TOOL filesystem.read_file path=/workspace/plan.md
[03:14:06] agent-executor TOOL code.run lang=python timeout=60s
[03:14:08] agent-executor TOOL filesystem.write_file path=/workspace/outputs/analysis.json bytes=18423
[03:14:09] agent-executor ERROR code.run exit_code=1 stderr="ModuleNotFoundError: pandas"
[03:14:09] agent-executor RETRY installing missing dep: pandas
[03:14:11] agent-executor TOOL filesystem.write_file path=/workspace/outputs/analysis.json bytes=22841`,
    errors: 1, total: 11, p99_ms: 2100,
  },
};

function initPipeline() {
  let fsDemo = new FsDemo();
  let selectedLog = "nginx";
  let pipelineRun = 0;
  let running = false;

  const logChips = document.querySelectorAll(".log-chip");
  logChips.forEach((chip) => {
    chip.addEventListener("click", () => {
      logChips.forEach((c) => c.classList.remove("active"));
      chip.classList.add("active");
      selectedLog = chip.dataset.log;
    });
  });

  function setAgentState(id, state) {
    const card = document.getElementById(`agent-${id}`);
    const status = document.getElementById(`status-${id}`);
    card.className = "agent-card" + (state !== "idle" && state !== "waiting" ? ` ${state}` : "");
    const labels = {
      idle: "idle",
      waiting_logs: "waiting for logs…",
      waiting_analysis: "waiting for analysis…",
      waiting_aggregates: "waiting for aggregates…",
      processing: "⚙ processing…",
      done: "✓ done",
    };
    status.textContent = labels[state] || state;
  }

  function updatePipelineBadge() {
    const totals = JSON.parse(fsDemo.get_totals_json());
    const gen = fsDemo.generation();
    document.getElementById("pipeline-gen-badge").textContent =
      `gen ${gen} · ${totals.file_count} files · ${fmtBytes(totals.total_stored)} stored`;
  }

  function renderPipelineTree() {
    const files = JSON.parse(fsDemo.get_tree_json());
    const tree = document.getElementById("pipeline-filetree");
    tree.innerHTML = renderFileTree(files, "Volume is empty…");
    const totals = JSON.parse(fsDemo.get_totals_json());
    document.getElementById("pipeline-stats").innerHTML =
      renderStatBox(totals.file_count.toString(), "files in volume") +
      renderStatBox(fmtBytes(totals.total_original), "total data") +
      renderStatBox(fmtBytes(totals.total_stored), "stored (compressed)") +
      renderStatBox(Math.round((1 - totals.ratio) * 100) + "%", "compression savings", "green");
  }

  async function runPipeline(logKey) {
    if (running) return;
    running = true;
    document.getElementById("btn-inject-log").disabled = true;
    document.getElementById("btn-inject-more").disabled = true;
    pipelineRun++;
    const run = pipelineRun;
    const log = LOG_SAMPLES[logKey];
    const now = Date.now();
    const runPrefix = `run${run}`;

    // ── Stage 1: Ingest
    setAgentState("ingest", "processing");
    await sleep(400);

    const logPath = `logs/${runPrefix}-${log.name}`;
    fsDemo.add_file(logPath, log.content, now);
    fsDemo.checkpoint(`after ingest run ${run}`);
    renderPipelineTree();
    updatePipelineBadge();
    setAgentState("ingest", "done");
    setAgentState("analysis", "processing");

    await sleep(500);

    // ── Stage 2: Analysis
    const analysisData = JSON.stringify({
      source: logPath,
      total_lines: log.total,
      error_count: log.errors,
      warn_count: Math.floor(log.total * 0.25),
      p99_ms: log.p99_ms,
      error_rate_pct: ((log.errors / log.total) * 100).toFixed(1),
      top_errors: log.content
        .split("\n")
        .filter((l) => l.includes("ERROR") || l.includes("500") || l.includes("404"))
        .slice(0, 3),
      analyzed_at: new Date(now).toISOString(),
    });
    fsDemo.add_file(`analysis/${runPrefix}-${log.name}.analysis.json`, analysisData, now + 1000);
    fsDemo.checkpoint(`after analysis run ${run}`);
    renderPipelineTree();
    updatePipelineBadge();
    setAgentState("analysis", "done");
    setAgentState("aggregator", "processing");

    await sleep(500);

    // ── Stage 3: Aggregator
    const allFiles = JSON.parse(fsDemo.get_tree_json());
    const analysisPaths = allFiles
      .filter((f) => f.path.startsWith("analysis/") && f.path.endsWith(".json"))
      .map((f) => f.path);
    const summaryData = JSON.stringify({
      generated_at: new Date(now + 2000).toISOString(),
      runs_analyzed: run,
      total_log_lines: log.total * run,
      total_errors: log.errors * run,
      overall_error_rate_pct: ((log.errors / log.total) * 100).toFixed(1),
      avg_p99_ms: log.p99_ms,
      sources: analysisPaths,
    });
    fsDemo.add_file(`aggregates/summary-${runPrefix}.json`, summaryData, now + 2000);
    fsDemo.checkpoint(`after aggregate run ${run}`);
    renderPipelineTree();
    updatePipelineBadge();
    setAgentState("aggregator", "done");
    setAgentState("dashboard", "processing");

    await sleep(500);

    // ── Stage 4: Dashboard
    const errRate = ((log.errors / log.total) * 100).toFixed(1);
    const health = log.errors === 0 ? "healthy" : log.errors <= 2 ? "degraded" : "critical";
    const dashboardHtml = `<!doctype html><html><head><title>ClawFS Observability Dashboard</title></head><body>
<h1>Agent Pipeline Dashboard</h1>
<p>Generated: ${new Date(now + 3000).toISOString()}</p>
<h2>System Health: ${health.toUpperCase()}</h2>
<ul>
  <li>Runs analyzed: ${run}</li>
  <li>Total log lines: ${log.total * run}</li>
  <li>Error rate: ${errRate}%</li>
  <li>p99 latency: ${log.p99_ms}ms</li>
</ul>
<h2>Volume Stats</h2>
<p>${JSON.parse(fsDemo.get_totals_json()).file_count} files · ${fmtBytes(JSON.parse(fsDemo.get_totals_json()).total_stored)} stored</p>
</body></html>`;
    fsDemo.add_file(`dashboard/index.html`, dashboardHtml, now + 3000);
    fsDemo.checkpoint(`dashboard ready run ${run}`);
    renderPipelineTree();
    updatePipelineBadge();
    setAgentState("dashboard", "done");

    running = false;
    document.getElementById("btn-inject-log").disabled = false;
    document.getElementById("btn-inject-more").disabled = false;
  }

  function resetPipeline() {
    fsDemo.free();
    fsDemo = new FsDemo();
    pipelineRun = 0;
    running = false;

    ["ingest", "analysis", "aggregator", "dashboard"].forEach((id) => {
      const defaultStates = { ingest: "idle", analysis: "waiting_logs", aggregator: "waiting_analysis", dashboard: "waiting_aggregates" };
      setAgentState(id, defaultStates[id]);
    });

    document.getElementById("pipeline-filetree").innerHTML =
      `<div style="color:var(--ink-soft);font-size:.8rem;padding:.5rem">Volume is empty. Inject a log file to start the pipeline…</div>`;
    document.getElementById("pipeline-stats").innerHTML = "";
    document.getElementById("pipeline-gen-badge").textContent = "gen 0 · 0 files";
    document.getElementById("btn-inject-log").disabled = false;
    document.getElementById("btn-inject-more").disabled = true;
  }

  document.getElementById("btn-inject-log").addEventListener("click", () => runPipeline(selectedLog));
  document.getElementById("btn-inject-more").addEventListener("click", () => runPipeline(selectedLog));
  document.getElementById("btn-pipeline-reset").addEventListener("click", resetPipeline);
}
