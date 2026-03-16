// demo.js — Interactive demo controller for ClawFS website
// Loads ClawFS WASM (compiled from clawfs-wasm-demo/) and drives the three tab demos.

import init, { compute_storage, FsDemo } from "./wasm/clawfs_wasm_demo.js";

// ─── WASM bootstrap ──────────────────────────────────────────────────────────
let wasmReady = false;

async function loadWasm() {
  const status = document.getElementById("wasm-status");
  try {
    status.style.display = "block";
    await init();
    wasmReady = true;
    status.style.display = "none";
    initDemos();
  } catch (e) {
    status.innerHTML = `<span style="color:#e85c5c">⚠ Could not load WebAssembly: ${e.message}. Try a modern browser.</span>`;
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
        const fname = f.path.split("/").pop();
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
  initCrashRecovery();
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
// TAB 2: Crash Recovery
// ══════════════════════════════════════════════════════════════════════════════

const RESEARCH_FILES = [
  { path: "papers/arxiv_dump.jsonl",        bytes: 48_000_000,  compressible: true },
  { path: "papers/embeddings_batch_1.npy",  bytes: 180_000_000, compressible: false },
  { path: "papers/embeddings_batch_2.npy",  bytes: 180_000_000, compressible: false },
  { path: "analysis/patterns_v1.json",      bytes: 1_200_000,   compressible: true },
  { path: "analysis/patterns_v2.json",      bytes: 1_800_000,   compressible: true },
  { path: "analysis/clusters.json",         bytes: 3_400_000,   compressible: true },
  { path: "workspace/context.md",           bytes: 22_000,      compressible: true },
  { path: "workspace/scratchpad.md",        bytes: 8_400,       compressible: true },
  { path: "workspace/agent_state.json",     bytes: 140_000,     compressible: true },
  { path: "outputs/summary_draft_v1.md",    bytes: 48_000,      compressible: true },
  { path: "outputs/summary_draft_v2.md",    bytes: 62_000,      compressible: true },
  { path: "papers/embeddings_batch_3.npy",  bytes: 180_000_000, compressible: false },
  { path: "analysis/cross_ref.json",        bytes: 4_200_000,   compressible: true },
  { path: "workspace/iteration_log.jsonl",  bytes: 280_000,     compressible: true },
  { path: "outputs/final_insights.md",      bytes: 94_000,      compressible: true },
];

function initCrashRecovery() {
  let fsDemo = new FsDemo();
  let running = false;
  let crashed = false;
  let currentGen = 0;
  let fileIdx = 0;
  let animHandle = null;

  const crashTerm = document.getElementById("crash-term");
  const crashTree = document.getElementById("crash-filetree");

  function renderCrashTree(files) {
    crashTree.innerHTML = renderFileTree(files, "No files written yet…");
  }

  function updateProgress(count, totalBytes) {
    const pct = Math.round((count / RESEARCH_FILES.length) * 100);
    document.getElementById("crash-progress").style.width = pct + "%";
    document.getElementById("crash-progress-label").textContent =
      `${count} files written · ${fmtBytes(totalBytes)} accumulated`;

    const totals = JSON.parse(fsDemo.get_totals_json());
    document.getElementById("crash-stats").innerHTML =
      renderStatBox(count.toString(), "files in volume") +
      renderStatBox(fmtBytes(totals.total_original), "accumulated") +
      renderStatBox(fmtBytes(totals.total_stored), "stored (compressed)");
  }

  function renderTimeline() {
    const cps = JSON.parse(fsDemo.get_checkpoints_json());
    const container = document.getElementById("crash-timeline");
    container.innerHTML = `<div class="cp-node locked" data-gen="0"><div class="cp-dot">0</div><div class="cp-lbl">start</div></div>`;
    cps.forEach((cp) => {
      container.innerHTML += `<span class="cp-arrow">→</span>
        <div class="cp-node locked" data-gen="${cp.generation}">
          <div class="cp-dot">G${cp.generation}</div>
          <div class="cp-lbl">${cp.label}<br>${cp.file_count} files</div>
        </div>`;
    });
  }

  async function runAgent() {
    running = true;
    crashed = false;
    document.getElementById("btn-crash-run").disabled = true;
    document.getElementById("btn-crash-crash").disabled = false;

    appendLine(crashTerm, `$ clawfs mount vol-research-2025 /workspace`, "prompt");
    await sleep(200);
    appendLine(crashTerm, `✓ Mounted at /workspace (gen ${currentGen})`, "ok");
    await sleep(150);
    appendLine(crashTerm, `$ python research_agent.py --resume`, "prompt");
    await sleep(200);

    const dot = document.getElementById("crash-dot");
    const workerLabel = document.getElementById("crash-worker-label");
    dot.className = "dot-green";
    workerLabel.textContent = "Research Agent — running";

    const now = Date.now();

    while (fileIdx < RESEARCH_FILES.length && !crashed) {
      const f = RESEARCH_FILES[fileIdx];
      const content = f.compressible
        ? JSON.stringify({ file: f.path, data: "x".repeat(Math.min(f.bytes, 2048)) })
        : new Array(32).fill(0).map(() => Math.random().toString(36)).join("");

      fsDemo.add_file(f.path, content, now + fileIdx * 1000);
      fileIdx++;

      const fname = f.path.split("/").pop();
      appendLine(crashTerm, `  → writing ${fname} (${fmtBytes(f.bytes)})`, "dim");

      const files = JSON.parse(fsDemo.get_tree_json());
      renderCrashTree(files);
      const totalBytes = files.reduce((s, x) => s + x.original_bytes, 0);
      updateProgress(fileIdx, totalBytes);

      // Auto-checkpoint every 5 files
      if (fileIdx % 5 === 0) {
        const gen = fsDemo.checkpoint(`~${fmtBytes(totalBytes)}`);
        currentGen = gen;
        appendLine(crashTerm, `  ✓ checkpoint gen ${gen}`, "ok");
        renderTimeline();
      }

      await sleep(350);
    }

    if (!crashed) {
      appendLine(crashTerm, `✓ Research complete. All outputs written.`, "ok");
      running = false;
      document.getElementById("btn-crash-crash").disabled = true;
      document.getElementById("btn-crash-reset").style.display = "inline-block";
    }
  }

  function crash() {
    crashed = true;
    running = false;
    const dot = document.getElementById("crash-dot");
    const workerLabel = document.getElementById("crash-worker-label");
    dot.className = "dot-red";
    workerLabel.textContent = "Research Agent — CRASHED";

    appendLine(crashTerm, ``, "");
    appendLine(crashTerm, `FATAL: worker preempted by OOM killer`, "err");
    appendLine(crashTerm, `Process terminated. All in-memory state lost.`, "err");
    appendLine(crashTerm, ``, "");
    appendLine(crashTerm, `Without ClawFS: 0 of ${fileIdx} files recoverable.`, "err");
    appendLine(crashTerm, `With ClawFS:    volume persisted — resume from gen ${currentGen}.`, "ok");

    // Grey out the tree
    const tree = document.getElementById("crash-filetree");
    tree.style.opacity = "0.4";

    document.getElementById("btn-crash-crash").disabled = true;
    document.getElementById("btn-crash-resume").disabled = false;
  }

  async function resume() {
    document.getElementById("btn-crash-resume").disabled = true;

    const dot = document.getElementById("crash-dot");
    const workerLabel = document.getElementById("crash-worker-label");
    dot.className = "dot-yellow";
    workerLabel.textContent = "Research Agent — resuming…";

    appendLine(crashTerm, ``, "");
    appendLine(crashTerm, `$ # New worker starting up…`, "prompt");
    await sleep(300);
    appendLine(crashTerm, `$ clawfs mount vol-research-2025 /workspace`, "prompt");
    await sleep(200);
    appendLine(crashTerm, `✓ Mounted at /workspace — restoring gen ${currentGen}`, "ok");
    await sleep(150);

    fsDemo.restore(currentGen);
    const files = JSON.parse(fsDemo.get_tree_json());
    document.getElementById("crash-filetree").style.opacity = "1";
    renderCrashTree(files);
    const totalBytes = files.reduce((s, x) => s + x.original_bytes, 0);
    fileIdx = files.length;
    updateProgress(fileIdx, totalBytes);

    appendLine(crashTerm, `✓ ${files.length} files restored from checkpoint (gen ${currentGen})`, "ok");
    appendLine(crashTerm, `$ python research_agent.py --resume`, "prompt");
    await sleep(200);
    appendLine(crashTerm, `  ↩ Resuming from last checkpoint — continuing work…`, "accent");

    dot.className = "dot-green";
    workerLabel.textContent = "Research Agent — running (resumed)";

    // Continue from where we left off
    await sleep(400);
    await runAgent();

    document.getElementById("btn-crash-reset").style.display = "inline-block";
  }

  function resetCrash() {
    if (animHandle) clearTimeout(animHandle);
    fsDemo.free();
    fsDemo = new FsDemo();
    running = false;
    crashed = false;
    currentGen = 0;
    fileIdx = 0;

    clearTerm(crashTerm);
    document.getElementById("crash-dot").className = "dot-yellow";
    document.getElementById("crash-worker-label").textContent = "Research Agent — idle";
    document.getElementById("crash-progress").style.width = "0%";
    document.getElementById("crash-progress-label").textContent = "0 files written · 0 MB accumulated";
    document.getElementById("crash-stats").innerHTML = "";
    document.getElementById("crash-filetree").style.opacity = "1";
    crashTree.innerHTML = `<div style="color:var(--ink-soft);font-size:.8rem;padding:.5rem">No files yet…</div>`;
    document.getElementById("crash-timeline").innerHTML = `
      <div class="cp-node active" data-gen="0"><div class="cp-dot">0</div><div class="cp-lbl">empty</div></div>`;
    document.getElementById("btn-crash-run").disabled = false;
    document.getElementById("btn-crash-crash").disabled = true;
    document.getElementById("btn-crash-resume").disabled = true;
    document.getElementById("btn-crash-reset").style.display = "none";
  }

  document.getElementById("btn-crash-run").addEventListener("click", runAgent);
  document.getElementById("btn-crash-crash").addEventListener("click", crash);
  document.getElementById("btn-crash-resume").addEventListener("click", resume);
  document.getElementById("btn-crash-reset").addEventListener("click", resetCrash);
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
