// GoFlow dashboard. Vanilla JS, no build step, no framework: it polls the
// same REST API any other client would use and renders the result.

const POLL_INTERVAL_MS = 2000;

const statGrid = document.getElementById("stat-grid");
const workersBody = document.querySelector("#workers-table tbody");
const jobsBody = document.querySelector("#jobs-table tbody");
const healthPill = document.getElementById("health-pill");
const healthText = document.getElementById("health-text");
const workersCount = document.getElementById("workers-count");
const jobsCount = document.getElementById("jobs-count");

const STAT_CARDS = [
  { key: "total", label: "Total Jobs", cls: "" },
  { key: "pending", label: "Pending", cls: "" },
  { key: "running", label: "Running", cls: "accent" },
  { key: "completed", label: "Completed", cls: "green" },
  { key: "failed", label: "Failed", cls: "red" },
  { key: "retrying", label: "Retrying", cls: "amber" },
  { key: "dead", label: "Dead", cls: "red" },
  { key: "queue_size", label: "Queue Size", cls: "purple" },
  { key: "active_workers", label: "Active Workers", cls: "accent" },
];

function renderStats(stats) {
  statGrid.innerHTML = STAT_CARDS.map(({ key, label, cls }) => `
    <div class="stat-card ${cls}">
      <div class="value">${stats[key] ?? 0}</div>
      <div class="label">${label}</div>
    </div>
  `).join("");

  if (stats.avg_processing_ms) {
    const avgCard = document.createElement("div");
    avgCard.className = "stat-card";
    avgCard.innerHTML = `
      <div class="value">${Math.round(stats.avg_processing_ms)}ms</div>
      <div class="label">Avg Processing Time</div>
    `;
    statGrid.appendChild(avgCard);
  }
}

function fmtTime(iso) {
  if (!iso) return "&mdash;";
  const d = new Date(iso);
  return d.toLocaleTimeString();
}

function renderWorkers(workers) {
  workersCount.textContent = `${workers.length} registered`;
  workersBody.innerHTML = workers.map(w => `
    <tr>
      <td>${w.id}</td>
      <td><span class="badge ${w.status}">${w.status}</span></td>
      <td>${w.current_job_id ? w.current_job_id.slice(0, 14) + "&hellip;" : "&mdash;"}</td>
      <td>${w.jobs_processed}</td>
      <td>${w.jobs_failed}</td>
      <td>${fmtTime(w.last_heartbeat)}</td>
    </tr>
  `).join("");
}

function renderJobs(jobs) {
  jobsCount.textContent = `${jobs.length} shown`;
  jobsBody.innerHTML = jobs.slice(0, 25).map(j => `
    <tr>
      <td title="${j.id}">${j.id.slice(0, 16)}&hellip;</td>
      <td>${j.type}</td>
      <td>${j.priority}</td>
      <td><span class="badge ${j.status}">${j.status}</span></td>
      <td>${j.retry_count}/${j.max_retries}</td>
      <td>${fmtTime(j.created_at)}</td>
    </tr>
  `).join("");
}

async function pollOnce() {
  try {
    const [statsRes, jobsRes, workersRes, healthRes] = await Promise.all([
      fetch("/api/stats"),
      fetch("/api/jobs?limit=25"),
      fetch("/api/workers"),
      fetch("/api/health"),
    ]);

    if (statsRes.ok) renderStats(await statsRes.json());
    if (jobsRes.ok) renderJobs(await jobsRes.json());
    if (workersRes.ok) renderWorkers(await workersRes.json());

    if (healthRes.ok) {
      healthPill.className = "status-pill ok";
      healthText.textContent = "healthy";
    } else {
      throw new Error("unhealthy");
    }
  } catch (err) {
    healthPill.className = "status-pill down";
    healthText.textContent = "unreachable";
  }
}

document.getElementById("submit-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const form = e.target;
  const feedback = document.getElementById("submit-feedback");
  const type = form.type.value;
  const priority = form.priority.value;
  const maxRetries = parseInt(form.max_retries.value, 10);
  const payloadText = document.getElementById("job-payload").value.trim();

  let payload;
  try {
    payload = payloadText ? JSON.parse(payloadText) : defaultPayloadFor(type);
  } catch (err) {
    feedback.textContent = "Invalid JSON payload";
    feedback.style.color = "var(--red)";
    return;
  }

  try {
    const res = await fetch("/api/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ type, priority, max_retries: maxRetries, payload }),
    });
    const body = await res.json();
    if (!res.ok) throw new Error(body.error || "submission failed");
    feedback.textContent = `Submitted ${body.id}`;
    feedback.style.color = "var(--green)";
    pollOnce();
  } catch (err) {
    feedback.textContent = err.message;
    feedback.style.color = "var(--red)";
  }
});

function defaultPayloadFor(type) {
  switch (type) {
    case "email":
      return { to: "user@example.com", subject: "Hello", body: "Sent from the GoFlow dashboard" };
    case "report_generation":
      return { report_name: "weekly-summary", rows: 250 };
    case "data_processing":
      return { dataset: "orders", operation: "aggregate" };
    case "image_processing":
      return { image_url: "https://example.com/photo.jpg", resize: "800x600" };
    case "webhook":
      return { url: "https://example.com/hook", method: "POST" };
    default:
      return {};
  }
}

document.getElementById("job-type").addEventListener("change", (e) => {
  document.getElementById("job-payload").placeholder =
    JSON.stringify(defaultPayloadFor(e.target.value));
});
document.getElementById("job-payload").placeholder = JSON.stringify(defaultPayloadFor("email"));

pollOnce();
setInterval(pollOnce, POLL_INTERVAL_MS);
