const yearEl = document.getElementById("year");
if (yearEl) yearEl.textContent = String(new Date().getFullYear());

const revealEls = [...document.querySelectorAll(".reveal")];
const observer = new IntersectionObserver(
  (entries) => {
    for (const entry of entries) {
      if (!entry.isIntersecting) continue;
      const delay = Number(entry.target.getAttribute("data-delay") || 0);
      window.setTimeout(() => entry.target.classList.add("show"), delay);
      observer.unobserve(entry.target);
    }
  },
  { threshold: 0.12 }
);

for (const el of revealEls) observer.observe(el);

const matrix = document.querySelector(".matrix");
if (matrix) {
  for (const row of matrix.querySelectorAll("tbody tr")) {
    const cells = [...row.querySelectorAll("td")];
    if (cells.length < 2) continue;
    const rowLabel = cells[0].textContent.trim().toLowerCase();
    if (rowLabel === "best fit") continue;
    for (const cell of cells.slice(1)) {
      const value = cell.textContent.trim().toLowerCase();
      if (value === "yes" || value === "anywhere" || value.startsWith("online checkpoints")) {
        cell.classList.add("matrix-good");
      } else if (
        value === "partial" ||
        value === "limited" ||
        value === "basic" ||
        value === "provider tooling" ||
        value === "offline checkpoints"
      ) {
        cell.classList.add("matrix-warn");
      } else if (value === "no" || value === "cloud only") {
        cell.classList.add("matrix-bad");
      }
    }
  }
}
