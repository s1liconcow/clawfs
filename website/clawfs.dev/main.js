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

const navToggle = document.querySelector(".nav-toggle");
const siteNav = document.getElementById("site-nav");

if (navToggle && siteNav) {
  const closeNav = () => {
    navToggle.setAttribute("aria-expanded", "false");
    navToggle.setAttribute("aria-label", "Open navigation menu");
    siteNav.classList.remove("is-open");
    document.body.classList.remove("nav-open");
  };

  navToggle.addEventListener("click", () => {
    const isOpen = navToggle.getAttribute("aria-expanded") === "true";
    if (isOpen) {
      closeNav();
      return;
    }
    navToggle.setAttribute("aria-expanded", "true");
    navToggle.setAttribute("aria-label", "Close navigation menu");
    siteNav.classList.add("is-open");
    document.body.classList.add("nav-open");
  });

  for (const link of siteNav.querySelectorAll("a")) {
    link.addEventListener("click", closeNav);
  }

  window.addEventListener("resize", () => {
    if (window.innerWidth > 760) closeNav();
  });
}
