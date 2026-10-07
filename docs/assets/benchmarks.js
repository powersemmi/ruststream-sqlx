/*
 * Renders the Benchmarks page from the document the crate publishes next to it,
 * `benchmarks/results.json`.
 *
 * Three tables. The scenario table is the wall clock: a raw sqlx loop and the service a user
 * writes, and the difference between them. The code table is what a message costs on the
 * service's thread in instructions and allocations, the raw loop's cost beside it, and what
 * starting the service cost once. The throughput table is messages per second for a filled table
 * drained on each database, at each worker count, single deliveries and batches.
 *
 * The figures are fetched in the reader's browser rather than written into the page. A
 * re-measurement rewrites one JSON document, and a table copied into three translated pages
 * would be stale from the moment the next run finished. The pages therefore carry prose and no
 * figures at all, so there is nothing left to drift.
 *
 * Prose is never written here. Every label the page shows travels as JSON on the container, so
 * each translated page controls its own wording.
 *
 * A document that does not load, or that declares a schema this page does not render, leaves a
 * line saying so: a broken publish is visible instead of silently blank.
 *
 * No dependency and no build step. The script is a no-op on every page without the containers.
 */

(() => {
  "use strict";

  // The schema this page renders. A later revision may retype a field, and rendering it as if it
  // were this one would print wrong numbers instead of no numbers.
  const SCHEMAS = [3];
  const TIMEOUT_MS = 8000;
  // Where the document sits when the page does not say. The English page is the one it sits
  // next to; a translated page carries the way back to it on the container.
  const DEFAULT_RESULTS = "results.json";

  // The machine and the build, in the order the schema documents the fields. Values are printed
  // as the document wrote them; the page adds no words of its own to them.
  const ENVIRONMENT = [
    ["machine", ["cpu", "architecture", "cpu_frequency", "cores", "memory", "memory_speed"]],
    ["os", ["os"]],
    ["broker", ["broker", "round_trip"]],
    ["build", ["rustc", "sqlx", "valgrind", "profile", "features", "rustflags"]],
  ];

  const text = (tag, value) => {
    const node = document.createElement(tag);
    node.textContent = value;
    return node;
  };

  async function load(url) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
    try {
      const response = await fetch(url, { signal: controller.signal });
      return response.ok ? await response.json() : null;
    } catch {
      return null;
    } finally {
      clearTimeout(timer);
    }
  }

  const number = (value, lang) =>
    typeof value === "number" ? value.toLocaleString(lang, { maximumFractionDigits: 1 }) : "-";

  // The best round, and the median round in parentheses.
  function side(measurement, unit, lang) {
    if (!measurement || typeof measurement.best !== "number") {
      return "-";
    }
    const best = number(measurement.best, lang) + (unit ? " " + unit : "");
    if (typeof measurement.median !== "number") {
      return best;
    }
    return best + " (" + number(measurement.median, lang) + ")";
  }

  // The honesty rule of the methodology, enforced where it is read: a difference smaller than the
  // run-to-run spread is a verdict, never a percentage. The run decides it and the document
  // carries the decision, so this only renders it.
  const overhead = (percent, verdict, labels) =>
    verdict === "indistinguishable"
      ? labels.indistinguishable
      : (percent >= 0 ? "+" : "") + percent + "%";

  function table(columns, rows) {
    const element = document.createElement("table");
    const head = element.createTHead().insertRow();
    for (const column of columns) {
      head.appendChild(text("th", column));
    }
    const body = element.createTBody();
    for (const cells of rows) {
      const row = body.insertRow();
      for (const cell of cells) {
        row.appendChild(text("td", cell));
      }
    }
    return element;
  }

  function scenarios(results, labels, lang) {
    return table(
      [labels.scenario, labels.raw, labels.framework, labels.overhead],
      results.scenarios.map((scenario) => [
        scenario.name,
        side(scenario.raw, scenario.unit, lang),
        side(scenario.framework, scenario.unit, lang),
        overhead(scenario.overhead_percent, scenario.verdict, labels),
      ]),
    );
  }

  function code(results, labels, lang) {
    return table(
      [
        labels.scenario,
        labels.instructions,
        labels.rawInstructions,
        labels.allocations,
        labels.rawAllocations,
        labels.cold,
      ],
      results.code.map((scenario) => [
        scenario.name,
        number(scenario.framework?.instructions, lang),
        number(scenario.raw?.instructions, lang),
        number(scenario.framework?.allocations, lang),
        number(scenario.raw?.allocations, lang),
        // Two numbers in one cell: what starting cost in instructions, and in allocations.
        scenario.cold
          ? number(scenario.cold.instructions, lang) + " / " + number(scenario.cold.allocations, lang)
          : "-",
      ]),
    );
  }

  function throughput(results, labels, lang) {
    return table(
      [
        labels.database,
        labels.delivery,
        labels.workers,
        labels.raw,
        labels.framework,
        labels.overhead,
      ],
      results.throughput.map((run) => [
        run.database + ", " + (labels.forms[run.form] || run.form),
        typeof run.batch === "number"
          ? labels.batchOf.replace("{size}", String(run.batch))
          : labels.single,
        String(run.workers),
        side(run.raw, "msg/s", lang),
        side(run.framework, "msg/s", lang),
        overhead(run.overhead_percent, run.verdict, labels),
      ]),
    );
  }

  function environment(results, labels) {
    const values = results.environment || {};
    const element = document.createElement("table");
    const body = element.createTBody();
    const row = (label, value) => {
      const line = body.insertRow();
      line.appendChild(text("th", label));
      line.appendChild(text("td", value));
    };
    for (const [label, fields] of ENVIRONMENT) {
      // `unknown` is how the document writes a field the machine does not publish, and a bare
      // "unknown" in a list of values reads as a value. Leaving it out says the same thing.
      const parts = fields.map((field) => values[field]).filter((v) => v && v !== "unknown");
      if (parts.length) {
        row(labels[label], parts.join(", "));
      }
    }
    row(
      labels.versions,
      results.crate + " " + results.crate_version + ", ruststream " + results.core_version,
    );
    row(labels.measured, results.measured_at);
    return element;
  }

  async function main() {
    const container = document.getElementById("benchmark-results");
    if (!container) {
      return;
    }
    const machine = document.getElementById("benchmark-environment");
    const sections = [
      [document.getElementById("benchmark-code"), "code", code],
      [document.getElementById("benchmark-throughput"), "throughput", throughput],
    ];
    const lang = document.documentElement.lang || "en";
    const labels = JSON.parse(container.dataset.benchmarkLabels);
    const url = container.dataset.benchmarkResults || DEFAULT_RESULTS;
    const unavailable = () =>
      labels.unavailable.replace("{url}", new URL(url, location.href).href);
    for (const element of [container, machine, ...sections.map(([element]) => element)]) {
      element?.replaceChildren(text("p", labels.loading));
    }

    const results = await load(url);
    const decline = (message) => {
      container.replaceChildren(text("p", message));
      machine?.replaceChildren();
      for (const [element] of sections) {
        element?.replaceChildren(text("p", message));
      }
    };
    if (!results) {
      decline(unavailable());
      return;
    }
    if (!SCHEMAS.includes(results.schema)) {
      decline(labels.unknownSchema.replace("{schema}", String(results.schema)));
      return;
    }
    if (results.scenarios?.length) {
      container.replaceChildren(scenarios(results, labels, lang));
    } else {
      container.replaceChildren(text("p", unavailable()));
    }
    machine?.replaceChildren(environment(results, labels));
    // Each section is written by a run of its own, so one can be published while another is not.
    for (const [element, field, render] of sections) {
      if (results[field]?.length) {
        element?.replaceChildren(render(results, labels, lang));
      } else {
        element?.replaceChildren(text("p", unavailable()));
      }
    }
  }

  // Material swaps page content without a reload, so the tables are built on every navigation
  // rather than once per document.
  if (window.document$) {
    window.document$.subscribe(main);
  } else {
    document.addEventListener("DOMContentLoaded", main);
  }
})();
