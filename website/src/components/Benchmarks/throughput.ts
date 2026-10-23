/*
 * The throughput grid of this crate's results: a filled table drained at several concurrencies,
 * on each database in the form it takes there. Every cell measures the same three loops as a
 * scenario of the comparison, so a cell becomes a scenario of the model, and each database and
 * form becomes one chart drawn the way the comparison is drawn. Prose is never written here: the
 * page passes every word.
 */

import type {Labels, Loaded, Measurement, Results, Scenario} from './model';
import {SQLX} from './sqlx';

/** One cell of the grid, as the benchmark recipe writes it. */
export interface ThroughputRow {
  database: string;
  form: string;
  // A cell of a plugin the crate ships, measured as one app in three variants.
  plugin?: string;
  // The batch size, or `null` for a handler of single deliveries.
  batch: number | null;
  workers: number;
  pool: number;
  rows: number;
  rounds: number;
  raw: Measurement;
  adapter?: Measurement;
  framework: Measurement;
  overhead_percent?: number;
  verdict?: string;
  adapter_overhead_percent?: number;
  adapter_verdict?: string;
  plugin_overhead_percent?: number;
  plugin_verdict?: string;
}

/** The words of the grid: templates with `{..}` placeholders, and the page's name for a form. */
export interface ThroughputLabels {
  // A chart's title, from `{database}` and `{form}`.
  group: string;
  single: string;
  // A batch handler, from `{size}`.
  batchOf: string;
  // The concurrency of a cell, from `{count}`.
  workers: string;
  forms?: Record<string, string>;
}

/** One chart of the grid: a database and a form, with the words its chart is titled by. */
export interface ThroughputGroup {
  title: string;
  labels: Labels;
  entry: Loaded;
}

const fill = (template: string, values: Record<string, string | number>): string =>
  template.replace(/\{(\w+)\}/g, (whole, key: string) =>
    key in values ? String(values[key]) : whole,
  );

function scenario(cell: ThroughputRow, words: ThroughputLabels): Scenario {
  const delivery =
    cell.batch === null ? words.single : fill(words.batchOf, {size: cell.batch});
  return {
    name: `${delivery}, ${fill(words.workers, {count: cell.workers})}`,
    unit: 'msg/s',
    plugin: cell.plugin,
    raw: cell.raw,
    adapter: cell.adapter,
    framework: cell.framework,
    overhead_percent: cell.overhead_percent,
    verdict: cell.verdict,
    adapter_overhead_percent: cell.adapter_overhead_percent,
    adapter_verdict: cell.adapter_verdict,
    plugin_overhead_percent: cell.plugin_overhead_percent,
    plugin_verdict: cell.plugin_verdict,
  };
}

/**
 * The grid's charts: one per database and form, in the order the document first names each,
 * every cell a scenario in the document's order. A chart is titled by its group in place of the
 * comparison's own title, and its run by its versions and date alone: the machine is the page's.
 */
export function throughputGroups(
  run: Pick<Results, 'schema' | 'crate' | 'crate_version' | 'core_version' | 'measured_at'>,
  cells: ThroughputRow[],
  words: ThroughputLabels,
  labels: Labels,
): ThroughputGroup[] {
  const groups = new Map<string, {title: string; scenarios: Scenario[]}>();
  for (const cell of cells) {
    const key = `${cell.database}\u0000${cell.form}`;
    const title = fill(words.group, {
      database: cell.database,
      form: words.forms?.[cell.form] ?? cell.form,
    });
    const group = groups.get(key) ?? {title, scenarios: []};
    group.scenarios.push(scenario(cell, words));
    groups.set(key, group);
  }
  return [...groups.values()].map(({title, scenarios}) => ({
    title,
    labels: {...labels, comparison: title, plugin: title},
    entry: {
      broker: SQLX,
      results: {
        schema: run.schema,
        crate: run.crate,
        crate_version: run.crate_version,
        core_version: run.core_version,
        measured_at: run.measured_at,
        environment: undefined,
        scenarios,
      },
    },
  }));
}
