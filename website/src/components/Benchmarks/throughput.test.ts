import fs from 'node:fs';
import path from 'node:path';
import {describe, expect, it} from 'vitest';
import {tabContent, type Labels} from './model';
import {SQLX} from './sqlx';
import {throughputGroups, type ThroughputLabels, type ThroughputRow} from './throughput';

// The document as the benchmark recipe wrote it, read apart from the module under test.
const document = JSON.parse(
  fs.readFileSync(path.resolve(__dirname, '../../../../docs/benchmarks/results.json'), 'utf8'),
) as {throughput: ThroughputRow[]};

// Every word is its own key, so the tests read the rules and never a page's wording. The words
// are an object of their own: the model copies them for a plugin's chart.
const labels = Object.fromEntries(
  [
    'loading',
    'scenario',
    'raw',
    'adapter',
    'framework',
    'overhead',
    'against',
    'indistinguishable',
    'brokerBound',
    'measured',
    'details',
    'pending',
    'instructions',
    'allocations',
    'cold',
    'table',
    'axis',
    'rounds',
    'best',
    'median',
    'noise',
    'comparison',
    'code',
    'broker',
    'plugin',
    'pluginRaw',
    'pluginAdapter',
    'pluginFramework',
    'pluginAxis',
    'pluginAgainst',
    'pluginOverhead',
    'pluginOverHand',
  ].map((key) => [key, key]),
) as Labels;
const words: ThroughputLabels = {
  group: '{database} / {form}',
  single: 'single',
  batchOf: 'batch {size}',
  workers: 'workers({count})',
};

const measured = (best: number) => ({best, median: best, worst: best});
const row = (fields: Partial<ThroughputRow>): ThroughputRow => ({
  database: 'Postgres',
  form: 'row lock',
  batch: null,
  workers: 1,
  pool: 16,
  rows: 20000,
  rounds: 3,
  raw: measured(100),
  adapter: measured(99),
  framework: measured(98),
  overhead_percent: 2,
  verdict: 'measured',
  adapter_overhead_percent: 1,
  adapter_verdict: 'indistinguishable',
  ...fields,
});

const results = {schema: 3, crate: 'ruststream-sqlx', crate_version: '0.7.0', core_version: '0.7.1', measured_at: '2026-10-18'};

describe('the throughput grid', () => {
  it('groups the cells by database and form, in the order the document first names each', () => {
    const groups = throughputGroups(
      results,
      [
        row({database: 'Postgres', form: 'row lock', workers: 1}),
        row({database: 'SQLite', form: 'lease', workers: 1}),
        row({database: 'Postgres', form: 'row lock', workers: 4}),
      ],
      words,
      labels,
    );
    expect(groups.map((group) => group.title)).toEqual(['Postgres / row lock', 'SQLite / lease']);
    expect(groups[0]!.entry.results!.scenarios!.map((scenario) => scenario.name)).toEqual([
      'single, workers(1)',
      'single, workers(4)',
    ]);
  });

  it('names a cell by its delivery and its workers, in the page words', () => {
    const [group] = throughputGroups(results, [row({batch: 64, workers: 8})], words, labels);
    expect(group!.entry.results!.scenarios![0]!.name).toBe('batch 64, workers(8)');
  });

  it('turns each cell into a scenario of the model, its three loops and verdicts as measured', () => {
    const cell = row({workers: 4});
    const [group] = throughputGroups(results, [cell], words, labels);
    const [scenario] = group!.entry.results!.scenarios!;
    expect(scenario).toMatchObject({
      unit: 'msg/s',
      raw: cell.raw,
      adapter: cell.adapter,
      framework: cell.framework,
      overhead_percent: 2,
      verdict: 'measured',
      adapter_overhead_percent: 1,
      adapter_verdict: 'indistinguishable',
    });
    // The run is named by its versions and its date; the machine is the page's own section.
    expect(group!.entry.results).toMatchObject({...results, environment: undefined});
    expect(group!.entry.broker).toBe(SQLX);
  });

  it('keeps a plugin cell a plugin scenario, so the chart names its loops as the plugin variants', () => {
    const cell = row({form: 'outbox round trip', plugin: 'outbox', plugin_overhead_percent: -1.5, plugin_verdict: 'indistinguishable'});
    const [group] = throughputGroups(results, [cell], words, labels);
    expect(group!.entry.results!.scenarios![0]).toMatchObject({
      plugin: 'outbox',
      plugin_overhead_percent: -1.5,
      plugin_verdict: 'indistinguishable',
    });
    // The model titles a plugin's chart with the plugin label: the group's title takes its place.
    expect(group!.labels.plugin).toBe('Postgres / outbox round trip');
    expect(tabContent(group!.entry, 'en', group!.labels).comparison[0]!.title).toBe(
      'Postgres / outbox round trip',
    );
  });

  it('titles a broker group with its database and form', () => {
    const [group] = throughputGroups(results, [row({})], words, labels);
    expect(tabContent(group!.entry, 'en', group!.labels).comparison[0]!.title).toBe(
      'Postgres / row lock',
    );
  });

  it('translates a form through the page words, and keeps a form the page has no word for', () => {
    const groups = throughputGroups(
      results,
      [row({form: 'row lock'}), row({form: 'lease', database: 'SQLite'})],
      {...words, forms: {'row lock': 'verrou'}},
      labels,
    );
    expect(groups.map((group) => group.title)).toEqual(['Postgres / verrou', 'SQLite / lease']);
  });

  it('draws every cell of the published document', () => {
    const groups = throughputGroups(results, document.throughput, words, labels);
    expect(groups.flatMap((group) => group.entry.results!.scenarios!)).toHaveLength(
      document.throughput.length,
    );
  });
});
