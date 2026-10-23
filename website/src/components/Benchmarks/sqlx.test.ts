import fs from 'node:fs';
import path from 'node:path';
import {describe, expect, it} from 'vitest';
import {SCHEMAS, tabContent, type Labels, type Results} from './model';
import {SQLX, sqlx} from './sqlx';

// The document as the benchmark recipe wrote it, read apart from the module under test, so a
// test compares what the page shows with what the file says.
const document = JSON.parse(
  fs.readFileSync(path.resolve(__dirname, '../../../../docs/benchmarks/results.json'), 'utf8'),
) as Results;

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

describe("this crate's results", () => {
  it('are the document committed beside the pages, under the catalog entry of this crate', () => {
    expect(sqlx.broker).toBe(SQLX);
    expect(SQLX.repo).toBe('ruststream-sqlx');
    expect(sqlx.results).toEqual(document);
  });

  it('declare a schema the model renders', () => {
    expect(SCHEMAS).toContain(sqlx.results!.schema);
  });

  it('draw the inbox and the outbox apart, each scenario in the order the document measured it', () => {
    const {comparison, code, pending} = tabContent(sqlx, 'en', labels);
    expect(comparison.map((group) => group.plugin)).toEqual([null, 'outbox']);
    expect(comparison.flatMap((group) => group.rows.map((row) => row.scenario))).toEqual(
      document.scenarios!.map((scenario) => scenario.name),
    );
    expect(code!.rows.map((row) => row.scenario)).toEqual(
      document.code!.map((scenario) => scenario.name),
    );
    expect(pending).toBeNull();
  });
});
