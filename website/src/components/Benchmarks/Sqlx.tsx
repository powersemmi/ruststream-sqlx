import type {ReactNode} from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {codeColumns, type CountedScenario} from './codeColumns';
import {Results} from './index';
import type {Labels} from './model';
import {sqlx} from './sqlx';
import {throughputGroups, type ThroughputLabels, type ThroughputRow} from './throughput';

/*
 * The sections of this crate's Benchmarks page the shared charts do not draw: the throughput
 * grid, one chart per database and form drawn as the comparison is, and the code counts beside
 * the loops they are measured against.
 */

// The sections this crate's document carries beside those of the model.
const extra = sqlx.results as unknown as {throughput?: ThroughputRow[]; code?: CountedScenario[]};

function useLang(): string {
  const {i18n} = useDocusaurusContext();
  return i18n.localeConfigs[i18n.currentLocale]?.htmlLang ?? i18n.currentLocale;
}

/** The throughput grid, a chart per database and form, from the document's `throughput`. */
export function Throughput({
  labels,
  words,
  details,
}: {
  labels: Labels;
  words: ThroughputLabels;
  details: string;
}): ReactNode {
  const groups = throughputGroups(sqlx.results!, extra.throughput ?? [], words, labels);
  return (
    <>
      {groups.map((group) => (
        <Results key={group.title} entry={group.entry} labels={group.labels} details={details} />
      ))}
    </>
  );
}

/** The words of the two count tables, keyed by column. */
export type CodeLabels = Record<
  | 'scenario'
  | 'instructions'
  | 'rawInstructions'
  | 'allocations'
  | 'rawAllocations'
  | 'pluginInstructions'
  | 'pluginAllocations',
  string
>;

/** The inbox's counts beside the raw loop, and the outbox's beside no outbox and one by hand. */
export function CodeCounts({labels}: {labels: CodeLabels}): ReactNode {
  const {inbox, plugins} = codeColumns(extra.code ?? [], useLang());
  return (
    <>
      <table>
        <thead>
          <tr>
            <th>{labels.scenario}</th>
            <th>{labels.instructions}</th>
            <th>{labels.rawInstructions}</th>
            <th>{labels.allocations}</th>
            <th>{labels.rawAllocations}</th>
          </tr>
        </thead>
        <tbody>
          {inbox.map((row) => (
            <tr key={row.scenario}>
              <td>{row.scenario}</td>
              <td>{row.instructions}</td>
              <td>{row.rawInstructions}</td>
              <td>{row.allocations}</td>
              <td>{row.rawAllocations}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {plugins.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>{labels.scenario}</th>
              <th>{labels.pluginInstructions}</th>
              <th>{labels.pluginAllocations}</th>
            </tr>
          </thead>
          <tbody>
            {plugins.map((row) => (
              <tr key={row.scenario}>
                <td>{row.scenario}</td>
                <td>{row.instructions}</td>
                <td>{row.allocations}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}
