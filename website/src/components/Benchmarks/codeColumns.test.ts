import fs from 'node:fs';
import path from 'node:path';
import {describe, expect, it} from 'vitest';
import {codeColumns, type CountedScenario} from './codeColumns';

// The document as the benchmark recipe wrote it, read apart from the module under test.
const document = JSON.parse(
  fs.readFileSync(path.resolve(__dirname, '../../../../docs/benchmarks/results.json'), 'utf8'),
) as {code: CountedScenario[]};

const count = (instructions: number, allocations: number) => ({instructions, allocations});

describe('the code counts beside what they are measured against', () => {
  it('sets each inbox scenario beside the raw loop running the same statements', () => {
    const {inbox, plugins} = codeColumns(
      [{name: 'consume', framework: count(49000.3, 32), raw: count(45401.1, 33)}],
      'en',
    );
    expect(inbox).toEqual([
      {
        scenario: 'consume',
        instructions: '49,000.3',
        rawInstructions: '45,401.1',
        allocations: '32',
        rawAllocations: '33',
      },
    ]);
    expect(plugins).toEqual([]);
  });

  it('sets a plugin scenario in its three variants: none, by hand and the crate', () => {
    const {inbox, plugins} = codeColumns(
      [
        {
          name: 'round trip',
          plugin: 'outbox',
          framework: count(81278.4, 42.1),
          adapter: count(75960.7, 39.1),
          raw: count(7751.6, 6),
        },
      ],
      'en',
    );
    expect(inbox).toEqual([]);
    expect(plugins).toEqual([
      {
        scenario: 'round trip',
        instructions: '7,751.6 / 75,960.7 / 81,278.4',
        allocations: '6 / 39.1 / 42.1',
      },
    ]);
  });

  it('marks a variant the document does not carry, as a plugin scenario with nothing to write by hand', () => {
    const {plugins} = codeColumns(
      [{name: 'untracked', plugin: 'outbox', framework: count(7238.2, 6), raw: count(6788.2, 6)}],
      'en',
    );
    expect(plugins[0]).toMatchObject({instructions: '6,788.2 / - / 7,238.2', allocations: '6 / - / 6'});
  });

  it('writes the numbers in the page language', () => {
    const {inbox} = codeColumns(
      [{name: 'consume', framework: count(49000.3, 32), raw: count(45401.1, 33)}],
      'ru',
    );
    expect(inbox[0]!.instructions).toBe((49000.3).toLocaleString('ru', {maximumFractionDigits: 1}));
  });

  it('keeps every scenario of the published document, each in its table', () => {
    const {inbox, plugins} = codeColumns(document.code, 'en');
    expect([...inbox, ...plugins].map((row) => row.scenario).sort()).toEqual(
      document.code.map((scenario) => scenario.name).sort(),
    );
  });
});
