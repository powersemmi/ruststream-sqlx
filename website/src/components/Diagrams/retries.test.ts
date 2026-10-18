import {expect, it} from 'vitest';
import {retries} from './retries';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('lets the attempt cap choose between returning the row and its dead letter', () => {
  const {links} = retries(words);
  const from = (id: string): string[] =>
    links.filter((link) => link.from === id).map((link) => link.to);
  expect(from('row')).toEqual(['outcome']);
  expect(from('outcome')).toEqual(['cap']);
  expect(from('cap')).toEqual(['returns', 'deadLetter']);
  // Nothing is published: both ends are statements on the same table.
  expect([...from('returns'), ...from('deadLetter')]).toEqual([]);
});
