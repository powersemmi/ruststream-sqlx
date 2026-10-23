import {expect, it} from 'vitest';
import {table} from './table';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('takes a row into work only from the due rows, and settles it into each state an outcome names', () => {
  const {links} = table(words);
  const into = (id: string): string[] =>
    links.filter((link) => link.to === id).map((link) => link.from);
  const from = (id: string): string[] =>
    links.filter((link) => link.from === id).map((link) => link.to);
  expect(from('write')).toEqual(['scheduled', 'due']);
  expect(into('inWork')).toEqual(['due']);
  expect(from('inWork').sort()).toEqual(['deadLetter', 'due', 'finished', 'scheduled']);
  // A scheduled row becomes due when its time comes; it is never claimed before.
  expect(from('scheduled')).toEqual(['due']);
});
