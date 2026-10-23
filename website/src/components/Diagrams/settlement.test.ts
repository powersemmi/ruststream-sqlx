import {expect, it} from 'vitest';
import {ENDS, settlement} from './settlement';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('rolls back before the statement, and runs the statement where each form holds the row', () => {
  const {links} = settlement(words);
  const from = (id: string): string[] =>
    links.filter((link) => link.from === id).map((link) => link.to);
  expect(from('outcome')).toEqual(['rollback']);
  expect(from('rollback')).toEqual(['statement']);
  expect(from('statement')).toEqual([...ENDS]);
});
