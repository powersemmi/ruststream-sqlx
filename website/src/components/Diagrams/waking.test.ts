import {expect, it} from 'vitest';
import {SOURCES, waking} from './waking';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('ends a wait early only through the sources it names, and loops every wait back to a claim', () => {
  const {links} = waking(words);
  const into = (id: string): string[] =>
    links.filter((link) => link.to === id).map((link) => link.from);
  expect(into('wait').sort()).toEqual(['full', ...SOURCES].sort());
  expect(into('claim').sort()).toEqual(['full', 'wait']);
  // A writer outside the broker reaches a subscription through a notification alone.
  expect(links.filter((link) => link.from === 'writer').map((link) => link.to)).toEqual([
    'notification',
  ]);
});
