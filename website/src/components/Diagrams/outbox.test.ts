import {expect, it} from 'vitest';
import {outbox} from './outbox';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('records before it sends, and runs the handler only for a record still unprocessed', () => {
  const {links} = outbox(words);
  const from = (id: string): string[] =>
    links.filter((link) => link.from === id).map((link) => link.to);
  expect(from('producer')).toEqual(['record']);
  expect(from('record')).toEqual(['broker']);
  // The startup republish reaches the consumer through the same broker, with the record's id.
  expect(from('republish')).toEqual(['broker']);
  expect(from('fetch')).toEqual(['processed', 'consumer']);
  expect(from('processed')).toEqual([]);
  expect(from('consumer')).toEqual(['mark', 'waits']);
});
