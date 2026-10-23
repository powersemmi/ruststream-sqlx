import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * A retry on a queue table: the row itself counts its deliveries in its attempt column and keeps
 * its delay in its retry_after column, so a returned row is the same row, and the delay outlives a
 * restart. At the declared cap the row moves to the dead letter instead.
 */
export function retries(words: Words): {boxes: Box[]; links: Link[]} {
  const wide = (id: string, y: number, kind: Box['kind']): Box => ({
    id,
    x: 12,
    y,
    w: 336,
    h: 44,
    kind,
    lines: lines(words, id),
  });
  const boxes: Box[] = [
    wide('row', 0, 'broker'),
    wide('outcome', 72, 'service'),
    wide('cap', 144, 'core'),
    {id: 'returns', x: 12, y: 220, w: 160, h: 74, kind: 'step', lines: lines(words, 'returns')},
    {
      id: 'deadLetter',
      x: 188,
      y: 220,
      w: 160,
      h: 74,
      kind: 'bad',
      lines: lines(words, 'deadLetter'),
    },
    {id: 'restart', x: 0, y: 316, w: 360, h: 44, kind: 'note', lines: lines(words, 'restart')},
  ];
  const along = (x: number): number => (x + 80 - 12) / 336;
  const links: Link[] = [
    {from: 'row', to: 'outcome', label: word(words, 'handled')},
    {from: 'outcome', to: 'cap', label: word(words, 'retries')},
    {from: 'cap', to: 'returns', exitAt: along(12), label: word(words, 'below')},
    {from: 'cap', to: 'deadLetter', exitAt: along(188), label: word(words, 'atCap')},
  ];
  return {boxes, links};
}
