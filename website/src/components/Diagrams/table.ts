import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The two columns of row states: the rows that wait, and the rows the queue works through.
const LEFT = 12;
const RIGHT = 198;
const COLUMN = 150;

/*
 * One queue table and the states of its rows. A publish or the service's own insert writes a
 * row, due now or later; a claim takes due rows into work in claim order; the outcome finishes the
 * row, returns it at once or later, or moves it to the dead letter. The roles of the columns sit at
 * the foot of the table.
 */
export function table(words: Words): {boxes: Box[]; links: Link[]} {
  const state = (id: string, x: number, y: number, kind: Box['kind'], href?: string): Box => ({
    id,
    x,
    y,
    w: COLUMN,
    h: 58,
    kind,
    lines: lines(words, id),
    href,
  });
  const boxes: Box[] = [
    {id: 'write', x: 12, y: 0, w: 336, h: 44, kind: 'service', lines: lines(words, 'write')},
    {id: 'table', x: 0, y: 70, w: 360, h: 390, kind: 'group', lines: lines(words, 'table')},
    state('scheduled', LEFT, 96, 'step'),
    state('due', RIGHT, 96, 'step'),
    state('inWork', RIGHT, 195, 'core', '/architecture/queues/claims/'),
    state('deadLetter', LEFT, 294, 'bad', '/architecture/settlement/'),
    state('finished', RIGHT, 294, 'good', '/architecture/settlement/'),
    {id: 'roles', x: 12, y: 374, w: 336, h: 74, kind: 'broker', lines: lines(words, 'roles')},
    {id: 'optional', x: 0, y: 472, w: 360, h: 44, kind: 'note', lines: lines(words, 'optional')},
  ];
  // Where along the write box a link leaves, straight above the column it enters.
  const above = (x: number): number => (x + COLUMN / 2 - 12) / 336;
  const links: Link[] = [
    {from: 'write', to: 'scheduled', exitAt: above(LEFT), label: word(words, 'later')},
    {from: 'write', to: 'due', exitAt: above(RIGHT), label: word(words, 'now')},
    {from: 'scheduled', to: 'due'},
    {from: 'due', to: 'inWork', exitAt: 0.3, enterAt: 0.3, label: word(words, 'claim')},
    {from: 'inWork', to: 'due', exitAt: 0.75, enterAt: 0.75, label: word(words, 'retry')},
    {
      from: 'inWork',
      to: 'scheduled',
      exit: 'left',
      enter: 'bottom',
      exitAt: 0.3,
      label: word(words, 'retryAfter'),
    },
    {
      from: 'inWork',
      to: 'deadLetter',
      exit: 'left',
      enter: 'top',
      exitAt: 0.7,
      label: word(words, 'spent'),
    },
    {from: 'inWork', to: 'finished', label: word(words, 'ack')},
  ];
  return {boxes, links};
}
