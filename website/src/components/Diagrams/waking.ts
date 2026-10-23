import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The claim loop on the left, standing off the edge so the loop back to the claim has a gutter;
// what ends a wait early on the right.
const LOOP_X = 40;
const LOOP_W = 140;
const SOURCE_X = 200;
const SOURCE_W = 148;

// What ends a subscription's wait, top to bottom, and where each one meets the wait box.
export const SOURCES = ['publish', 'reconnect', 'notification'] as const;

/*
 * A subscription's claim loop. A claim that filled its limit claims again at once; one that
 * found fewer rows waits the poll interval, which a publish of the same broker, a Postgres
 * notification or a listening connection that came back cuts short. A writer outside RustStream
 * wakes the subscriptions of other processes by notifying the table's channel.
 */
export function waking(words: Words): {boxes: Box[]; links: Link[]} {
  const loop = (id: string, y: number, h: number, kind: Box['kind']): Box => ({
    id,
    x: LOOP_X,
    y,
    w: LOOP_W,
    h,
    kind,
    lines: lines(words, id),
  });
  const source = (id: string, y: number, kind: Box['kind'], href?: string): Box => ({
    id,
    x: SOURCE_X,
    y,
    w: SOURCE_W,
    h: 58,
    kind,
    lines: lines(words, id),
    href,
  });
  const boxes: Box[] = [
    loop('claim', 0, 58, 'broker'),
    loop('full', 100, 44, 'step'),
    loop('wait', 200, 58, 'core'),
    source('publish', 0, 'service', '/architecture/path/'),
    source('reconnect', 86, 'broker'),
    source('notification', 172, 'broker'),
    source('writer', 270, 'service'),
    {id: 'floor', x: 0, y: 352, w: 360, h: 44, kind: 'note', lines: lines(words, 'floor')},
  ];
  const links: Link[] = [
    {from: 'claim', to: 'full', exitAt: 0.35, enterAt: 0.35},
    {from: 'full', to: 'claim', exitAt: 0.8, enterAt: 0.8, label: word(words, 'filled')},
    {from: 'full', to: 'wait', exitAt: 0.35, enterAt: 0.35, label: word(words, 'fewer')},
    {from: 'wait', to: 'claim', exit: 'left', enter: 'left'},
    ...SOURCES.map(
      (id, index): Link => ({
        from: id,
        to: 'wait',
        exit: 'left',
        enter: 'right',
        enterAt: 0.2 + 0.3 * index,
      }),
    ),
    {from: 'writer', to: 'notification', label: word(words, 'commit')},
  ];
  return {boxes, links};
}
