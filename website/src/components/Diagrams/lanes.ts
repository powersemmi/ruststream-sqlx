import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The workers of the drawing, each running a claim, a handler and a settlement of its own.
export const WORKERS = 4;
const WIDTH = 75;
const workerX = (index: number): number => 12 + index * (WIDTH + 12);

/*
 * One subscription mounted with `workers(4)` on a pool of eight: a claim in flight for each free
 * worker, within the seven connections the subscription may hold, and one connection left for
 * what the handlers run of their own.
 */
export function lanes(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [
    {
      id: 'subscription',
      x: 12,
      y: 0,
      w: 336,
      h: 44,
      kind: 'core',
      lines: lines(words, 'subscription'),
    },
  ];
  for (let index = 0; index < WORKERS; index += 1) {
    boxes.push({
      id: `worker${index}`,
      x: workerX(index),
      y: 84,
      w: WIDTH,
      h: 58,
      kind: 'service',
      lines: lines(words, `worker${index}`),
    });
  }
  boxes.push(
    {id: 'pool', x: 12, y: 182, w: 336, h: 44, kind: 'broker', lines: lines(words, 'pool')},
    {id: 'oneClaim', x: 0, y: 250, w: 360, h: 58, kind: 'note', lines: lines(words, 'oneClaim')},
    {id: 'byKey', x: 0, y: 318, w: 360, h: 44, kind: 'note', lines: lines(words, 'byKey')},
  );
  const centre = (index: number): number => (workerX(index) + WIDTH / 2 - 12) / 336;
  const links: Link[] = [];
  for (let index = 0; index < WORKERS; index += 1) {
    links.push(
      {
        from: 'subscription',
        to: `worker${index}`,
        exitAt: centre(index),
        label: index === 0 ? word(words, 'claims') : undefined,
      },
      {from: `worker${index}`, to: 'pool', enterAt: centre(index)},
    );
  }
  return {boxes, links};
}
