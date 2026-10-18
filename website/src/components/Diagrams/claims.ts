import type {Box, Lane, Link} from './flow';
import {lines, word, type Words} from './words';

// The three forms side by side, each a column from its claim to its settlement.
export const FORMS = ['rowLock', 'lease', 'advisory'] as const;
export type Form = (typeof FORMS)[number];

const COLUMN = 104;
const columnX = (index: number): number => 12 + index * (COLUMN + 12);
const centre = (index: number): number => columnX(index) + COLUMN / 2;

const TOP = 28;
const STEP = 88;
const H = 74;

// The steps of each form, by the row of the drawing they stand in: the handler and the
// settlement share a row across the forms, so the three columns read side by side.
export const STEPS: Record<Form, [string, number][]> = {
  rowLock: [
    ['rowLockClaim', 0],
    ['rowLockHandler', 3],
    ['rowLockSettle', 4],
  ],
  lease: [
    ['leaseClaim', 0],
    ['leaseCommit', 1],
    ['leaseHandler', 3],
    ['leaseSettle', 4],
  ],
  advisory: [
    ['advisoryCandidates', 0],
    ['advisoryLock', 1],
    ['advisoryTake', 2],
    ['advisoryHandler', 3],
    ['advisorySettle', 4],
  ],
};

/*
 * How each form claims a row and settles it: the row lock form holds a transaction across the
 * handler, the lease form commits its claim and settles by the lease it wrote, and the advisory
 * lock form holds a key's lock on a connection of its own.
 */
export function claims(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const boxes: Box[] = FORMS.flatMap((form, index) =>
    STEPS[form].map(
      ([id, row]): Box => ({
        id,
        x: columnX(index),
        y: TOP + row * STEP,
        w: COLUMN,
        h: H,
        kind: id.endsWith('Handler') ? 'service' : id.endsWith('Settle') ? 'core' : 'broker',
        code: id === 'rowLockClaim' || id === 'leaseClaim',
        lines: lines(words, id),
        href: id.endsWith('Settle') ? '/architecture/settlement/' : undefined,
      }),
    ),
  );
  // The lease is kept alive beside its handler, by a task of the subscription's own.
  boxes.push({
    id: 'keeper',
    x: columnX(1),
    y: TOP + 2 * STEP,
    w: COLUMN,
    h: H,
    kind: 'note',
    lines: lines(words, 'keeper'),
  });
  boxes.push({
    id: 'connections',
    x: 0,
    y: TOP + 4 * STEP + H + 22,
    w: 360,
    h: 44,
    kind: 'note',
    lines: lines(words, 'connections'),
  });
  const links: Link[] = FORMS.flatMap((form) =>
    STEPS[form].slice(1).map(([id], index) => ({from: STEPS[form][index]![0], to: id})),
  );
  const lanes: Lane[] = FORMS.map((form, index) => ({x: centre(index), label: word(words, form)}));
  return {boxes, links, lanes};
}
