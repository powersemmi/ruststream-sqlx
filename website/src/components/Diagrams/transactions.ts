import type {Box, Lane, Link} from './flow';
import {lines, word, type Words} from './words';

const COLUMN = 104;
const columnX = (index: number): number => 12 + index * (COLUMN + 12);

// The rows of the drawing; the outcome row holds the longest text.
const ROWS = [52, 152, 252, 352, 466];
const HEIGHTS = [58, 58, 58, 89, 44];

/*
 * The transaction a handler writes through in transactional mode, per form. The row lock form
 * writes inside the claim's own transaction, behind a savepoint; the lease form opens one after
 * its claim committed; the advisory lock form opens one on the connection that holds the key and
 * ends it before the unlock. Each frame is the transaction; what stands outside it commits on
 * its own.
 */
export function transactions(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const step = (id: string, column: number, row: number, kind: Box['kind'] = 'broker'): Box => ({
    id,
    x: columnX(column),
    y: ROWS[row]!,
    w: COLUMN,
    h: HEIGHTS[row]!,
    kind,
    lines: lines(words, id),
  });
  // A frame stands a little outside its column and starts above its first step, for its name.
  const frame = (id: string, column: number, first: number, last: number): Box => ({
    id,
    x: columnX(column) - 4,
    y: ROWS[first]! - 24,
    w: COLUMN + 8,
    h: ROWS[last]! + HEIGHTS[last]! + 10 - (ROWS[first]! - 24),
    kind: 'group',
    lines: lines(words, id),
  });
  const boxes: Box[] = [
    frame('rowLockTx', 0, 0, 3),
    step('rowLockClaim', 0, 0),
    step('rowLockSavepoint', 0, 1),
    step('rowLockWrites', 0, 2, 'service'),
    step('rowLockOutcome', 0, 3, 'core'),
    step('leaseClaim', 1, 0),
    frame('leaseTx', 1, 1, 3),
    step('leaseBegin', 1, 1),
    step('leaseWrites', 1, 2, 'service'),
    step('leaseOutcome', 1, 3, 'core'),
    step('advisoryTake', 2, 0),
    frame('advisoryTx', 2, 1, 3),
    step('advisoryBegin', 2, 1),
    step('advisoryWrites', 2, 2, 'service'),
    step('advisoryOutcome', 2, 3, 'core'),
    step('advisoryUnlock', 2, 4),
  ];
  const chain = (ids: string[]): Link[] =>
    ids.slice(1).map((id, index) => ({from: ids[index]!, to: id}));
  const links: Link[] = [
    ...chain(['rowLockClaim', 'rowLockSavepoint', 'rowLockWrites', 'rowLockOutcome']),
    ...chain(['leaseClaim', 'leaseBegin', 'leaseWrites', 'leaseOutcome']),
    ...chain(['advisoryTake', 'advisoryBegin', 'advisoryWrites', 'advisoryOutcome', 'advisoryUnlock']),
  ];
  const lanes: Lane[] = ['rowLock', 'lease', 'advisory'].map((form, index) => ({
    x: columnX(index) + COLUMN / 2,
    label: word(words, form),
  }));
  return {boxes, links, lanes};
}
