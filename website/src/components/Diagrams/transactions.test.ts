import {expect, it} from 'vitest';
import type {Box} from './flow';
import {transactions} from './transactions';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

const inside = (outer: Box, inner: Box): boolean =>
  inner.x >= outer.x &&
  inner.y >= outer.y &&
  inner.x + inner.w <= outer.x + outer.w &&
  inner.y + inner.h <= outer.y + outer.h;

it('frames in each form the steps its transaction holds, and leaves out what commits on its own', () => {
  const {boxes} = transactions(words);
  const byId = new Map(boxes.map((box) => [box.id, box]));
  const framed = (frame: string): string[] =>
    boxes
      .filter((box) => box.kind !== 'group' && inside(byId.get(frame)!, box))
      .map((box) => box.id);
  // The row lock form writes inside the claim's own transaction.
  expect(framed('rowLockTx')).toEqual([
    'rowLockClaim',
    'rowLockSavepoint',
    'rowLockWrites',
    'rowLockOutcome',
  ]);
  // The lease claim commits before the delivery's transaction opens.
  expect(framed('leaseTx')).toEqual(['leaseBegin', 'leaseWrites', 'leaseOutcome']);
  // The take commits on its own, and the unlock follows the transaction's end.
  expect(framed('advisoryTx')).toEqual(['advisoryBegin', 'advisoryWrites', 'advisoryOutcome']);
});
