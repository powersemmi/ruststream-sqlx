import type {Box, Link} from './flow';
import {lines, type Words} from './words';

// The three forms at the foot of the drawing, each where its statement takes effect.
export const ENDS = ['rowLock', 'lease', 'advisory'] as const;
const COLUMN = 104;
const columnX = (index: number): number => 12 + index * (COLUMN + 12);

/*
 * A settlement: the handler's outcome picks a statement, transactional mode rolls the handler's
 * writes back first unless the outcome acknowledges, and the statement runs where the form holds
 * the row: in the claim's transaction, behind the lease's token, or on the connection that holds
 * the key.
 */
export function settlement(words: Words): {boxes: Box[]; links: Link[]} {
  const wide = (id: string, y: number, kind: Box['kind'], h = 44): Box => ({
    id,
    x: 12,
    y,
    w: 336,
    h,
    kind,
    lines: lines(words, id),
  });
  const boxes: Box[] = [
    wide('outcome', 0, 'service'),
    wide('rollback', 72, 'core'),
    wide('statement', 144, 'broker', 58),
    ...ENDS.map(
      (id, index): Box => ({
        id,
        x: columnX(index),
        y: 234,
        w: COLUMN,
        h: 89,
        kind: 'broker',
        lines: lines(words, id),
      }),
    ),
    {id: 'dropped', x: 0, y: 345, w: 360, h: 44, kind: 'note', lines: lines(words, 'dropped')},
  ];
  const along = (index: number): number => (columnX(index) + COLUMN / 2 - 12) / 336;
  const links: Link[] = [
    {from: 'outcome', to: 'rollback'},
    {from: 'rollback', to: 'statement'},
    ...ENDS.map((id, index): Link => ({from: 'statement', to: id, exitAt: along(index)})),
  ];
  return {boxes, links};
}
