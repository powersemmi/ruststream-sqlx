import type {Box, Lane, Link} from './flow';
import {lines, word, type Words} from './words';

// The three shapes a delivery takes: one row at a time, a batch, and a row handed over whole.
const COLUMNS = {single: 64, batch: 180, row: 296} as const;
const W = 104;
const H = 74;
const STEP = 90;

// Each column's steps, by the row of the drawing they stand in. The handler and the
// settlement share a row across the columns, so the three read side by side.
export const STEPS: Record<keyof typeof COLUMNS, [string, number][]> = {
  single: [
    ['claim', 0],
    ['delivery', 1],
    ['decode', 2],
    ['handler', 3],
    ['reply', 4],
    ['settle', 5],
  ],
  batch: [
    ['batchClaim', 0],
    ['batchDelivery', 1],
    ['batchDecode', 2],
    ['batchHandler', 3],
    ['batchSettle', 5],
  ],
  row: [
    ['rowClaim', 0],
    ['rowDelivery', 1],
    ['rowHandler', 3],
    ['rowSettle', 5],
  ],
};

/*
 * The path of one row through a service, in the three shapes a subscription takes: one row at a
 * time with the reply it answers with, a batch that one claim fills, and row mode, where the
 * handler takes the row as the driver read it and no codec runs.
 */
export function path(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const kind = (id: string): Box['kind'] => {
    if (/handler$/i.test(id)) {
      return 'service';
    }
    return /settle$/i.test(id) ? 'core' : 'broker';
  };
  const href = (id: string): string | undefined => {
    if (/[sS]ettle$/.test(id)) {
      return '/architecture/settlement/';
    }
    return /[cC]laim$/.test(id) ? '/architecture/queues/claims/' : undefined;
  };
  const boxes: Box[] = (Object.keys(STEPS) as (keyof typeof COLUMNS)[]).flatMap((column) =>
    STEPS[column].map(
      ([id, row]): Box => ({
        id,
        x: COLUMNS[column] - W / 2,
        y: 28 + row * STEP,
        w: W,
        h: H,
        kind: kind(id),
        lines: lines(words, id),
        href: href(id),
      }),
    ),
  );
  const chain = (column: keyof typeof COLUMNS): Link[] =>
    STEPS[column].slice(1).map(([id], index) => ({from: STEPS[column][index]![0], to: id}));
  const links: Link[] = [...chain('single'), ...chain('batch'), ...chain('row')];
  // Row mode hands the handler the row itself: no codec runs between the two.
  links.find((link) => link.to === 'rowHandler')!.label = word(words, 'noCodec');
  const lanes: Lane[] = (Object.keys(COLUMNS) as (keyof typeof COLUMNS)[]).map((column) => ({
    x: COLUMNS[column],
    label: word(words, column),
  }));
  return {boxes, links, lanes};
}
