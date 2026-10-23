import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The message's way down the left; what the startup and the consumer's checks branch to on the
// right.
const LEFT = 12;
const LEFT_W = 200;
const RIGHT = 228;
const RIGHT_W = 120;

/*
 * The outbox over any broker. The publish layer records a message in the service's table on a
 * connection of its own, then sends it with the record's id; the subscription layer fetches the
 * record by that id, skips the handler for a record already processed, and marks the record once
 * the handler acknowledges. At startup the republish sends every record still unprocessed.
 */
export function outbox(words: Words): {boxes: Box[]; links: Link[]} {
  const left = (id: string, y: number, h: number, kind: Box['kind']): Box => ({
    id,
    x: LEFT,
    y,
    w: LEFT_W,
    h,
    kind,
    lines: lines(words, id),
  });
  const right = (id: string, y: number, kind: Box['kind']): Box => ({
    id,
    x: RIGHT,
    y,
    w: RIGHT_W,
    h: 58,
    kind,
    lines: lines(words, id),
  });
  const boxes: Box[] = [
    left('producer', 0, 44, 'service'),
    right('republish', 0, 'core'),
    left('record', 76, 58, 'core'),
    left('broker', 160, 44, 'broker'),
    left('fetch', 232, 58, 'core'),
    right('processed', 232, 'good'),
    left('consumer', 316, 44, 'service'),
    left('mark', 388, 58, 'good'),
    right('waits', 388, 'bad'),
    {id: 'guarantee', x: 0, y: 470, w: 360, h: 44, kind: 'note', lines: lines(words, 'guarantee')},
  ];
  const links: Link[] = [
    {from: 'producer', to: 'record'},
    {from: 'record', to: 'broker', label: word(words, 'sends')},
    {from: 'republish', to: 'broker', exit: 'bottom', enter: 'right', label: word(words, 'resends')},
    {from: 'broker', to: 'fetch', label: word(words, 'delivers')},
    {from: 'fetch', to: 'processed'},
    {from: 'fetch', to: 'consumer', label: word(words, 'unprocessed')},
    {from: 'consumer', to: 'mark', label: word(words, 'acks')},
    {from: 'consumer', to: 'waits', exit: 'right', enter: 'top', label: word(words, 'retries')},
  ];
  return {boxes, links};
}
