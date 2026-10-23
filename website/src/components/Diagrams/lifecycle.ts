import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * The broker's states as types on the left, and on the right what each transition does on the
 * database: the configuration recorded at construction, the one connection connect takes, what
 * each subscription checks and prepares at startup, the work that runs while the service does,
 * and what shutdown stops and releases.
 */
export function lifecycle(words: Words): {boxes: Box[]; links: Link[]} {
  const left = (id: string, y: number, h: number, href?: string): Box => ({
    id,
    x: 12,
    y,
    w: 156,
    h,
    kind: 'broker',
    code: true,
    lines: lines(words, id),
    href,
  });
  const right = (id: string, y: number, h: number, href?: string): Box => ({
    id,
    x: 192,
    y,
    w: 156,
    h,
    kind: 'core',
    lines: lines(words, id),
    href,
  });
  const boxes: Box[] = [
    {id: 'states', x: 0, y: 0, w: 176, h: 490, kind: 'group', lines: lines(words, 'states')},
    {id: 'database', x: 184, y: 0, w: 176, h: 490, kind: 'group', lines: lines(words, 'database')},
    left('unconnected', 28, 44),
    left('connected', 224, 44),
    left('closed', 400, 74),
    right('config', 28, 58),
    right('connection', 100, 74),
    right('subscriptions', 196, 89, '/architecture/queues/'),
    right('running', 307, 58, '/architecture/queues/waking/'),
    right('stop', 387, 89),
    {
      id: 'afterShutdown',
      x: 0,
      y: 512,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'afterShutdown'),
    },
  ];
  const links: Link[] = [
    {from: 'unconnected', to: 'connected', label: word(words, 'connect')},
    {from: 'connected', to: 'closed', label: word(words, 'shutdown')},
    {from: 'config', to: 'connection'},
    {from: 'connection', to: 'subscriptions'},
    {from: 'subscriptions', to: 'running'},
    {from: 'running', to: 'stop'},
  ];
  return {boxes, links};
}
