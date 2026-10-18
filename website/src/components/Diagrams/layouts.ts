import {claims} from './claims';
import {design} from './design';
import type {Box, Lane, Link} from './flow';
import {lanes} from './lanes';
import {lifecycle} from './lifecycle';
import {outbox} from './outbox';
import {path} from './path';
import {retries} from './retries';
import {settlement} from './settlement';
import {table} from './table';
import {transactions} from './transactions';
import {waking} from './waking';
import type {Words} from './words';

export interface Layout {
  boxes: Box[];
  links: Link[];
  lanes?: Lane[];
}

/** Every diagram a page can name: a layout places the boxes, the page supplies the words. */
export const LAYOUTS = {
  table,
  claims,
  transactions,
  waking,
  outbox,
  design,
  lifecycle,
  path,
  settlement,
  retries,
  lanes,
} satisfies Record<string, (words: Words) => Layout>;

export type DiagramName = keyof typeof LAYOUTS;
