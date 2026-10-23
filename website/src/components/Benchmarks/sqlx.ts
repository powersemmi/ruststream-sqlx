/*
 * This crate's results, as the pages of this site show them. The document is the one the
 * benchmark recipe committed beside the pages, read when the site is built: a version of the
 * documentation shows the numbers measured for it, and an archive keeps its own. Besides the
 * sections the model renders, it carries the throughput grid and the raw loop's code counts,
 * which the modules beside this one read.
 */

import document from '../../../../docs/benchmarks/results.json';
import {BROKERS, type Broker, type Loaded, type Results} from './model';

export const SQLX: Broker = BROKERS.find((broker) => broker.repo === 'ruststream-sqlx')!;

export const sqlx: Loaded = {broker: SQLX, results: document as Results};
