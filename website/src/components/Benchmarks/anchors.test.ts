import {describe, expect, it} from 'vitest';
import {movedSection} from './anchors';

describe('a section of the earlier benchmarks page', () => {
  it('opens the section of the results page that took it over', () => {
    expect(movedSection('#the-numbers')).toBe('#results');
    expect(movedSection('#the-crates-own-code')).toBe('#cost-of-the-code');
  });

  it('opens the page of its own that took it over', () => {
    expect(movedSection('#what-they-do-not-mean')).toBe('numbers/#what-they-do-not-mean');
    expect(movedSection('#running-it-yourself')).toBe('methodology/#running-it-yourself');
    expect(movedSection('#the-costs-that-remain')).toBe('numbers/#the-costs-that-remain');
  });

  it('leaves an anchor the results page still has where it is', () => {
    expect(movedSection('#the-machine')).toBeUndefined();
    expect(movedSection('#throughput')).toBeUndefined();
    expect(movedSection('#the-outbox-as-a-plugin')).toBeUndefined();
    expect(movedSection('#against-a-raw-client')).toBeUndefined();
    expect(movedSection('')).toBeUndefined();
    expect(movedSection('#constructor')).toBeUndefined();
  });
});
