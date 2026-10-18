import {expect, it} from 'vitest';
import {FORMS, STEPS, claims} from './claims';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('runs each form from its claim to its settlement in one column, never across', () => {
  const {boxes, links, lanes} = claims(words);
  const column = new Map(boxes.map((box) => [box.id, box.x + box.w / 2]));
  for (const [index, form] of FORMS.entries()) {
    const ids = STEPS[form].map(([id]) => id);
    // A chain from the first step to the last, each step linked to the next.
    expect(links.filter((link) => ids.includes(link.from)).map((link) => link.to)).toEqual(
      ids.slice(1),
    );
    for (const id of ids) {
      expect(column.get(id), id).toBe(lanes[index]!.x);
    }
  }
});

it('stands the handler and the settlement of every form on one row', () => {
  const {boxes} = claims(words);
  const y = (id: string): number => boxes.find((box) => box.id === id)!.y;
  expect(new Set(FORMS.map((form) => y(`${form}Handler`))).size).toBe(1);
  expect(new Set(FORMS.map((form) => y(`${form}Settle`))).size).toBe(1);
});
