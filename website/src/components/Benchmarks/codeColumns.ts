/*
 * The code counts of this crate beside what each one is measured against. The model's cost chart
 * draws the service alone; this crate's document also counts the raw sqlx loop that runs the same
 * statements, and for the outbox the app without it and with an outbox written by hand. These
 * functions turn those counts into the cells of two tables.
 */

import type {CodeScenario} from './model';

type Count = CodeScenario['framework'];

/** A code scenario as this crate's recipe writes it: the model's fields and the loops beside. */
export interface CountedScenario extends CodeScenario {
  plugin?: string;
  // The raw sqlx loop; for a plugin scenario, the app without the plugin.
  raw?: Count;
  // For a plugin scenario, the plugin written by hand.
  adapter?: Count;
}

export interface InboxCodeRow {
  scenario: string;
  instructions: string;
  rawInstructions: string;
  allocations: string;
  rawAllocations: string;
}

export interface PluginCodeRow {
  scenario: string;
  // Without the plugin, by hand, and the crate's, in one cell.
  instructions: string;
  allocations: string;
}

// What a cell shows for a loop the document does not carry, as the model's tables do.
const EMPTY = '-';

const number = (value: number | undefined, lang: string): string =>
  typeof value === 'number' ? value.toLocaleString(lang, {maximumFractionDigits: 1}) : EMPTY;

/** The inbox's scenarios beside the raw loop, and each plugin's in its three variants. */
export function codeColumns(
  scenarios: CountedScenario[],
  lang: string,
): {inbox: InboxCodeRow[]; plugins: PluginCodeRow[]} {
  const variants = (scenario: CountedScenario, field: keyof Count): string =>
    [scenario.raw, scenario.adapter, scenario.framework]
      .map((count) => number(count?.[field], lang))
      .join(' / ');
  return {
    inbox: scenarios
      .filter((scenario) => scenario.plugin === undefined)
      .map((scenario) => ({
        scenario: scenario.name,
        instructions: number(scenario.framework.instructions, lang),
        rawInstructions: number(scenario.raw?.instructions, lang),
        allocations: number(scenario.framework.allocations, lang),
        rawAllocations: number(scenario.raw?.allocations, lang),
      })),
    plugins: scenarios
      .filter((scenario) => scenario.plugin !== undefined)
      .map((scenario) => ({
        scenario: scenario.name,
        instructions: variants(scenario, 'instructions'),
        allocations: variants(scenario, 'allocations'),
      })),
  };
}
