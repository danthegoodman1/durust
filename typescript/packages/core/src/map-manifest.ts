import { decodePayload, encodePayload, type PayloadRef } from "./payload.js";

// The paged-manifest level walk, written once.
//
// Every map manifest — activity-map input, activity-map result,
// child-workflow-map result — is the same three-level `PayloadRef` structure:
// a manifest ref, whose decoded value holds page refs, whose decoded values
// hold the items. What differs between them is only the field a page carries
// its items in. Everything else — walking the levels, flattening, and the two
// count invariants — is identical, which is why it had been written seven
// times: once privately in `api.ts` and twice more in each of the three
// providers.
//
// The walk is also where payload hydration lands. `decodePayload` refuses a
// blob ref, so a provider wrapped by the payload layer must have hydrated
// every level before these functions see it; keeping the traversal in one
// place keeps that requirement in one place too.

/** Any map manifest, viewed as the level structure the walk needs. */
export interface PagedManifest<Page> {
  readonly itemCount: number;
  readonly pageLengths: readonly number[];
  readonly pages: readonly PayloadRef<Page>[];
}

/** A named result manifest, which is a `PagedManifest` plus its handle. */
export interface NamedPagedManifest<Page> extends PagedManifest<Page> {
  readonly name: string;
}

/**
 * Flatten a paged manifest into its items in ascending ordinal order,
 * enforcing both count invariants: the pages must yield exactly `itemCount`
 * items, and `pageLengths` must sum to the same number. A manifest that fails
 * either is corrupt, and reading past it would silently drop or duplicate map
 * items.
 *
 * `pageItems` is the only per-manifest difference: it names the field the page
 * carries.
 */
export function readMapManifestItems<Page, Item>(
  manifestRef: PayloadRef<PagedManifest<Page>>,
  pageItems: (page: Page) => readonly Item[],
  label: string
): readonly Item[] {
  const manifest = decodePayload<PagedManifest<Page>>(manifestRef);
  const items: Item[] = [];
  for (const pageRef of manifest.pages) {
    items.push(...pageItems(decodePayload<Page>(pageRef)));
  }
  if (items.length !== manifest.itemCount) {
    throw new Error(
      `${label} item count mismatch: expected ${manifest.itemCount}, got ${items.length}`
    );
  }
  const pageItemCount = manifest.pageLengths.reduce((sum, count) => sum + count, 0);
  if (pageItemCount !== manifest.itemCount) {
    throw new Error(
      `${label} page length mismatch: expected ${manifest.itemCount}, got ${pageItemCount}`
    );
  }
  return items;
}

/**
 * The item outcomes a terminal map's result manifest is assembled from,
 * checked against the count the engine says the map has.
 *
 * A provider stores item outcomes in a dense array with a hole for every
 * ordinal that has not landed, and assembles the manifest when the engine says
 * the map is complete. If the two ever disagree — a hole at assembly time, or
 * a slot array of the wrong length — the manifest silently acquires a missing
 * or extra item, and the parent reads a result set that never existed. This
 * turns that into a named failure at the moment it happens, which is also what
 * makes a mutation to the engine's completion rule fail loudly rather than as
 * an incidental type error deep in the encoder.
 */
export function completeMapItems<Item>(
  label: string,
  itemCount: number,
  slots: readonly (Item | null)[]
): readonly Item[] {
  if (slots.length !== itemCount) {
    throw new Error(
      `${label} covers ${slots.length} items, expected ${itemCount}`
    );
  }
  for (let ordinal = 0; ordinal < slots.length; ordinal += 1) {
    if (slots[ordinal] === null || slots[ordinal] === undefined) {
      throw new Error(`${label} is missing the outcome for item ${ordinal}`);
    }
  }
  return slots as readonly Item[];
}

/**
 * Build a named result manifest over `items` in ascending ordinal order.
 *
 * `page` is the only per-manifest difference: it wraps a run of items in the
 * page shape that manifest's reader expects. An empty map gets no page at all
 * rather than one empty page, so a zero-item manifest round-trips through
 * `readMapManifestItems` without a page whose length is zero.
 */
export function writeMapManifest<Item, Page>(
  name: string,
  items: readonly Item[],
  page: (pageItems: readonly Item[]) => Page
): PayloadRef<NamedPagedManifest<Page>> {
  const empty = items.length === 0;
  return encodePayload<NamedPagedManifest<Page>>({
    name,
    itemCount: items.length,
    pageLengths: empty ? [] : [items.length],
    pages: empty ? [] : [encodePayload<Page>(page(items))]
  });
}
