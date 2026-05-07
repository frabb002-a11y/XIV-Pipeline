---
name: PowerBI market recommendation model
overview: Build a daily-updated “best items to sell today” Power BI model for Louisoix by combining 30d sales history (demand) with a daily listing snapshot (today pricing) and a buy-mats-from-marketboard ROI calculation for the top N items.
todos:
  - id: inspect-current-etl
    content: Inspect `extract.py` and `transform.py` to see what endpoint/data you already pull and what grain it uses.
    status: pending
  - id: add-dev-caching
    content: During development, cache `/api/v2/marketable` item IDs to a local JSON so iteration doesn’t re-download the universe each run.
    status: pending
  - id: add-discovery-scan
    content: Add a one-time/occasional discovery scan to compute a whitelist of top-K items (prod target K=1000; dev K=100) for Louisoix, then only refresh that whitelist daily.
    status: pending
  - id: add-universalis-full-coverage-ranking
    content: For discovery, pull `/api/v2/aggregated/Louisoix/{itemIds}` for all marketable items (batched) and compute an approximate gil/day ranking using `avgSalePrice_4d * dailySaleVelocity_4d`.
    status: pending
  - id: add-universalis-history-ingest
    content: Pull `/api/v2/history/Louisoix/{topNItemIds}` (30d) only for the selected top N items to compute true 30d demand metrics and to power “sale price over time” charts.
    status: pending
  - id: add-universalis-snapshot-ingest
    content: Ingest a daily Louisoix pricing snapshot via `/api/v2/aggregated/{world}/{itemIds}` into `fact_market_snapshot_daily`.
    status: pending
  - id: add-recipe-bom
    content: Add a recipe bill-of-materials source (XIVAPI or static dump) and build `fact_recipe_ingredient` (non-recursive) for buy-mats-from-marketboard costing.
    status: pending
  - id: update-db-schema
    content: Extend `load.py` schema to include `fact_sales`, `fact_market_snapshot_daily`, and recipe BOM tables; ensure idempotent upserts and append-only facts where needed.
    status: pending
  - id: powerbi-model-measures
    content: Create Power BI relationships and measures (hot-cakes ranking, today ROI, profit/day) and a ranked “best to sell today” page.
    status: pending
isProject: false
---

# Goal

Answer: **“What should I sell today on Louisoix?”** by ranking items that:

- **Sell fast** (high volume, 30d)
- **Move lots of gil** (gil volume/day, 30d)
- **Have good today margin/ROI** assuming **you buy all mats from the Louisoix marketboard**

Daily refresh. Start with **top N = 500**.

Also support a **discovery mode** that builds a longer-lived **whitelist** (target **top K = 1000**; dev/test **K = 100**) so daily runs only refresh a known-good set.

# What endpoints we will use

- **Universe of sellable items**: `/api/v2/marketable`
- **Full coverage ranking (cheap)**: `/api/v2/aggregated/Louisoix/{itemIds}` (compute approx gil/day via `avgSalePrice_4d * dailySaleVelocity_4d`)
- **30d demand + sale charts (expensive; top N only)**: `/api/v2/history/Louisoix/{itemIds}`
- **Today pricing snapshot (top N)**: `/api/v2/aggregated/Louisoix/{topNItemIds}` (median/min listing fields for “today” prices)
- **Item names**: XIVAPI (you already use it in `Enrich.py`)
- **Recipe BOM (ingredients)**: XIVAPI v2 sheets:
  - `RecipeLookup/{craftedItemId}` → recipe row id(s)
  - `Recipe/{recipeRowId}` → ingredient ids + quantities (BOM)
  - (optional) `RecipeLevelTable/{id}` → craft metadata (level/difficulty), not needed for ROI

# Data model to build in the database (star schema)

Your current DB tables only cover item names and last upload time:

- `[c:\Users\truet\Documents\XIV Pipeline\load.py]`: `xiv_data.raw_data(itemid, lastuploadtime, worldid, worldname)`
- `[c:\Users\truet\Documents\XIV Pipeline\Enrich.py]`: fills `xiv_data.name_data(itemid, itemname)` using XIVAPI.

Add these tables (names can be adjusted, but keep a star shape):

## Dimensions

- `**dim_item`**: `item_id`, `item_name` (can be your existing `xiv_data.name_data`, possibly renamed later)
- `**dim_world`**: `world_id`, `world_name` (populate from Universalis `/api/v2/worlds` so facts can store `world_id` only)
- `**dim_date**` (Power BI can generate this too, but having it in SQL is convenient): `date_key`, `date`, etc.

## Facts (minimum viable)

- `**fact_market_snapshot_daily**` (from `/api/v2/aggregated/Louisoix/...`):
  - grain: `**snapshot_date_utc` × `world_id` × `item_id**`
  - columns (NQ-only MVP):\n
    - `snapshot_date_utc` (derived at load time as current UTC date)\n
    - `world_id`\n
    - `item_id`\n
    - `nq_min_listing_price_world`\n
    - `nq_avg_sale_price_4d_world` (nullable)\n
    - `nq_daily_sale_velocity_4d_world` (nullable)\n
    - `world_upload_ts_ms` (optional, from `worldUploadTimes` for the requested world_id)\n
- `**fact_listings_snapshot**` (optional v1+, from `/api/v2/{worldDcRegion}/{itemIds}`):
  - grain: `**snapshot_time` × `world_id` × `item_id` × `listing_id**`
  - columns: `price_per_unit`, `quantity`, `hq_flag`, etc.
- `**fact_sales**` (from `/api/v2/history/Louisoix/...`):
  - grain: one row per sale event\n
  - columns (NQ-only MVP): `world_id`, `item_id`, `sold_at_ts`, `price_per_unit`, `quantity`\n
  - note: do not assume a reliable `sale_id` exists; use overwrite-window loading instead.

## Recipe BOM (non-recursive)

To compute “buy-mats-from-marketboard cost”, add a BOM table:

- `**fact_recipe_ingredient**`
  - grain: `crafted_item_id` × `ingredient_item_id`
  - columns: `crafted_item_id`, `ingredient_item_id`, `ingredient_qty`
  - note: do **not** expand ingredients-of-ingredients (by design).

## Costs / ROI (v2, recommended)

Universalis does **not** include vendor/NPC prices. For “real in-game cost”, you need game data (XIVAPI or Lumina).

Add `**fact_item_cost`** using a hybrid cost model:

- **VendorPrice (preferred when available)**: pull gil prices / shop prices from **XIVAPI** (or Lumina export).
- **FallbackMarketPrice**: when no vendor price exists (gathered/dropped/crafted mats), use **Universalis** to price the material “right now”.
- grain: `as_of_date` × `world_id` × `item_id`
- columns: `estimated_unit_cost`, `cost_source` (e.g. `vendor`, `market`, `manual`)

If you later add recipe decomposition, compute crafted item cost as the sum of ingredient costs (with recursion for sub-recipes).

# Power BI modeling (relationships)

- `fact_market_agg_snapshot[item_id]` → `dim_item[item_id]` (many-to-one)
- `fact_market_agg_snapshot[world_id]` → `dim_world[world_id]` (many-to-one)
- `fact_market_agg_snapshot[snapshot_date]` → `dim_date[date]` (many-to-one)
- If you add listings/sales, relate them the same way to `dim_item`, `dim_world`, `dim_date`.

# World + time conventions (locked)

- Facts store `**world_id`** (space-efficient, joinable); human-readable names come from `dim_world`.\n
- `snapshot_date_utc` is the **UTC date at pipeline run time** (Universalis aggregated does not provide a snapshot date; we stamp it).\n
- Sale events use `sold_at_ts` timestamps from Universalis history; store as timestamp (or bigint ms) consistently.

# Measures (dual pricing: 30d demand + today ROI)

- **HotCakes_GilVolumePerDay (30d)**: from `fact_sales`: \n  -  \text{GilVolumePerDay} = \frac{\sum(\text{pricePerUnit} \times \text{quantity})}{30} \n
- **HotCakes_UnitsPerDay (30d)**:  \frac{\sum(\text{quantity})}{30} \n
- **Today_SellPrice**: from `fact_market_snapshot_daily` (prefer `median_listing_price` for stability)\n
- **Today_MatCost**: sum over `fact_recipe_ingredient` × today ingredient price (from snapshot)\n
- **Today_UnitProfit**: `Today_SellPrice - Today_MatCost`\n
- **Today_ROI**: `Today_UnitProfit / Today_MatCost`\n
- **ProfitPerDayEstimate**: `Today_UnitProfit * HotCakes_UnitsPerDay`\n

Rank by **ProfitPerDayEstimate** with guardrails (min sales count, min units/day).

# ETL plan (phase-by-phase)

## Extract

- **Discovery mode** (local, occasional; builds/refreshes whitelist):
  - dev).
    Pull `/api/v2/marketable` (cache locally as JSON during
  - Pull `/api/v2/aggregated/Louisoix/{itemIds}` for **all** marketable items (batched by 100).
  - Output: `aggregated_rows_all` (one row per item with `avgSalePrice_4d`, `dailySaleVelocity_4d`, and listing stats).
- **Daily refresh mode** (scheduled; uses whitelist):
  - Load whitelist item IDs (dev K=100; prod K=1000).
  - Pull `/api/v2/history/Louisoix/{itemIds}` (batched by 100) for **whitelist only** (30d sale events).
  - Pull `/api/v2/aggregated/Louisoix/{itemIds}` (batched by 100) for **whitelist only** (today pricing snapshot).
  - Pull recipe BOM for crafted items in whitelist (XIVAPI or static dump; not Universalis).
  - Outputs: `sales_events_30d`, `aggregated_rows_today`, `recipe_bom_rows`.

## Transform

- **Discovery transform**:
  - Compute `ApproxGilPerDay = avgSalePrice_4d * dailySaleVelocity_4d`.
  - Select **top K** and build the whitelist.
  - Output: `df_watchlist` (item_id list + rank/score).
- **Daily refresh transform**:
  - Flatten `sales_events_30d` → `df_fact_sales`.
  - Flatten `aggregated_rows_today` → `df_fact_market_snapshot_daily` (today row(s)).
  - Normalize `recipe_bom_rows` → `df_fact_recipe_ingredient`.
  - Output metrics for Power BI:
    - `GilVolumePerDay_30d`, `UnitsPerDay_30d`, `MedianSalePrice_30d`
    - today ROI measures (from the Measures section).

## Load

- Upsert/append into DB tables (single tables, many rows):
  - `dim_watchlist_item` (whitelist)
  - `fact_sales` (overwrite-window each run for last-30d for whitelist items; optional intra-run exact-duplicate removal)
  - `fact_market_snapshot_daily` (upsert by `snapshot_date_utc + world_id + item_id`)
  - `fact_recipe_ingredient` (upsert by `crafted_item_id + ingredient_item_id`)

## Enrich (runs after Load 1)

- Purpose: keep dimensions human-readable for Power BI.\n
- After LoaMERGE/upsert `itemname`\n
- Note: `dim_world(world_id, world_name)` is populated from Universalis; that is a dimension-load step (not XIVAPI enrichment).

## Main / orchestration

- Add a **testing flag** (e.g. `TEST_MODE=true`) so that:
  - If `TEST_MODE==true` **and** whitelist exists → **skip discovery** and only refresh the existing whitelist.
  - If `TEST_MODE==false` → run discovery on your chosen schedule (e.g. weekly) and update the whitelist, then run daily refresh.

## Implementation notes (current codebase)

- `extract.py`: implement the Extract items above (batching + concurrency + local caching for `/marketable`).
- `transform.py`: implement the transforms and top-K selection.
- `load.py`: extend schema + idempotent merges/appends for the new tables.
- `Enrich.py`: keep as-is (item name fill for missing IDs).
- `main.py`: route between discovery vs refresh and pass `K` + `TEST_MODE`.

## Steps to implementation (dev-first sequence we agreed)

### Definitions

- `WORLD = "Louisoix"`
- `DEVMODE`: when true, cache intermediate API outputs to local JSON and reuse them if present.
- `K_final = 1000` (final whitelist size)
- `K_test = 100` (fast iteration whitelist size while testing)

### Extract step 1 (`extract.py`): pull marketable universe

- Call `GET /api/v2/marketable` (this can take a while).
- Implementation detail: use a single request (no batching needed), but keep standard timeout/retry handling.
- Confirmation: `/api/v2/marketable` returns **item IDs** usable directly in `/api/v2/aggregated/{world}/{itemIds}` and `/api/v2/history/{world}/{itemIds}`.\n

### Transform 1 (`transform.py`): normalize + make item IDs batchable (dev input prioritized)

- Goal: make `item_ids` accessible in a consistent shape **regardless of input** (API response vs cached JSON).
- Input priority:
  - If `DEVMODE == true` and `marketable.json` exists → use `marketable.json`
  - Else → use the in-memory result from `GET /api/v2/marketable`
- Standard transformations:
  - validate all entries are ints
  - dedupe + sort (optional but makes runs deterministic)
  - chunk into lists of **100** item IDs (because Universalis accepts up to 100 per request)
- Output:
  - `item_id_batches_100` to feed `GET /api/v2/aggregated/Louisoix/{itemIds}`

### Extract step 2 (`extract.py`): pull aggregated stats for all marketable items

- Call `GET /api/v2/aggregated/Louisoix/{itemIds}` for all item IDs (chunks of 100).
- Implementation detail: perform **concurrent pulls** across batches using multiple connections (e.g. `ThreadPoolExecutor`), while respecting Universalis limits (max ~8 simultaneous connections and overall request rate limits).
- Retry policy (for every API call): retry up to **3** times with sleeps of **1s, 2s, 3s**, then **log and continue** (collect as much as possible).
- Note: this endpoint returns **aggregated summary stats** (4-day window). It is **not** full sale-event history.

#### Aggregated ranking fields (what we keep and why)

- **Core ranking inputs** (used in Transform 2):\n
  - For now, use **NQ only**.\n
  - `avgSalePrice_4d_world_nq = nq.averageSalePrice.world.price`\n
  - `dailySaleVelocity_4d_world_nq = nq.dailySaleVelocity.world.quantity`\n
  - `ApproxGilPerDay = avgSalePrice_4d_world_nq * dailySaleVelocity_4d_world_nq`\n
- **Keep for later requirements (ROI + sanity checks)**:\n
  - `today_sell_price_nq = nq.minListing.world.price` → “today undercut price” reference\n
  - `recentPurchasePrice_world_nq` + `recentPurchaseTimestamp_world_nq` (`nq.recentPurchase.world.`*) → recency sanity + last trade\n
  - `worldUploadTimes` → data freshness per worldId (optional)\n
- Note (confirmed by live query): Universalis `/aggregated/Louisoix/{itemIds}` can occasionally omit specific `*.world` metrics for an item (even though the item is marketable). For the NQ-only, world-only MVP:\n
  - If `nq.averageSalePrice.world.price` or `nq.dailySaleVelocity.world.quantity` is missing, treat that item as **unrankable** for discovery (log + skip).\n
  - If `nq.minListing.world.price` is missing, treat that item as **unpriceable** for “today undercut” (log + skip).\n

### Transform 2 (`transform.py`): rank aggregated → export whitelist (+ keep whitelist aggregated fields)

- Goal: take aggregated summary stats and produce a ranked “most marketable → least marketable” whitelist.
- Input priority:
  - If `DEVMODE == true` and `aggregated.json` exists → use `aggregated.json`
  - Else → use the in-memory aggregated results from Extract step 2
- Standard transformations:
  - normalize/flatten per-item aggregated results into rows like:
    - `item_id`, `avg_sale_price_4d`, `daily_sale_velocity_4d`, `min_listing`, `median_listing`, etc.
  - compute:
    - `ApproxGilPerDay = avgSalePrice_4d * dailySaleVelocity_4d`
  - sort descending by `ApproxGilPerDay`
  - optional guardrails:
    - require `dailySaleVelocity_4d > 0` (drops dead items)
- Export whitelist outputs:
  - `whitelist.json` (IDs + ranking score):
    - If `DEVMODE == true`: limit to top `K_test = 100` (fast testing loop)
    - Else: limit to top `K_final = 1000` (final scheduled whitelist size)
  - `whitelist_aggregated.json` (the same whitelist IDs, but with the aggregated fields we care about):
    - `item_id`, `min_listing`, `median_listing`, `avg_sale_price_4d`, `daily_sale_velocity_4d`, `upload_time`, `ApproxGilPerDay`
    - Reason: in a single pipeline run, we already have the aggregated data; **do not re-call** `/api/v2/aggregated/...` just to get “today pricing”.

### Extract step 3 (`extract.py`): pull sale-event history for the whitelist (detailed)

- Input priority:
  - If `DEVMODE == true` and `whitelist.json` exists → use `whitelist.json`
  - Else → use the in-memory whitelist result from Transform 2
- Call `GET /api/v2/history/Louisoix/{itemIds}` for whitelist items (chunks of 100), with a 30-day window.
- Output:
  - `history.json` (dev cache) or in-memory sale events for Transform 3.

#### 30-day window parameters (deterministic meaning)

- `entriesWithin`: how far back to include sale events, in **seconds**.\n
  - For 30 days: `entriesWithin = 30 * 24 * 60 * 60 = 2592000`
- `entriesUntil` (optional): an explicit “end time” in **unix seconds**.\n
  - If set to “now”, the 30-day window becomes exactly [now-30d, now].\n
  - If omitted, the API uses “now” implicitly.

#### History precision (what we keep)

- For charts + metrics we only need per-entry:\n
  - `timestamp`, `pricePerUnit`, `quantity`\n
- Everything else (buyerName, histograms, etc.) can be dropped in Transform 3 unless you want extra analysis later.

### Transform 3 (`transform.py`): sales history → true 30d demand metrics + charts input

- Flatten sale events into rows (per sale):
  - `item_id`, `sold_at`, `price_per_unit`, `quantity`\n
  - Note: for now, drop `hq` from the pipeline (NQ-only MVP).
- Compute true metrics per item over 30d:
  - `GilVolumePerDay_30d = SUM(price_per_unit * quantity) / 30`
  - `UnitsPerDay_30d = SUM(quantity) / 30`
  - `MedianSalePrice_30d` (optional but useful)
  - Note: you can compute these either in SQL or in Power BI measures. If computed in Power BI, only load the flattened sale rows.

### Note: “today pricing” source (no extra extract in dev-first single run)

- We **do not** re-call `/api/v2/aggregated/...` in the same run.\n
- Use `whitelist_aggregated.json` as the “today pricing snapshot” inputs for ROI.\n
- In scheduled daily refresh runs, we will re-call `/api/v2/aggregated/Louisoix/{whitelistItemIds}` to refresh today prices.

### Extract step 5 (`recipe BOM`, outside Universalis): ingredients for crafted items

- Source: XIVAPI v2 (live confirmed).\n
- For each crafted `item_id`, fetch recipe row id(s) via:\n
  - `GET https://v2.xivapi.com/api/sheet/RecipeLookup/{craftedItemId}?fields=ALC@as(raw),ARM@as(raw),BSM@as(raw),CRP@as(raw),CUL@as(raw),GSM@as(raw),LTW@as(raw),WVR@as(raw)`\n
  - Choose the non-zero job field(s) → `recipeRowId` (if all zero, not craftable)\n
- Why `RecipeLookup` is required:\n
  - The `Recipe` sheet is keyed by **recipe row id**, not by **crafted item id**.\n
  - You cannot derive the recipe row id from the crafted item id without either:\n
    - `RecipeLookup/{craftedItemId}` (fast, direct), or\n
    - a full search over `Recipe` by `ItemResult` (slow/expensive at scale).\n
- Then fetch BOM arrays via (note: the field name is `Ingredient`, not `ItemIngredient`):\n
  - `GET https://v2.xivapi.com/api/sheet/Recipe/{recipeRowId}?fields=ItemResult@as(raw),AmountResult,Ingredient@as(raw),AmountIngredient[]`\n
- Transform: zip `Ingredient[i]` with `AmountIngredient[i]`, drop zeros, emit rows for the BOM table (non-recursive by design):\n
  - `crafted_item_id`, `ingredient_item_id`, `ingredient_qty`\n
- Optional craft metadata (NOT needed for ROI):\n
  - `Recipe/{recipeRowId}?fields=RecipeLevelTable@as(raw)` then `RecipeLevelTable/{id}` for `ClassJobLevel`, `Difficulty`, etc.

### How the BOM links to prices (quick intuition)

- Many crafted items can share the same ingredient — that’s normal.\n
- Mat cost is computed by joining `ingredient_item_id` to “today ingredient price” (from `whitelist_aggregated` or a pricing table) and summing `ingredient_qty * ingredient_price`.

### Transform 4 (`transform.py`): normalize BOM rows for the database

- Goal: take the raw recipe/BOM output and make it consistent and joinable.
- Standard transformations:
  - validate IDs are ints and quantities are positive numbers
  - dedupe rows on (`crafted_item_id`, `ingredient_item_id`) by summing quantities if duplicates appear
  - (optional) attach `crafted_item_name` / `ingredient_item_name` via `name_data` for human readability (Power BI can also join names later)
- Output:
  - `df_fact_recipe_ingredient` with columns:
    - `crafted_item_id`, `ingredient_item_id`, `ingredient_qty`

### Extract step 6 (`extract.py`): price the ingredients (needed for mat-cost ROI)

- Important: the whitelist aggregated data gives prices for the **crafted items**, not automatically the **ingredient items**.\n
- After Transform 4, collect the set of unique `ingredient_item_id` values and call:\n
  - `GET /api/v2/aggregated/Louisoix/{ingredientItemIds}` (batched 100, concurrent + retry policy)\n
- Output:\n
  - `ingredients_aggregated.json` (dev cache) or in-memory ingredient pricing rows

### Transform 5 (`transform.py`): prune recipes with non-marketable/unpriceable ingredients

- Goal: enforce your rule: “all ingredients must be purchasable from the marketboard; otherwise prune the crafted item.”
- Method:
  - Build a set of ingredient IDs that successfully returned pricing from Extract step 6.
  - Optional extra guardrail: also require ingredient IDs exist in the `/api/v2/marketable` universe (from Transform 1). This is a stricter definition of “MB purchasable”.
  - For each `crafted_item_id`, if any required `ingredient_item_id` is missing pricing → drop that crafted item from the final recommendation set.
- Output:
  - `df_recipe_ingredient_pruned` and/or `df_watchlist_pruned`

### Transform 6 (`transform.py`): compute mat cost inputs (ready for ROI)

- For MVP: stop at producing joinable tables for Power BI to compute ROI.\n
- Outputs needed:\n
  - Crafted today sell price (from aggregated): `item_id`, `today_sell_price_nq` (from `nq.minListing.world.price`)\n
  - Ingredient today prices (from aggregated): `ingredient_item_id`, `ingredient_today_price_nq` (use `nq.minListing.world.price`)\n
  - BOM: `crafted_item_id`, `ingredient_item_id`, `ingredient_qty`\n
- Power BI then computes:\n
  - `Today_MatCost = SUMX(ingredients, ingredient_qty * ingredient_today_price_nq)`\n
  - `Today_ROI = (Today_SellPrice_NQ - Today_MatCost) / Today_MatCost`\n

### Load 1 (DB schema + row definitions)

This is the minimal set of tables to support Power BI for the NQ-only MVP. Facts store `world_id` and join to `dim_world` for display names.

#### `xiv_data.dim_world`

- **Grain**: 1 row per world
- **Primary key**: `world_id`
- **Columns**:
  - `world_id` (INT, PK)
  - `world_name` (VARCHAR(50), NOT NULL)
- **Source**: Universalis `GET /api/v2/worlds` (extract later)
- **Load rule**: MERGE/upsert by `world_id`

#### `xiv_data.dim_watchlist_item`

- **Grain**: 1 row per `world_id` × `item_id` in the whitelist
- **Primary key**: (`world_id`, `item_id`)
- **Columns**:
  - `world_id` (INT, NOT NULL)
  - `item_id` (INT, NOT NULL)
  - `rank_score` (DOUBLE PRECISION, NULL)  \n
    - used for discovery ranking (e.g. ApproxGilPerDay); can be NULL if not computed
  - `asof_date_utc` (DATE, NOT NULL) \n
    - stamp when this whitelist was produced
- **Source**: Transform 2 output (top K IDs + score)
- **Load rule**: simplest is overwrite per world:\n
  - DELETE `WHERE world_id = :world_id` then INSERT all rows for that world_id\n
  - (or MERGE if you prefer; overwrite is clearer)

#### `xiv_data.fact_market_snapshot_daily`

- **Grain**: 1 row per `snapshot_date_utc` × `world_id` × `item_id`
- **Primary key**: (`snapshot_date_utc`, `world_id`, `item_id`)
- **Columns (NQ-only MVP)**:
  - `snapshot_date_utc` (DATE, NOT NULL)
  - `world_id` (INT, NOT NULL)
  - `item_id` (INT, NOT NULL)
  - `nq_min_listing_price_world` (INT, NULL) \n
    - your “today undercut price”; if NULL treat item as unpriceable for today
  - `nq_avg_sale_price_4d_world` (DOUBLE PRECISION, NULL) \n
    - may be NULL for some items; if NULL treat item as unrankable for discovery
  - `nq_daily_sale_velocity_4d_world` (DOUBLE PRECISION, NULL) \n
    - may be NULL for some items; if NULL treat item as unrankable for discovery
  - `world_upload_ts_ms` (BIGINT, NULL) \n
    - optional: last Universalis upload timestamp (ms) for data freshness
- **Source**: Universalis `GET /api/v2/aggregated/{world}/{itemIds}` (whitelist items and also ingredient items)
- **Load rule**: MERGE/upsert by PK (same day reruns should overwrite the same row)

#### `xiv_data.fact_sales_30d`

- **Grain**: 1 row per sale event
- **Primary key**: none required for overwrite-window approach (no reliable sale id)
- **Columns (NQ-only MVP)**:
  - `world_id` (INT, NOT NULL)
  - `item_id` (INT, NOT NULL)
  - `sold_at_ts` (TIMESTAMP, NOT NULL) \n
    - if you prefer, store as BIGINT ms; but pick one and stay consistent
  - `price_per_unit` (INT, NOT NULL)
  - `quantity` (INT, NOT NULL)
- **Source**: Universalis `GET /api/v2/history/{world}/{itemIds}?entriesWithin=2592000`, flattened
- **Load rule (overwrite-window, every run)**:\n
  - DELETE `WHERE world_id = :world_id AND item_id IN (:whitelist_ids) AND sold_at_ts >= (now_utc - interval '30 days')`\n
  - INSERT freshly flattened events\n
  - Optional: drop exact duplicates in the incoming batch on (`world_id`,`item_id`,`sold_at_ts`,`price_per_unit`,`quantity`)

#### `xiv_data.fact_recipe_ingredient`

- **Grain**: 1 row per `crafted_item_id` × `ingredient_item_id`
- **Primary key**: (`crafted_item_id`, `ingredient_item_id`)
- **Columns**:
  - `crafted_item_id` (INT, NOT NULL)
  - `ingredient_item_id` (INT, NOT NULL)
  - `ingredient_qty` (INT, NOT NULL)
- **Source**: XIVAPI v2 `RecipeLookup` + `Recipe` (`Ingredient@as(raw)` + `AmountIngredient[]`)
- **Load rule**: MERGE/upsert by PK

### DEVMODE cache rules (explicit)

#### Marketable cache

- If `DEVMODE == true` **and** `marketable.json` exists:
  - Load `marketable.json` and skip `GET /api/v2/marketable`
- Else:
  - Call `GET /api/v2/marketable`
  - If `DEVMODE == true`, write `marketable.json`

#### Aggregated cache

- If `DEVMODE == true` **and** `aggregated.json` exists:
  - Load `aggregated.json` and skip `GET /api/v2/aggregated/...`
- Else:
  - Call `GET /api/v2/aggregated/Louisoix/{itemIds}` in batches of 100
  - If `DEVMODE == true`, write `aggregated.json`

### Later (once whitelist works): detailed sale-event history for whitelist items

- Call `GET /api/v2/history/Louisoix/{whitelistItemIds}` (batched by 100, 30d window)
  - This is where we get sale events (`timestamp`, `pricePerUnit`, `quantity`) for charts + true 30d metrics.

# Dashboard pages to build in Power BI

- **“What to Sell Now”**: ranked table by OpportunityScore with slicers (DC → world, HQ/NQ, category if you add it later)
- **Market health**: trend of velocity/price for selected items
- **Competition view** (if listings): depth at min price, number of competitors, quantity wall

# Key implementation constraints

- Respect Universalis rate limits (25 req/s, 8 concurrent connections). Batch item IDs (max 100 per request) and throttle concurrency.
- Daily refresh means this can run as a single scheduled job (no near-real-time requirements).

