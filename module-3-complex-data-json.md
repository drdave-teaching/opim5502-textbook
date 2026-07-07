# Chapter 3 — Complex Data: JSON, Arrays, Maps & Schemas

Real-world data isn't always a flat table. This chapter tackles **JSON** — the nested, hierarchical format you meet when pulling from web APIs — and the **complex column types** Spark uses to represent it: **arrays**, **maps**, and **structs**. We learn the array functions, how structs nest columns, how to **impose a schema** for speed and data governance, and finally work a messy real-world **Senators JSON** end to end. Using TV-show data (Silicon Valley, Breaking Bad, Golden Girls), we flatten dense nested files into the 2-D tables that downstream tools expect.

## 3.1 What is JSON?

**JSON** (JavaScript Object Notation) is the dominant format for exchanging data between a client and a server, so most web data runs on it. Mentally, a JSON document is a **limited Python dictionary**: it opens with a curly brace (an **object**), holds **key–value pairs** where the **keys are always strings**, and the values can be numbers, booleans, strings, null, **arrays** (square brackets, like a Python list), or nested **objects**. That nesting is the point: think of a JSON file as a spreadsheet where a column can itself hold another spreadsheet.

The payoff over CSV is **efficiency — no duplicated data**. A show's ID and URL are stored **once**, with the 53 episodes nested underneath, whereas a flat CSV keyed on episode would repeat the show ID and link 53 times. These show files are tiny (~half a MB) but **dense**. A key quirk: even a file packed with 53 episodes is a **single line** of text — compact and efficient storage, ideal for databases.

## 3.2 Array operations

Spark represents a JSON array as an **array column** — multiple elements that must all be the **same type**. PySpark's array functions mostly carry the **`array_` prefix**:

- **`array_distinct`** — the unique elements of an array (e.g. `[comedy, horror, comedy, drama, comedy]` → `[comedy, horror, drama]`); conceptually like a per-row `distinct`.
- **`array_intersect(a, b)`** — what two array columns have in common (similar in spirit to pandas `isin`), typically used inside `select(...).alias("genres")`.
- **`array_position(col, value)`** — the index of an element. **Important:** positions count from **1, not 0** — `horror` in `[comedy, horror]` returns **2**.

(A live-coding aside worth remembering: aliasing a column and then referencing its *original* name causes "column not found" errors — keep track of what you've renamed.)

## 3.3 Maps and structs

A **map** is close to a Python dictionary but **typed**: all keys share one type and all values share one type (key-type and value-type may differ). Build one from two array columns with **`map_from_arrays(keys, values)`**. Maps are optional here since the show data doesn't use them.

The workhorse is the **struct** — for **nesting columns within a column**. Unlike an array (same-typed, key-less elements), a struct is like a JSON object: named fields (string keys) whose values can be **different types**, including arrays *and* single strings. The show data has structs such as `schedule` and `ratings`. Navigate into one with dotted `select` (e.g. `show.embedded`), and clean up the annoying nesting: the `_embedded` struct here holds a single element, so you **select `episodes`**, pull the fields out from under `_embedded`, and **drop** the now-useless struct to get a shallower layout. To turn a struct's array field into rows, **`explode`** it — e.g. exploding episode summaries into one row each, then `count()` confirms **53 episodes** in Silicon Valley (matching reality). This is the recurring goal: flatten an elegant-but-tricky nested behemoth into a **business-friendly 2-D file**.

## 3.4 Schemas

Accepting Spark's inferred schema for JSON works, but being **explicit** is faster and enforces **data governance** — clean, standardized types instead of everything falling back to strings on dirty data. Import the types module as **`T`** (alongside functions as `F`). Types use camelCase: `LongType`, `StringType`, `DateType`, `TimestampType`, and `DecimalType` (the only one taking arguments — precision and scale). Compose complex types with **`ArrayType(StringType())`**, and build records from **`StructType`** + **`StructField(name, dataType, nullable)`** (plus optional metadata, revisited in ch. 13).

A powerful trick: define the fiddly sub-schemas first and **inherit** them by variable. The `episode_link_schema` (a struct → `self` struct → `href` string) and `episode_image_schema` (medium/original hyperlink strings) look gnarly out of context, but once defined they slot cleanly into `episode_schema`, keeping that code readable — and you can cast `airdate` to a real **`DateType`** and `airstamp` to **`TimestampType`**. Because this file nests everything under `_embedded`, wrap it once more in an `embedded_schema`, then read with **`schema=embedded_schema`** and **`mode="FAILFAST"`** so a schema mismatch **errors immediately** rather than silently coercing to strings. The result reads fast and clean over just the useful subset, and dates/timestamps come back correctly formatted. (Handy along the way: **f-strings** for quick string substitution when selecting columns.)

## 3.5 A real-world example: Senators JSON

Working a genuine **Senators JSON** file surfaces the practical snags:

- **"Corrupt record" on read** — the data was all there, but the file is **multi-line** (top-level metadata, then everything under an `objects` key). Adding **`multiLine=True`** fixes it. Expect this on other real JSON files.
- **The double-explode trap** — exploding **two** array columns at once does a **cross join**: 100 senators × 100 addresses = **10,000** rows. Exploding one gives the right 100. The fix is **`arrays_zip`** to pair party and office **together** before a single explode.
- **Renaming struct keys** — a zipped struct comes out with fields named `0` and `1`; rename them by applying an updated schema, then drop the struct and re-select the individual columns for a tidy 100-row table.
- **String wrangling** — office strings mix numbers and building names, so **`split`** on spaces, pull items one at a time (no slicing on `getItem`), and **`concat`** them back with `F.lit(" ")` literals. Group by building and you learn the Russell/Hart/Dirksen party breakdowns.

The honest lesson Dave shares: one question took an hour or two, because **not every function is in the book** — you search your resources, read the schema constantly, and practice. Structs and JSON are the hardest data to work with, which is exactly why they're worth practicing before a web-scraping project.

## 📌 Lecture key points

*Distilled takeaways from the video lectures behind this chapter — click each to expand.*


:::{admonition} Introduction to JSON files
:class: note dropdown
- A JSON document is a **limited Python dictionary**: opens with `{`, holds **key–value pairs**, keys are always strings.
- Values can be numbers, booleans, strings, null, **arrays** (`[...]`, like a list), or nested **objects** (`{...}`).
- JSON is efficient — shared data (show ID, URL) is stored **once** with episodes nested, vs. a CSV repeating it per row.
- Even a file with 53 episodes is a **single dense line** of text — compact storage, great for databases.
:::

:::{admonition} Array operations (distinct, intersect, position)
:class: note dropdown
- A Spark **array column** holds multiple elements that must all be the **same type**.
- **`array_distinct`** returns unique elements; **`array_intersect(a, b)`** returns what two array columns share (like `isin`).
- **`array_position(col, value)`** counts from **1, not 0**.
- Watch aliasing: referencing a column by its pre-alias name causes "column not found" errors.
:::

:::{admonition} Maps and structs
:class: note dropdown
- A **map** is a typed dictionary (uniform key type, uniform value type); build with **`map_from_arrays`**.
- A **struct** nests **named fields of mixed types** (like a JSON object) — e.g. `schedule`, `ratings`.
- Navigate with dotted `select` (`show.embedded`); drop single-element structs to flatten the layout.
- **`explode`** turns a struct's array field into rows; `count()` confirmed 53 Silicon Valley episodes.
:::

:::{admonition} Schemas
:class: note dropdown
- Explicit schemas are **faster** and enforce **data governance** (types instead of everything-as-string on dirty data).
- Import types as **`T`**; use `StructType`/`StructField(name, type, nullable)`, `ArrayType`, `DateType`, `TimestampType`, `DecimalType`.
- Define fiddly sub-schemas first and **inherit** them by variable to keep the main schema readable.
- Read with `schema=...` and **`mode="FAILFAST"`** so mismatches error immediately instead of coercing to strings.
:::

:::{admonition} Playing with Senators JSON
:class: note dropdown
- A "corrupt record" on a valid file usually means it's **multi-line** — add **`multiLine=True`**.
- Exploding **two** arrays at once does a **cross join** (100×100 = 10,000); use **`arrays_zip`** then a single `explode`.
- Zipped structs get fields named `0`/`1` — rename via an updated schema, then drop and re-select.
- Real work needs `split`, `getItem`, `concat`, and `F.lit(...)`; read the schema constantly and expect to search beyond the book.
:::
