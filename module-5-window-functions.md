# Chapter 5 — Window Functions

**Window functions** compute a value over a *portion* of a DataFrame — a **window frame** — without collapsing rows the way a `groupBy` does. They're the precursor to **time-series feature engineering** at scale: rolling averages, rankings, lags, and running summaries over massive data. This chapter covers the window spec (`partitionBy` / `orderBy`), the analytical functions (`lag`, `lead`, `cume_dist`, `percent_rank`), the three window shapes (**static, growing, unbounded**) by **rows vs. ranges**, the ranking functions, and finally **UDFs over windows**. We use the **GSOD** weather data (12M rows) plus a tiny 10-row `gsod_light` for hand-checking.

*(Install note: Spark's hosted tarball kept moving and breaking Java setup, so from here on just use **`pip install pyspark`** — it wires everything up behind the scenes, no paths or environment variables.)*

## 5.1 Why windows: the coldest-day problem

Suppose you want the coldest day of the year *with* its month, day, station, and temperature. The **long way**: `groupBy("year").agg(F.min("temp"))` gives the minimum but **loses the other columns**, so you'd join that back to the 12M rows — a **self-join** (left semi-join) on year + temperature, then order and select. It works, but it's clunky and slow. As the book's mantra goes: *get it to work, get it clean, get it fast.*

The **window** way skips the join. Build a **window spec** — `from pyspark.sql.window import Window`, then `w = Window.partitionBy("year")` — which tells Spark how to split rows across nodes. Compute `F.min("temp").over(w)`, which **broadcasts** each year's minimum onto every one of its ~4M rows, then filter where `temp == min_temp` and select your columns. No join, cleaner, faster. A window can partition on **more than one column** too (`partitionBy("year", "month")` → the min per year-month, 36 rows). Think of a window function as **a method applied to a column over a partition**.

## 5.2 Analytical functions: lag/lead and cume_dist vs. percent_rank

**`lag`** and **`lead`** shift a column up or down within the window — invaluable for **feature engineering without data leakage**. If you're predicting the next five days' temperature, including *today's* value leaks the future; `lag` lets you reference only **past** observations safely. Always `orderBy` inside the window (else distributed data is out of order), and the partition prevents leaking across years/stations. Leading/lagging rows produce **nulls** at the boundary (you can supply a default like 0 or 999).

Two normalized-rank functions, easy to confuse:
- **`cume_dist`** (cumulative distribution) = (records with value **≤** current) / (records in window). Intuitive; good for reaching the **maximum**.
- **`percent_rank`** = (records with value **strictly <** current) / (records in window **− 1**). Note the `− 1` in the denominator; good for reaching **0/min**. For four ascending values it gives 0, 0.33, 0.66, 1.

## 5.3 Static, growing, and unbounded windows (rows vs. ranges)

The key differentiator is **ordering**. An **unordered** window (just `partitionBy`) applies a **constant** aggregate to every row in the partition — implicitly `rowsBetween(unboundedPreceding, unboundedFollowing)` (the whole partition). Add an **`orderBy`** and you get a **growing** window — `rangeBetween(unboundedPreceding, currentRow)` — so a running average grows row by row (e.g. the "not ordered" average is constant, while the ordered one climbs). You can be explicit with **`rowsBetween`** / **`rangeBetween`** using `Window.unboundedPreceding`, `Window.currentRow`, `Window.unboundedFollowing`.

**Rows vs. ranges** = *where you are* vs. *what you are*. **Row** windows tie the frame to row **positions** (good when there's no time). **Range** windows tie it to the **value** of the current row — ideal for **dates/times**. For a **60-day sliding average**, build a Unix timestamp (seconds since 1970-01-01), `orderBy` it, and use `rangeBetween(-60_days_in_seconds, +60_days_in_seconds)` (~2.5M seconds ≈ a month), so each record averages the observations within ±one month — regardless of whether a month has 28/29/30/31 days.

## 5.4 Summarizing and ranking

Beyond aggregates, windows **rank** rows. The ranking functions:
- **`rank`** — "Olympic" ranking: ties share a rank, then it **skips** (1, 1, 3).
- **`dense_rank`** — no gaps after ties (1, 1, 2).
- **`percent_rank`** — normalized rank (0 = min, 1 = max), with the `n − 1` denominator.
- **`ntile(n)`** — splits into n buckets (2-tile halves, 4-tile quartiles, 10-tile deciles, 100-tile percentiles); with tiny data, boundary overlaps land in the earlier bucket, so it can look lopsided.
- **`row_number`** — a strict increasing position, ties broken arbitrarily (less robust than `rank`).

`orderBy` defaults to **ascending**; for "top hottest" queries chain **`.desc()`** on the order column. As always, `partitionBy` sets the group (e.g. per month or per year) and `orderBy` sets the ranking column.

## 5.5 UDFs over windows, and summary

You can apply a **UDF over a window** — a **series-to-scalar** pandas UDF that runs once per record and returns a scalar. (Note: unbounded-window UDFs need Spark ≥ 2.4, bounded-window UDFs need Spark ≥ 3.0 — we're on 3.3, so both are fine.) Declare it with the decorator (e.g. a `median` function taking a `pd.Series` → float), then apply inline with **`.over(Window.partitionBy("year"))`** — unordered gives a constant median per year, while adding `orderBy("month", "day")` makes it a **growing** median. This sets up the feature-engineering work ahead.

**Summary:** a window function runs over a **window frame** defined by a **window spec** — `partitionBy` (how to split), `orderBy` (how to order), and optionally `rowsBetween`/`rangeBetween` (how to bound). By default an **unordered** frame is **unbounded** (constant aggregate); an **ordered** frame **grows** left-to-right (first row → current row). Bounds can be by **row** (positional) or **range** (value-based, ideal for time). Windows, UDFs, blended PySpark/SQL, and JSON all come together in the course project.

## 📌 Lecture key points

*Distilled takeaways from the video lectures behind this chapter — click each to expand.*


:::{admonition} Introduction to Window Functions
:class: note dropdown
- Window functions compute over a **partition** without collapsing rows — the precursor to time-series feature engineering.
- The "coldest day with details" problem needs a clunky **self-join** the long way; a **window** avoids it.
- Build a spec with `Window.partitionBy(...)`, compute `F.min("temp").over(w)` (broadcast per partition), then filter.
- Partition on **multiple columns** for per-year-month results; from here, install Spark with **`pip install pyspark`**.
:::

:::{admonition} Analytical functions (lag, lead, cume_dist vs. percent_rank)
:class: note dropdown
- **`lag`/`lead`** shift a column within the window — key for feature engineering **without data leakage** (no future info).
- Always `orderBy` inside the window; boundary rows produce **nulls** (or a supplied default).
- **`cume_dist`** = records **≤** current / records in window (intuitive; reaches the max).
- **`percent_rank`** = records **<** current / (records **− 1**) (reaches 0/min); e.g. 0, 0.33, 0.66, 1.
:::

:::{admonition} Static, growing, unbounded windows
:class: note dropdown
- **Ordering** is the switch: unordered → **constant** aggregate (whole partition); ordered → **growing** window (first → current row).
- Be explicit with **`rowsBetween`/`rangeBetween`** and `unboundedPreceding`/`currentRow`/`unboundedFollowing`.
- **Row** windows are positional; **range** windows are value-based — use ranges for **dates/times**.
- A 60-day sliding average uses a **Unix timestamp** + `rangeBetween(±~2.5M seconds)`, robust to variable month lengths.
:::

:::{admonition} Summarizing, ranking, and analyzing window functions
:class: note dropdown
- **`rank`** skips after ties (1,1,3); **`dense_rank`** doesn't (1,1,2).
- **`percent_rank`** normalizes 0→1; **`ntile(n)`** buckets (quartiles, deciles, percentiles) — boundaries land in the earlier bucket on tiny data.
- **`row_number`** gives a strict position (ties broken arbitrarily; less robust than `rank`).
- `orderBy` is **ascending** by default — chain **`.desc()`** for top/extreme-value queries.
:::

:::{admonition} UDFs and Windows Chapter Summary
:class: note dropdown
- Apply a **series-to-scalar UDF over a window** with `.over(Window.partitionBy(...))` — one scalar per record.
- Unbounded-window UDFs need Spark ≥ 2.4, bounded ≥ 3.0 (we're on 3.3).
- Unordered → constant per partition; adding `orderBy` makes it a **growing** window.
- A window spec = `partitionBy` (split) + `orderBy` (order) + optional `rowsBetween`/`rangeBetween` (bound).
:::
