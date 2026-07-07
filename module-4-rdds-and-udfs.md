# Chapter 4 — RDDs & UDFs

So far we've leaned on Spark's built-in DataFrame functions. This chapter extends PySpark with **your own Python code**, two ways. **RDDs** (resilient distributed datasets) are flexible, schema-less bags of elements processed with the **higher-order functions** `map`, `filter`, and `reduce` — the classic **MapReduce** idea. **UDFs** (user-defined functions) promote a plain Python function to work on **DataFrame columns**, and **pandas UDFs** make that lightning-fast by operating on whole pandas Series at once. We meet the **Parquet** file format along the way, and finish with **grouped** (split-apply-combine) UDFs.

## 4.1 RDDs and `map`

An **RDD** is an unordered collection of independent objects — no columns, no schema, "a bag of elements" you can scatter to worker nodes, apply a function to, and collect back. Because it's schema-less, an RDD can hold **mixed types** (an integer, string, float, tuple, and dict together) that no DataFrame column would allow. RDDs come from the **SparkContext** (aliased **`sc`**, not the `spark` SQL session): `rdd = sc.parallelize(my_list)`.

You process an RDD with **higher-order functions** — functions that take a *function* as input. The first is **`map`**, which applies a Python function to **every element**. Crucially, the function is **plain Python**, not PySpark: `def add1(v): return v + 1`. But apply `add1` to a mixed-type RDD and it **fails only when collected** — a `TypeError` on the string, tuple, and dict (you can't add 1 to those). RDDs give you **no safeguards**, so you bake in your own error handling with **`try/except`** (and `assert`s to catch bugs). That flexibility-for-safety trade-off is the defining feature of RDDs.

## 4.2 `filter` and `reduce`

Two more higher-order functions:

- **`filter`** keeps elements satisfying a **predicate**. It pairs naturally with a **lambda** (anonymous, run-once function): `collection.filter(lambda x: isinstance(x, (int, float)))` keeps only numbers — no `try/except` needed. Remember it's lazy until you **`.collect()`**.
- **`reduce`** takes **two elements, applies a function, returns one**, chunking through the data (e.g. summing `[4, 7, 9, 1, 3]` by folding pairwise). For `reduce` to work **in a distributed world**, the function must be **commutative** (order doesn't matter — true for `+`, false for `−`) and **associative** (grouping doesn't matter). Addition qualifies; subtraction doesn't — so choose reduce functions carefully.

RDDs are powerful and still lurk under the hood, but the course — and the field — is moving toward **DataFrame** functionality, including pandas-like operations at Spark speed.

## 4.3 UDFs

A **UDF** promotes a regular Python function to run on **PySpark columns**. Write the function in three careful parts: (1) **document** it with a docstring, (2) mind the **input and output types** (type errors are deadly at scale), and (3) **test** it with `assert`s. The book's example reduces fractions (a `(numerator, denominator)` tuple) and converts them to floats.

Two ways to create a UDF:
- **Explicit** — `reduce_udf = F.udf(py_reduce_fraction, T.ArrayType(T.LongType()))`, passing the return type, then use it like any column function inside `withColumn`/`select`.
- **Decorator** — cleaner and more compact: put **`@F.pandas_udf(T.DoubleType())`** (or `@F.udf(...)`) above the function, and the UDF inherits its name.

Note the return types differ by data: an array of `LongType` for integer fractions, `DoubleType` for a float. UDFs are the powerful tool for **column transformations**; next we scale them with pandas.

## 4.4 pandas UDFs and Parquet

A **pandas UDF** leverages pandas *and* a performance boost: instead of processing **row by row**, it converts a column to a **pandas Series** and processes it **all at once**, then converts back to a PySpark column. It relies on **PyArrow** (pre-installed on Colab; install it yourself locally) to track records so work can be split across nodes and reassembled.

We also meet a new file format: **Parquet** — an open-source, **columnar** format that's dramatically more efficient than CSV. A terabyte of CSV becomes ~130 GB (**87% less storage**), and queries are **~34% faster** scanning **99% less data**, because Parquet reads only the **columns needed** rather than the whole file. The chapter's data is **GSOD** daily weather observations (station × year × month × day), read from three yearly Parquet files unioned with `functools.reduce` (~11 million rows).

(Two practical notes from the recording: Spark's download URL can go stale — check the Apache distributions and point Colab at the right tarball/folder; and `distinct`-count operations on grouped data can be very slow, so check your available CPUs/cores.)

## 4.5 The three flavors of pandas UDFs

1. **Series → Series** — a column function with pandas, faster than a plain UDF because it processes the whole Series. Use the **`@F.pandas_udf(T.DecimalType())`** decorator; the signature takes a `pd.Series` and returns one (e.g. Fahrenheit → Celsius, `(temp − 32) * 5/9`), applied with `withColumn`.
2. **Iterator of Series → Iterator of Series** — the same one-to-one transform, but for an **expensive cold-start** operation (e.g. deserializing an ML model). The signature switches to iterators; you `yield` results. Simulating the cost with a 5-second `sleep`, this run took ~19s vs. ~10s for the plain Series UDF — the iterator has setup overhead that pays off only for genuinely costly per-partition work.
3. **Iterator of *multiple* Series → Iterator of Series** — for **multiple input columns**. The signature is an *iterator of a tuple of* Series. To combine GSOD's separate `year`, `month`, `day` columns into one `date`, zip the three Series in order (order matters — no keyword args), build a datetime, and `yield` the result. PyArrow keeps the three columns aligned across nodes.

## 4.6 Grouped UDFs: split-apply-combine

Two flavors of UDFs on **grouped** data:

- **Grouped aggregate (Series → scalar)** — condense each group to a single value, placed inside **`.agg(...)`** after a `groupBy`. Beyond built-ins, you can call, say, **scikit-learn**: for each station-year-month group, fit a linear regression of temperature on day-of-month and return the **slope** (is it warming or cooling?). Shuffled rows are fine as long as day↔temperature pairs stay aligned. A gut check: ~12,000 stations × 36 months ≈ 390k rows — and the output was ~390k rows across 4 columns (within ~10%, the gap being incomplete data — knocked-over stations, bird nests).
- **Grouped map (split-apply-combine)** — split the DataFrame into per-group batches, apply a function that takes and returns a **whole DataFrame**, then recombine. Use **`.apply(scale_temperature, schema)`** (not `agg`) with an explicit output schema. Example: normalize each month's temperatures to [0, 1] per station-year-month; guard the min == max case (0/0) by recoding to **0.5**. The output keeps the **same row count**, transformed per group.

**What to use where:** group + single condensed value → **grouped aggregate**; transform keeping the same rows, order-independent → **grouped map**; pandas ops on the whole frame → map-in-pandas; single-column transform → Series-to-Series; expensive cold start → the iterator variants. Order-dependent, sliding-window work needs **window functions** — the next chapter.

## 📌 Lecture key points

*Distilled takeaways from the video lectures behind this chapter — click each to expand.*


:::{admonition} Introduction to RDDs and higher-order functions
:class: note dropdown
- An **RDD** is a schema-less "bag of elements" that can hold **mixed types**, created via `sc.parallelize(...)` from the **SparkContext**.
- **Higher-order functions** take a function as input; **`map`** applies a plain-Python function to every element.
- RDDs give **no safeguards** — mixed-type failures surface only on `collect()`, so add `try/except` and `assert`s.
- RDDs are flexible and fast but the field is moving toward **DataFrames** and UDFs.
:::

:::{admonition} Two other higher-order functions (filter and reduce)
:class: note dropdown
- **`filter`** keeps elements satisfying a **predicate**, often via a run-once **lambda**; lazy until `.collect()`.
- **`reduce`** folds **two elements into one** repeatedly, chunking across nodes.
- For distributed `reduce`, the function must be **commutative** and **associative** (`+` qualifies, `−` does not).
- No `try/except` needed for `filter` on a clean predicate.
:::

:::{admonition} Introduction to UDFs
:class: note dropdown
- A **UDF** promotes a plain Python function to run on **PySpark columns**.
- Write it in three parts: **docstring**, careful **input/output types**, and **`assert`** tests.
- Create explicitly with **`F.udf(fn, returnType)`** or with the **`@decorator`** (cleaner; inherits the function name).
- Return types must match the data (e.g. `ArrayType(LongType)` for int fractions, `DoubleType` for floats).
:::

:::{admonition} Intro to pandas UDFs and Parquet files
:class: note dropdown
- A **pandas UDF** processes a whole **Series at once** (not row-by-row), a big speed-up, backed by **PyArrow**.
- **Parquet** is a columnar format: ~87% less storage than CSV, ~34% faster queries reading only needed columns.
- Read multiple Parquet files and union them with `functools.reduce` (GSOD weather, ~11M rows).
- Practical snags: Spark's download URL can go stale; grouped `distinct` counts can be very slow.
:::

:::{admonition} Three flavors of pandas UDFs
:class: note dropdown
- **Series → Series**: a fast column transform (e.g. °F → °C) via the `@pandas_udf(type)` decorator.
- **Iterator of Series → Iterator of Series**: same one-to-one transform, but for an **expensive cold start**; you `yield`.
- **Iterator of *multiple* Series → Series**: combine several columns (e.g. year/month/day → date); zip **in order**.
- Order matters for multi-column UDFs — no keyword args; PyArrow keeps columns aligned across nodes.
:::

:::{admonition} split-apply-combine or UDFs for grouped data
:class: note dropdown
- **Grouped aggregate (Series → scalar)** condenses each group to one value inside `.agg(...)` — e.g. a scikit-learn regression **slope**.
- Gut check: ~12,000 stations × 36 months ≈ 390k rows (minus incomplete data).
- **Grouped map (split-apply-combine)** applies a DataFrame→DataFrame function per group via **`.apply(fn, schema)`**, keeping the row count.
- Guard divide-by-zero (min == max → recode to 0.5); order-dependent/sliding work needs **window functions** (next chapter).
:::
