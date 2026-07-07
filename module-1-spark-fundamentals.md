# Chapter 1 — Spark Fundamentals

This chapter gets you working in a **distributed** world. We start at the command line — the **Linux** commands you'll use to organize files and folders — then meet **Apache Spark**: what it is, why "scaling out" beats "scaling up," and how Spark spreads work across many machines using **lazy evaluation**. We put it into practice with **PySpark**, building a first DataFrame program and a complete **NLP word-count pipeline** that we then aim at a whole directory of books at once. The goal is comfort: type along, break things, and get a feel for the rhythm of big-data code.

## 1.1 The command line: Linux basics

Working with big data means giving up the point-and-click habit. You'll organize files with a handful of **Linux commands**: `pwd` (print working directory), `mkdir` (make a directory), `cd` (change directory), `ls` (list files), `cp` (copy), `less` (preview a file), `rmdir` (remove an *empty* directory), and `rm -r` (remove a directory *with* contents — permanent, so be careful).

In a Colab cell, prefix a shell command with **`%`** (a "magic") so it runs in the **same** persistent session — `%cd` actually changes your directory for later cells. Using **`!`** instead spawns a fresh shell that forgets the change as soon as it finishes, so `!cd somewhere` followed by `!pwd` won't show the move. Paths are **relative to the working directory**; `.` means "here," and `..` means "up one folder." A quick idiom: `mkdir class{1..9}` scaffolds nine class folders at once instead of right-clicking your way through. Copy with `cp source dest` (note the space), preview with `less file`, and remember spaces in names cause trouble — a shell reads a space as the start of a **new argument**, so use underscores.

## 1.2 What is Spark, and why scale out?

**Apache Spark** is a *unified analytics engine for large-scale data processing*. The core idea: when data is too big for one machine, it's cheaper and more reliable to **scale out** (chain together many cheap machines) than to **scale up** (buy one enormous one). RAM is the limiting resource, and it gets disproportionately expensive at the top end — two 64 GB sticks cost less than one 128 GB stick. So instead of one heroic desktop, picture a **cluster** — "a hundred laptops crammed in a closet," each a **worker node** holding a slice of the data.

Spark is written in **Scala**, but **PySpark** gives you a Python interface — you never touch Scala. On Colab you install Spark in ~10 seconds (it fetches Java and the latest Spark tarball, sets some environment variables, and starts a session). The package you'll live in is **`pyspark.sql`**; you always begin with `spark = SparkSession.builder...getOrCreate()`, giving each session a descriptive **app name** so multiple jobs stay organized. A useful mental model of the "Spark factory": the **master** owns the resources, the **driver** requests them, the **worker nodes** are the benches (hardware), and the **executors** are the software on each bench that does the work and returns results.

## 1.3 Distributed processing and lazy evaluation

How does Spark compute, say, an average across a huge file? The **driver** ships chunks to each worker, every worker **sums and counts** its own chunk, and Spark **combines** those partial results up the tree into one final answer — no single node ever holds all the data, so you avoid memory blowups. You don't care about the intermediate sums; only the final output.

The other big shift is **lazy evaluation**. Split your code into **transformations** (add a column, aggregate, compute summaries, train a model) and **actions** (I/O — `show`, `write`, `count`). Spark **defers** all transformations, building a *recipe* of instructions, and only executes when an **action** forces it. Don't be fooled by the name — "lazy" is good. It means Spark stores cheap **instructions** instead of expensive **intermediate copies** (the way pandas would spawn `df2`, `df3`, … in memory), it can **optimize** the whole plan before running, and — because the plan is known — it can **recover from node failure** by re-sending a lost chunk to another worker. If pandas-style **eager** evaluation is your background, this feels strange at first; we'll break operations out one at a time, then chain them.

## 1.4 A first PySpark program

Alongside `SparkSession`, import the SQL functions as **`F`**: `from pyspark.sql import functions as F`. This is a readability trick — every PySpark SQL call now visibly starts with `F.` (`F.split`, `F.explode`, `F.lower`, `F.col`, `F.regexp_replace`), so a reader instantly knows where a function comes from. A tiny end-to-end example reads a CSV with a header, uses `F.when(...)` to recode a column (values over a threshold → 10, else 0), filters rows, then `groupBy(...).count()` — all **chained**, producing no temporary tables. Even before you know the API, the code reads top-to-bottom like a sentence.

## 1.5 Text as data: the NLP word-count pipeline

Text is **unstructured**, so the job is to reshape it into something spreadsheet-like a computer can count. Using *Pride and Prejudice* (a Project Gutenberg book), the pipeline is:

1. **Read** the text file with the DataFrame reader → a DataFrame `book` with one string column named `value`. Inspect it with `printSchema()`, `.dtypes`, and `show(n, truncate=...)` (or `show(vertical=True)` to see records one at a time).
2. **Tokenize**: `F.split(F.col("value"), " ")` breaks each line into a **list** of words. **Alias** the result (`.alias("line")`) — Spark auto-names outputs after the operation, which is correct but ugly, so rename for readability. Here we get ~13,000 rows (lines), each a list.
3. **Explode**: `F.explode(...)` turns each list into **one row per word** — ~127,000 words.
4. **Clean**: `F.lower(...)` lowercases, then `F.regexp_replace(word, "[^a-zA-Z]", "")` strips punctuation so `prejudice,` and `prejudice` are the same token.
5. **Drop nulls**: filter where `word != ""` — this removes ~5,000 empty rows (the blank lines / paragraph breaks left by the space split), leaving ~122,000 words.
6. **Count and sort**: `groupBy("word").count()` then `orderBy(F.col("count").desc())` → the most frequent words (the usual **stop words**: *the, to, and, her* — the ones you'd drop for a real classification task).

A key habit throughout: **select the output** after each function (or it's lost), rename it, and keep an eye on the **row/column counts** so you never silently lose data.

## 1.6 Chaining and scaling out

The step-by-step version above spawns six temporary DataFrames (`book`, `lines`, `words`, `words_lower`, …) — fine for learning, wasteful in production. The same logic **chained** into a single expression keeps just the instructions:

```python
(spark.read.text("books/*.txt")
   .select(F.split(F.col("value"), " ").alias("line"))
   .select(F.explode(F.col("line")).alias("word"))
   .select(F.lower(F.col("word")).alias("word"))
   .select(F.regexp_replace(F.col("word"), "[^a-zA-Z]", "").alias("word"))
   .where(F.col("word") != "")
   .groupBy("word").count()
   .orderBy(F.col("count").desc()))
```

Now the magic: point the reader at a **wildcard directory** (`books/*.txt`) instead of one file, and the *same* program runs across **all six books at once** in a few seconds — no `for` loop, no manual file handling. Once your intent is expressed as instructions, Spark applies it at scale. This is the payoff of chained, lazy code: readable, and it grows from one file to a gigabyte without changing.

## 1.7 Saving results: the coalesced file

Writing with the **DataFrame writer** (`results.write.csv("path")`, optionally `.mode("overwrite")`) doesn't produce a single CSV — it produces a **folder of parts** (dozens of chunks), because the data lives across the distributed file system, and the parts aren't globally ordered. That's efficient for Spark but useless for a report or dashboard. To get one file, **`coalesce(1)`** before writing collapses the output into a **single part** — much easier to hand off, at the cost of funneling everything through one node.

## 1.8 Where to go from here (Week 1)

Three bits of housekeeping to close the first week: (1) get the free course text — *Data Analysis with Python and PySpark* by **Jonathan Rioux** — via the UConn library, and keep up with the readings. (2) When building file structures, **avoid spaces** (note how Drive uses `My Drive` carefully) — use underscores. (3) **Practice EDA in Python** (selecting rows/columns, plots, tables): the *spirit* of EDA carries into PySpark, though you'll be re-creating those pandas moves at scale — and remember pandas/Matplotlib/NumPy are **eager**, unlike Spark. A cool bridge worth knowing about later: **Koalas**, a pandas-like API on Spark.

## 📌 Lecture key points

*Distilled takeaways from the video lectures behind this chapter — click each to expand.*


:::{admonition} Linux Commands (Pt. 1)
:class: note dropdown
- Distributed work means dropping point-and-click for the command line; core commands: `pwd`, `mkdir`, `cd`, `ls`, `rmdir`.
- In Colab, prefix with **`%`** to stay in the same session (`%cd` persists); **`!`** spawns a throwaway shell that forgets `cd`.
- Paths are relative to the working directory: `.` = here, `..` = up one folder; `mkdir class{1..9}` scaffolds folders fast.
- `rmdir` deletes only **empty** directories; verify with `pwd`/`ls` before destructive commands.
:::

:::{admonition} Linux Commands (Pt. 2)
:class: note dropdown
- `cp source dest` copies a file (mind the space between the two paths); the original stays put.
- `ls` lists a directory and `less file` previews it without opening the whole thing.
- Removing a **non-empty** directory needs `rm -r` — permanent, so be careful.
- On your own: mount Google Drive and build class folders with these commands.
:::

:::{admonition} What did Spark do
:class: note dropdown
- Spark computes an average by having each worker **sum + count** its chunk, then **combining** partials up the tree — no node holds all the data, avoiding memory issues.
- Split code into **transformations** (add column, aggregate, train) and **actions** (`show`, `write`, `count` = I/O).
- **Lazy** evaluation stores cheap instructions, not expensive intermediate copies; "lazy" is a good thing, not a flaw.
- Benefits: less memory, whole-plan optimization, and fault tolerance (re-send a lost chunk to another worker).
:::

:::{admonition} What is Spark
:class: note dropdown
- Spark is a **unified analytics engine for large-scale data**; PySpark is its Python interface (Spark itself is Scala).
- **Scale out** (many cheap machines) beats **scale up** (one huge machine) — RAM gets disproportionately expensive.
- On Colab, install Spark (~10s), then `spark = SparkSession.builder...` with a descriptive **app name**; live in **`pyspark.sql`**.
- Factory model: **master** owns resources, **driver** requests them, **worker nodes** are hardware, **executors** do the work.
- Import functions as **`F`** so every PySpark SQL call is self-documenting.
:::

:::{admonition} Chaining and scaling out
:class: note dropdown
- The same logic written as **chained** operations replaces six temporary DataFrames with one readable expression.
- **Select the output** after each function or the result is lost; any `F.`-prefixed call comes from PySpark SQL.
- Point the reader at a **wildcard** (`books/*.txt`) and the identical program runs across every file — no `for` loop.
- Use `ls`/`mv`/`wc` to organize the corpus (underscores, not spaces) before firing the job at the whole directory.
:::

:::{admonition} Grouping data and saving a coalesced file
:class: note dropdown
- `groupBy(col).count()` aggregates; add `orderBy(..., ascending=False)` to sort high→low (distributed output is unordered otherwise).
- Writing with the **DataFrame writer** produces a **folder of parts**, not one CSV, and parts aren't globally ordered.
- **`coalesce(1)`** before `write` collapses output into a single file for reports/dashboards.
- Use `.mode("overwrite")` to replace an existing path, or Spark errors that it already exists.
:::

:::{admonition} Intro to NLP with Spark (Pt. 1)
:class: note dropdown
- Text is unstructured; the goal is to reshape it into countable, spreadsheet-like rows.
- Steps: **tokenize → clean (lowercase, strip punctuation) → count → sort** for most-frequent words.
- The DataFrame reader opens many formats (CSV, JSON, **Parquet**, text); `read.text` gives one string column `value`.
- Inspect with `printSchema()`, `.dtypes`, and `show(n, truncate=…, vertical=…)`.
- RDDs track individual rows; the newer **DataFrame** API focuses on column elements — we use DataFrames.
:::

:::{admonition} Intro to NLP with Spark (Pt. 2)
:class: note dropdown
- `F.split(F.col("value"), " ")` tokenizes each line into a **list**; **`.alias()`** renames the ugly auto-generated output.
- Select columns via `select`, bracket, or dot notation; a selected column is itself a DataFrame.
- **`F.explode`** turns each list into **one row per word** (~13k lines → ~127k words).
- Watch row/column counts at every step so you don't silently lose data.
:::

:::{admonition} Intro to NLP with Spark (Pt. 3)
:class: note dropdown
- Cleanup: `F.lower(...)` then `F.regexp_replace(word, "[^a-zA-Z]", "")` so `prejudice,` == `prejudice`.
- Filter `word != ""` to drop ~5,000 blank rows (the paragraph breaks from the space split) → ~122k words.
- Row count stays the same after cleaning; only the **contents** of each row change.
- This walkthrough is deliberately **eager** (many temp DataFrames) for learning; we'll tighten it into chained code later.
:::

:::{admonition} Where do we go from here (Week 1)
:class: note dropdown
- Get the free text — **Rioux, *Data Analysis with Python and PySpark*** — via the UConn library and keep up with readings.
- Build file structures **without spaces** (use underscores), mirroring how Drive names things.
- **Practice Python EDA** (rows/columns, plots, tables) — the spirit carries into PySpark, but pandas/Matplotlib are **eager**.
- **Koalas** (a pandas-like API on Spark) is worth exploring later for big-data EDA.
:::
