# Chapter 2 — DataFrames: EDA, Joins & SQL

This chapter moves from text to **structured, spreadsheet data**. We read a big CSV, perform an **exploratory data analysis** (select, drop, rename, and engineer columns; describe and summarize), then learn to **join** DataFrames on a common key — culminating in a "monstrous" chained query that answers a real business question. Then we switch languages: registering DataFrames as **SQL views**, comparing **PySpark vs. SQL** side by side, wrangling the multi-file **Backblaze** hard-drive dataset, and finally **blending** the two so you can use the best of each.

## 2.1 Reading a CSV and first looks

Spark infers types from a Python list of lists (`long` ≈ integer, `double` ≈ float, `string`), but real data comes from files. Reading a CSV means specifying the **column delimiter** (comma, semicolon, or bar `|`), the **header**, and optionally `inferSchema` and a `timestampFormat`:

```python
logs = (spark.read
        .csv(path, sep="|", header=True, inferSchema=True,
             timestampFormat="yyyy-MM-dd"))
```

The example dataset is a broadcaster's **broadcast logs** — ~238,000 rows × 30 columns. A habit worth keeping (not in the book): always check the **row and column counts** so you can anticipate the shape after downstream joins and filters. `printSchema()` shows the 30 columns and their types — integers, strings, and a `timestamp` for the log date. Note `duration` arrives as a **string**, which we'll fix.

## 2.2 Selecting, dropping, renaming, engineering columns

`select` returns a subset of columns (via strings, a list with `*` unpacking, or `F.col(...)`), and remember to **`.show()`** or the result is just a deferred instruction. To **drop**, you must **overwrite** the DataFrame (`logs = logs.drop("a", "b")`) — like pandas without `inplace=True` — and verify by checking the column count fell (30 → 28).

The fun part is **feature engineering** the string `duration` ("HH:MM:SS…"). Use `substring` to pull the first two characters (hours), the next two (minutes), and the seconds, **cast** each to integer, and **`.alias`** them. Then convert to a common unit and sum: `hours*3600 + minutes*60 + seconds` → a new `duration_seconds` column added with **`withColumn`**. Along the way you can `withColumnRenamed`, lowercase all names, or reorder columns alphabetically — but each is only *temporary output* until you overwrite the DataFrame.

## 2.3 Describing data

Two numeric-summary tools: **`describe()`** gives count/mean/stddev/min/max (mean and stddev are blank for string columns, which still show alphabetical min/max), and **`summary()`** adds the 25/50/75 percentiles and accepts custom ones (e.g. `summary("min","10%","90%","max")`). Both need a trailing `.show()`. The recurring theme: think of PySpark as a tool for building **intermediate reporting tables** at scale, which you then hand to Matplotlib, pandas, Power BI, or Tableau for the actual plotting.

## 2.4 Joining DataFrames

Joining works like SQL/pandas/R: a **left** table, a **right** table, a **predicate** (the matching condition), and a **how** (the method). Of the seven join types, the default and most common is the **inner join** (keep only matching keys). Joining `logs` to a `log_identifier` reference table on the shared `log_service_id` grows the data to ~339,000 rows × 33 columns — note the **row count rises** because a key can match multiple rows (duplicates).

Two wrinkles:
- **Multiple predicates:** combine conditions with `&` (AND) and `|` (OR), each condition in parentheses.
- **Different column names:** if the key is `key1` on the left and `key2` on the right, the join duplicates the column and Spark then can't tell which you mean. The fix is **aliasing** — name the tables `.alias("left")` and `.alias("right")`, join on the predicate, then drop the duplicate by its home table (`F.col("right.log_service_id")`). There's no `keep_first` like pandas; you drop explicitly.

Chained, a multi-join reads cleanly: join `logs` to `log_identifier`, then **left-join** `cd_category` on `category_id`, then left-join `cd_program_class` on `program_id`, growing to 37 columns.

## 2.5 The "monstrous" query

With `full_log` assembled (four tables, ~339k × 37), we answer: *which programs are mostly commercials?* Group by two columns (`program_class` and its description) and **`agg`** to sum `duration_seconds` (aliased `duration_total`), sorting high to low; `agg` can do many aggregations at once (min/max/mean/sum/count — anything a pivot table does). Then per `log_identifier`, sum the duration **only where** the program class is one of eight commercial codes (`F.when(...).otherwise(0)`) as `duration_commercial`, and compute a **`commercial_ratio`** = commercial ÷ total with `withColumn`. Order high to low and you find programs that are *all* commercials. Two null totals appear — either **`dropna()`** them (446 → 444 rows) or **`fillna(999)`** to make them obvious. Finally, **`.toPandas()`** converts the small summary table to a pandas DataFrame you can plot with Matplotlib — the canonical PySpark workflow: crunch big data into a summary, then visualize elsewhere.

## 2.6 Registering views and SQL: UNION vs JOIN

SQL and PySpark variables live in **separate namespaces** — to query a DataFrame with SQL you must first register it via the **Spark catalog** (`createOrReplaceTempView`). You can even create a view *inside* a SQL string (`CREATE OR REPLACE TEMP VIEW drive_days AS SELECT model, COUNT(*) ... GROUP BY model`), storing the **instructions**, not the data. **UNION** appends rows (concatenate), while **JOIN** extends columns — two different axes. A join in SQL (`SELECT ... FROM drive_days JOIN failures ON drive_days.model = failures.model`) is more verbose than the PySpark equivalent (`drive_days.join(failures, "model", "left")`) — "pick your poison," though PySpark often reads cleaner here.

## 2.7 The Backblaze dataset

The **Backblaze** hard-drive-failure data is large and split across many files. Use **`wget`** to grab four ~500 MB quarterly zips, **`unzip -d`** each into its own folder (naming them uniformly `Q1`–`Q4`), delete the junk Mac subdirectory and the zips to reclaim space. Each quarter unzips to ~3 GB (~92 daily CSVs), ~12 GB total, ~40 million rows. Read all files in a folder at once (no glob, no loop), and handle the real-world snag that **Q4 has two extra columns** by adding literal `None` string columns to the others so a **`union`** aligns. Cast `smart*` columns to numeric, and register the result as a view for SQL.

## 2.8 PySpark vs. SQL, and blending them

Side by side on essential EDA — **select/where**, **group by/order by**, and **having** (filter *after* grouping) — the two languages differ mainly in **order of operations**: PySpark chains *source → transform → action*, while SQL front-loads the `SELECT`. SQL niceties: reference columns by **index** in `GROUP BY`/`ORDER BY`, and name computed columns with a bare space (`... AS max_gb`). `HAVING` cleans up post-aggregation issues (e.g. drives reporting more than one capacity, or a negative capacity).

Best of both: three DataFrame methods accept SQL snippets — **`selectExpr`**, **`expr`**, and **`where`/`filter`**. A reliability function reads all files with **`functools.reduce`** to keep only the **common columns** (four clean lines, no manual `None` padding), computes **drive_days** and **failures** as grouped DataFrames, joins them (`fillna(0)`), derives a **`failure_rate`** column, and uses a `where("capacity BETWEEN ... AND ...")` SQL clause with string substitution and a ±25% tolerance to return the top-3 most reliable drives for a given capacity. The takeaway: lean on **`where(...)`** SQL for readable range filters, and PySpark for clean joins — mix freely.

:::{admonition} 🔗 Notebooks for this chapter
:class: seealso dropdown

- **Ch4 — reading CSVs, describe, columns** &nbsp; [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/drdave-teaching/OPIM5502-notebooks/blob/master/Module2_ETL_PySpark/M2_2_PySpark_EDA_on_spreadsheets/Ch4.ipynb) &nbsp; [GitHub](https://github.com/drdave-teaching/OPIM5502-notebooks/blob/master/Module2_ETL_PySpark/M2_2_PySpark_EDA_on_spreadsheets/Ch4.ipynb)
- **Ch5 — joins & the monstrous query** &nbsp; [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/drdave-teaching/OPIM5502-notebooks/blob/master/Module2_ETL_PySpark/M2_2_PySpark_EDA_on_spreadsheets/Ch5.ipynb) &nbsp; [GitHub](https://github.com/drdave-teaching/OPIM5502-notebooks/blob/master/Module2_ETL_PySpark/M2_2_PySpark_EDA_on_spreadsheets/Ch5.ipynb)
- **Structured-data EDA in Colab** &nbsp; [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/drdave-teaching/OPIM5502-notebooks/blob/master/Module2_ETL_PySpark/M2_2_PySpark_EDA_on_spreadsheets/Colab_and_PySpark%20-%20Structured%20Data.ipynb) &nbsp; [GitHub](https://github.com/drdave-teaching/OPIM5502-notebooks/blob/master/Module2_ETL_PySpark/M2_2_PySpark_EDA_on_spreadsheets/Colab_and_PySpark%20-%20Structured%20Data.ipynb)
- **Ch7 pt1 — views, UNION & JOIN** &nbsp; [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/drdave-teaching/OPIM5502-notebooks/blob/master/Module3_JSON_and_SQL/Module3_2_SQL/Ch7_pt1.ipynb) &nbsp; [GitHub](https://github.com/drdave-teaching/OPIM5502-notebooks/blob/master/Module3_JSON_and_SQL/Module3_2_SQL/Ch7_pt1.ipynb)
- **Ch7 pt2 — the Backblaze data** &nbsp; [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/drdave-teaching/OPIM5502-notebooks/blob/master/Module3_JSON_and_SQL/Module3_2_SQL/Ch7_pt2.ipynb) &nbsp; [GitHub](https://github.com/drdave-teaching/OPIM5502-notebooks/blob/master/Module3_JSON_and_SQL/Module3_2_SQL/Ch7_pt2.ipynb)
- **Ch7 pt3 — blending PySpark & SQL** &nbsp; [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/drdave-teaching/OPIM5502-notebooks/blob/master/Module3_JSON_and_SQL/Module3_2_SQL/Ch7_pt3.ipynb) &nbsp; [GitHub](https://github.com/drdave-teaching/OPIM5502-notebooks/blob/master/Module3_JSON_and_SQL/Module3_2_SQL/Ch7_pt3.ipynb)

*Class exercises & projects:*

- **Class exercise — structured data** &nbsp; [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/drdave-teaching/OPIM5502-notebooks/blob/master/Module2_ETL_PySpark/M2_2_PySpark_EDA_on_spreadsheets/ClassExercise_StructuredData_PySpark.ipynb) &nbsp; [GitHub](https://github.com/drdave-teaching/OPIM5502-notebooks/blob/master/Module2_ETL_PySpark/M2_2_PySpark_EDA_on_spreadsheets/ClassExercise_StructuredData_PySpark.ipynb)
- **Class exercise — SQL** &nbsp; [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/drdave-teaching/OPIM5502-notebooks/blob/master/Module3_JSON_and_SQL/Module3_2_SQL/ClassExercise_SQL.ipynb) &nbsp; [GitHub](https://github.com/drdave-teaching/OPIM5502-notebooks/blob/master/Module3_JSON_and_SQL/Module3_2_SQL/ClassExercise_SQL.ipynb)
- **Project 1 — EDA with PySpark (blank)** &nbsp; [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/drdave-teaching/OPIM5502-notebooks/blob/master/Module2_ETL_PySpark/Project1_EDA_PySpark/Project1_blank.ipynb) &nbsp; [GitHub](https://github.com/drdave-teaching/OPIM5502-notebooks/blob/master/Module2_ETL_PySpark/Project1_EDA_PySpark/Project1_blank.ipynb)
- **Project 1 — answer key** &nbsp; [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/drdave-teaching/OPIM5502-notebooks/blob/master/Module2_ETL_PySpark/Project1_EDA_PySpark/Project1_answers.ipynb) &nbsp; [GitHub](https://github.com/drdave-teaching/OPIM5502-notebooks/blob/master/Module2_ETL_PySpark/Project1_EDA_PySpark/Project1_answers.ipynb)
- **Primer — practical Python basics** &nbsp; [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/drdave-teaching/OPIM5502-notebooks/blob/master/Module1_Basic_Linux_Commands/M1_2_EDA/1_Practical_Python_Programming_Basics.ipynb) &nbsp; [GitHub](https://github.com/drdave-teaching/OPIM5502-notebooks/blob/master/Module1_Basic_Linux_Commands/M1_2_EDA/1_Practical_Python_Programming_Basics.ipynb)
- **Primer — data types, tables & wrangling** &nbsp; [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/drdave-teaching/OPIM5502-notebooks/blob/master/Module1_Basic_Linux_Commands/M1_2_EDA/2_DataTypes_Tables_Wrangling_and_Statistics.ipynb) &nbsp; [GitHub](https://github.com/drdave-teaching/OPIM5502-notebooks/blob/master/Module1_Basic_Linux_Commands/M1_2_EDA/2_DataTypes_Tables_Wrangling_and_Statistics.ipynb)
- **Primer — general EDA template** &nbsp; [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/drdave-teaching/OPIM5502-notebooks/blob/master/Module1_Basic_Linux_Commands/M1_2_EDA/0_General_EDA_Template.ipynb) &nbsp; [GitHub](https://github.com/drdave-teaching/OPIM5502-notebooks/blob/master/Module1_Basic_Linux_Commands/M1_2_EDA/0_General_EDA_Template.ipynb)
- **Primer — Boston EDA template** &nbsp; [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/drdave-teaching/OPIM5502-notebooks/blob/master/Module1_Basic_Linux_Commands/M1_2_EDA/1_Boston_General_EDA_Template.ipynb) &nbsp; [GitHub](https://github.com/drdave-teaching/OPIM5502-notebooks/blob/master/Module1_Basic_Linux_Commands/M1_2_EDA/1_Boston_General_EDA_Template.ipynb)
- **Primer — Markdown guide** &nbsp; [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/drdave-teaching/OPIM5502-notebooks/blob/master/Module1_Basic_Linux_Commands/M1_2_EDA/Markdown_Guide.ipynb) &nbsp; [GitHub](https://github.com/drdave-teaching/OPIM5502-notebooks/blob/master/Module1_Basic_Linux_Commands/M1_2_EDA/Markdown_Guide.ipynb)
:::

## 📌 Lecture key points

*Distilled takeaways from the video lectures behind this chapter — click each to expand.*


:::{admonition} EDA on a CSV file (Pt. 1)
:class: note dropdown
- Read a CSV by specifying the **delimiter** (comma/semicolon/`|`), `header`, `inferSchema`, and `timestampFormat`.
- Always check **rows × columns** (e.g. 238k × 30) so you can anticipate the shape after joins/filters.
- `select` takes strings, a list with `*`, or `F.col(...)`; you must `.show()` or it's only a deferred instruction.
- To **drop** a column you must **overwrite** the DataFrame (no `inplace`); verify the column count fell (30 → 28).
:::

:::{admonition} EDA on a CSV file (Pt. 2)
:class: note dropdown
- Check a column's type with `.dtypes` (a tuple of name + type); `duration` is a **string**, not a real time.
- Use **`substring`** + `cast("int")` + `.alias` to split "HH:MM:SS" into hours, minutes, seconds.
- Convert to one unit — `hours*3600 + minutes*60 + seconds` — and add it with **`withColumn`** (overwrite to persist).
- `withColumnRenamed`, lowercasing names, and alphabetical reordering are only temporary until you overwrite the DataFrame.
:::

:::{admonition} EDA on a CSV file (Pt. 3)
:class: note dropdown
- **`describe()`** gives count/mean/stddev/min/max; string columns show only alphabetical min/max.
- **`summary()`** adds 25/50/75 percentiles and accepts custom ones (e.g. `"10%","90%"`).
- Both are lazy — chain a trailing `.show()` to see output.
- Think of PySpark as a tool for **intermediate reporting tables**, then plot in pandas/Matplotlib/BI tools.
:::

:::{admonition} Joining .csv files on a common field
:class: note dropdown
- A join = **left** table + **right** table + **predicate** (match condition) + **how** (method); **inner** is the default.
- Joining on a shared key can **increase** the row count when a key matches multiple rows (238k → 339k).
- Use `&` (AND) and `|` (OR), each condition parenthesized, for **multiple predicates**.
- Join flavors: outer (may have nulls), left, **union** (row concatenation), and **cross** (the "nuclear" duplicating option).
:::

:::{admonition} Performing multiple joins with chaining
:class: note dropdown
- When the key has **different names** on each side, the join duplicates the column and Spark can't disambiguate it.
- Fix with **aliasing**: `.alias("left")`/`.alias("right")`, then drop by home table (`F.col("right.key")`) — no pandas `keep_first`.
- Chain several joins cleanly: inner-join the reference table, then **left-join** `cd_category` and `cd_program_class`.
- Each added table appends its columns (→ 37 columns), preserving rows with left joins.
:::

:::{admonition} A complex, monstrous query!
:class: note dropdown
- `groupBy(...).agg(...)` can compute many aggregations at once (sum/min/max/mean/count) — like a pivot table.
- Use `F.when(class in commercial_codes, duration).otherwise(0)` to sum a **conditional** total, then `withColumn` a **commercial_ratio**.
- Handle null totals with **`dropna()`** or **`fillna(999)`** (fillna keeps the row count constant).
- **`.toPandas()`** converts a small summary table for Matplotlib — the core workflow: crunch big → visualize small.
:::

:::{admonition} Assigning views with SQL, using UNION and JOIN
:class: note dropdown
- Register intermediate output as a **view** in the Spark catalog — even inside a SQL `CREATE OR REPLACE TEMP VIEW ... AS`.
- A view stores the **instructions**, not the data.
- **UNION** appends rows; **JOIN** extends columns — two different axes.
- The PySpark join (`drive_days.join(failures, "model", "left")`) is usually cleaner than the SQL equivalent.
:::

:::{admonition} Comparing PySpark code and SQL queries
:class: note dropdown
- SQL and PySpark differ mainly in **order of operations**: PySpark chains source→transform→action; SQL front-loads `SELECT`.
- Distributed output is **unordered** — add an explicit sort for reproducible results.
- To run SQL on a DataFrame you must **register it** as a view, or you'll hit `AnalysisException: table not found`.
- Import the `AnalysisException` to catch import/query errors early.
:::

:::{admonition} Downloading and processing the Backblaze data
:class: note dropdown
- Use **`wget`** to fetch four ~500 MB quarterly zips, **`unzip -d`** into per-quarter folders, then delete junk + zips.
- ~12 GB total, ~40 million rows across ~92 daily CSVs per quarter — read a whole folder at once (no glob/loop).
- Real-world snag: **Q4 has extra columns** — add literal `None` string columns to the others so **`union`** aligns.
- Cast `smart*` columns to numeric and **register** the combined DataFrame as a view for SQL.
:::

:::{admonition} Using SQL-like syntax within dataframe methods
:class: note dropdown
- Compare **select/where**, **group by/order by**, and **having** across SQL and PySpark on the failure-rate question.
- SQL lets you reference columns by **index** in `GROUP BY`/`ORDER BY` and name computed columns with a bare space.
- Bytes → GB via `capacity / 1024^3`; compute per-model **min/max** and order by the result.
- **`HAVING`** filters *after* grouping — useful for drives reporting multiple or negative capacities (data issues).
:::

:::{admonition} Blending PySpark and SQL code
:class: note dropdown
- Read many files and keep only **common columns** with `functools.reduce` — four clean lines, no manual `None` padding.
- Three DataFrame methods accept SQL snippets: **`selectExpr`**, **`expr`**, and **`where`/`filter`**.
- Build **drive_days** and **failures** as grouped DataFrames, join with `fillna(0)`, and derive a **`failure_rate`** column.
- A `where("capacity BETWEEN ... AND ...")` clause (string substitution, ±25% tolerance) cleanly returns the top-N reliable drives.
:::
