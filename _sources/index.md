<svg xmlns="http://www.w3.org/2000/svg" width="1000" height="118" viewBox="0 0 1000 118" role="img" style="width:100%;height:auto;display:block;margin-bottom:1rem">
<title>OPIM 5502 course banner</title>
<rect x="0" y="0" width="1000" height="118" rx="12" fill="#047857"/>
<rect x="0" y="0" width="9" height="118" rx="4.5" fill="#064E3B"/>
<text x="848" y="96" text-anchor="middle" font-family="Arial, Helvetica, sans-serif" font-size="86" font-weight="700" fill="#34D399" opacity="0.45">5502</text>
<text x="40" y="50" font-family="Arial, Helvetica, sans-serif" font-size="31" font-weight="700" fill="#ffffff">OPIM 5502</text>
<text x="42" y="78" font-family="Arial, Helvetica, sans-serif" font-size="17" font-weight="400" fill="#D1FAE5">Big Data with PySpark</text>
<text x="42" y="103" font-family="Arial, Helvetica, sans-serif" font-size="14" font-weight="700" fill="#ffffff">Dr. Dave Wanik</text>
<text x="158" y="103" font-family="Arial, Helvetica, sans-serif" font-size="14" font-weight="400" fill="#D1FAE5">· University of Connecticut</text>
</svg>

# Big Data with PySpark
### A hands-on course companion — OPIM 5502

*by Dave Wanik · University of Connecticut*

:::{admonition} ⚠️ Work in progress
:class: warning
These materials are a **living draft** — actively being written, revised, and expanded from the lecture videos. Many lecture narratives are still being filled in from transcripts (you'll see *"coming soon"* notes). This is a teaching companion, **not a final or official reference**. It's a work in progress!
:::

---

This book is the readable companion to **OPIM 5502: Data Science with Big Data**, a
hands-on course taught in **PySpark**. Each chapter follows a run of the course; each
section is a lecture, paired with the code Dr. Wanik demonstrated live. The narrative is
built from the lecture video transcripts, lightly edited for reading.

> **Taught with Apache Spark via PySpark**, in Google Colab and Databricks Community
> Edition — the same tools you can spin up for free to follow along.

## About the course
- **Instructor:** Dave Wanik (dave.wanik@uconn.edu)
- **Format:** Hands-on, code-along · big-data engineering and ML at scale
- **Description:** Working with data too big for one machine — distributed DataFrames,
  Spark SQL, RDDs and user-defined functions, window functions, and machine-learning
  pipelines with Spark MLlib.

### Learning objectives
By the end of the course you should be able to:
1. Manipulate large datasets with the **Spark DataFrame** API and **Spark SQL**
2. Read and reshape structured and semi-structured data (CSV, JSON, Parquet)
3. Extend Spark with **RDDs**, **UDFs**, and **pandas UDFs**
4. Apply **window functions** for ranked and running analytics
5. Build and evaluate **machine-learning pipelines** with Spark MLlib

## How the book is organized
| Ch. | Theme |
|---|---|
| 1 | Spark Fundamentals — Linux, what Spark is, first DataFrames, NLP |
| 2 | DataFrames — EDA, joins, and Spark SQL |
| 3 | Complex Data — JSON, arrays, maps, structs, schemas |
| 4 | RDDs & UDFs — higher-order functions and pandas UDFs |
| 5 | Window Functions — analytical, ranking, and running windows |
| 6 | Machine Learning — Databricks, pipelines, and Spark MLlib |

---

*Build status: structure and per-lecture key points are in place; full lecture narratives
are filled in as video transcripts are polished. Notebooks live on Dr. Wanik's course
repositories.*
