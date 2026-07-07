# OPIM 5502 — Big Data with PySpark (book)

The hands-on companion to **OPIM 5502** — Dr. Dave Wanik, University of Connecticut.
Built from the lecture videos and Colab / Databricks notebooks. **Work in progress:**
lecture narratives are filled in as transcripts are pulled and polished.

Live: https://drdave-teaching.github.io/opim5502-textbook/

Six chapters, each with full narrative prose and a per-lecture **📌 key points** section:

1. **Spark Fundamentals** — Linux, what Spark is, first DataFrames, an NLP word-count pipeline
2. **DataFrames** — EDA, joins, and Spark SQL
3. **Complex Data** — JSON, arrays, maps, structs, schemas
4. **RDDs & UDFs** — higher-order functions and pandas UDFs
5. **Window Functions** — analytical, ranking, and running windows
6. **Machine Learning** — Databricks, pipelines, and Spark MLlib

## Build locally

```bash
pip install "jupyter-book>=0.15,<2"
jupyter-book build .
# open _build/html/index.html
```

Pushes to `main` auto-build and deploy to GitHub Pages via the `deploy-book` workflow.
