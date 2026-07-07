# Chapter 6 — Machine Learning with Spark MLlib

The payoff chapter: **machine learning at scale** with Spark MLlib. We set up **Databricks Community Edition** as a free cluster, prepare a messy recipes dataset (imputing binary features, fixing string-typed numbers, capping outliers), meet **transformers and estimators** (`VectorAssembler`, `Imputer`, `MinMaxScaler`) and correlation analysis, then assemble everything into a **Pipeline**, add a **logistic-regression classifier**, **evaluate** it (confusion matrix, precision/recall/F1, ROC/AUC), and **tune** it with **cross-validation**. We close with running Spark on the UConn **HPC** cluster.

## 6.1 Databricks Community Edition

Colab auto-connects to a runtime; **Databricks** wants a real cloud workflow. Sign up for the free **Community Edition**, then **Compute → Create Compute** to spin up a (small, free) cluster — it starts in a minute or two. Upload data via **Data → Create Table** into the **file store** (`/FileStore/tables/`), and **attach your notebook to the cluster** (top-right) before running. Read from **DBFS** (enable the file browser under Settings → Admin → Workspace Settings if you don't see it); writes land in `/user/hive/warehouse/` (all lowercase). Manage files with `dbutils.fs` (`ls`, `rm`). PySpark runs **natively** — no install needed. Notebooks support **R, Python, Scala, SQL** (switch per-cell with a `%` magic), export via **File → Export → .ipynb**, and you can download displayed results as CSV. (AutoML is off in the free tier.)

## 6.2 Data prep: imputing binaries and capping outliers

Working the ~20,000-row × 680-column recipes dataset (target: "is it a dessert?"):

- **Drop null targets** with `dropna(subset=["target"])` (lost 5 rows); dropping fully-null feature rows found none, but nulls remain scattered.
- **Impute binary features** differently from continuous — you can't use the mean, so **fill with 0** (or the most common value): `fillna(0.0)`, looped over the binary columns.
- **Fix string-typed numbers** — data misalignment made `rating`/`calories` strings. Write an **`is_a_number` UDF** (returns a Boolean; `try float(x)` → `True`, except → `False`), use its complement (`~`) to find the one bad row (`"cucumber"` in `rating`), drop it, and **cast** those columns to `double`.
- **Cap outliers (truncation)** — some calories/protein values are absurd (100,000 calories). Recode anything below the **1st percentile** up to it, and above the **99th percentile** down to it, iterating a percentile dictionary while **preserving nulls**. (Prefer PySpark percentile functions over hard-coding.)

## 6.3 VectorAssembler, correlation, transformers vs. estimators

MLlib models need all features in **one vector column**. The **`VectorAssembler`** is a **transformer**: instantiate it with input columns → output column (no data yet), then `.transform(df)` adds a **vector-typed** column. Feed that to **`Correlation.corr`** (from `pyspark.ml.stat`) for pairwise Pearson correlations. The output is a **1×1 DataFrame** holding a **dense matrix** (7 features → 49 entries), so select `[0][0]`, convert to an array, then to a labeled **pandas DataFrame** for a readable heatmap — and drop features that are too highly correlated (redundant information confuses the model).

The core distinction:
- **Transformer** — no learnable parameters; `.transform()` takes a DataFrame → DataFrame (e.g. `VectorAssembler` pasting columns into a vector).
- **Estimator** — an algorithm called with **`.fit()`** that *learns* parameters and **produces a model** (itself a transformer) you then **`.transform()`**. This is the familiar `fit`/`transform` split (fit on train, transform on test), just not fused into one `fit_transform`.

## 6.4 Imputer and MinMaxScaler (estimators)

Two estimators for continuous features:
- **`Imputer`** (strategy `"mean"`) — **learns** each column's mean on `.fit()`, then `.transform()` fills nulls (output columns suffixed `_i` so you can compare).
- **`MinMaxScaler`** — **learns** each column's min/max on `.fit()`, then rescales to [0, 1] on `.transform()`. It operates on the **vector** column, so assemble first.

The recurring pattern — separate data into **identifiers / continuous / binary / target**, impute, assemble, scale — is a **reusable recipe**, which is exactly what pipelines formalize. (And the reason for all this: pandas caps out around 30–40 GB with limited functionality, while **PySpark keeps going** at 70 GB+ — use PySpark when the data is big, pandas when it's small.)

## 6.5 Building a Pipeline

A **`Pipeline`** chains stages as pure **instructions** (no data): `Imputer` → continuous `VectorAssembler` → `MinMaxScaler` → a **pre-ML assembler** that merges the scaled continuous vector **with the binary and ratio columns** into one final **`features`** vector (you can't have multiple vector columns — they must be combined). Then `.fit(df).transform(df)` runs the whole thing. Update stages with **`setStages`**. The `features` column may print in **sparse** notation (positions + values for a mostly-zero vector) vs. **dense** (all values) — and the **ML metadata** (original column names per index) lives in the schema's struct if you need to trace a feature back. *(Naming tip Dave stresses: call it `df` and `my_pipeline`, not `food` / `food_pipeline`, so your code generalizes — graders notice copy-paste.)*

## 6.6 Adding a logistic-regression classifier

Append a **`LogisticRegression`** (from `pyspark.ml.classification`) as the pipeline's final stage: `featuresCol="features"`, `labelCol="target"`, `predictionCol="prediction"`. **Split** the data (`randomSplit([0.7, 0.3], seed=13)`), **`fit` on train**, **`transform` test** — the pipeline references column *names* in `train`/`test`, so there's no separate `X_train`/`y_train` to manage. You can **hot-swap** any classifier (decision tree, random forest, boosted tree, SVM). The output has **`rawPrediction`**, **`probability`**, and **`prediction`** columns: convert a logit to probability via `e^x / (1 + e^x)`; `probability` is a two-element list (P(class 0), P(class 1)); `prediction` is the rounded argmax. Confident models cluster near 0.9/0.1; unsure ones near 0.4/0.6 — a cue for **threshold tuning** (e.g. 0.52 instead of 0.5).

## 6.7 Evaluating the model

Inspect the predictions with a **confusion matrix** (top-left TN, bottom-right TP, top-right FP, bottom-left FN). Accuracy alone misleads on **imbalanced** data (predicting "never dessert" could be 80% accurate), so use:
- **Recall** — of the actual 1s, how many you caught = TP / (TP + FN). *Mnemonic: **R** is for **R**ow and **R**ecall.*
- **Precision** — of your predicted 1s, how many are right = TP / (TP + FP) (a **column**).
- **F1** — the harmonic mean, `2·P·R / (P+R)`; it collapses if the model predicts only one class, so it's more robust than accuracy (use a **weighted** F1 across classes via support proportions).

The **ROC curve** sweeps the decision threshold; a confident model's true-positive rate spikes toward the top-left, and the **AUC** (area under the curve) ranks models — the 45° line is random guessing. The fraud analogy: to catch all fraud you accept false positives (high **recall**, lower **precision** — the annoying "did you buy this?" texts).

## 6.8 Cross-validation and hyperparameter tuning

Rather than a single holdout, use **`CrossValidator`** with a **`ParamGridBuilder`**. For a decision-tree regressor, tuning `maxDepth` and `maxBins` (5 values each) over **5-fold** CV fits **5 × 5 × 5 = 125** models, scored by a **`RegressionEvaluator`** (e.g. RMSE) with explicit `predictionCol`/`labelCol`. Read `avgMetrics` (25 configs), pick the best with **`argmin`** (RMSE — lower is better; best here `maxDepth=10`, `maxBins=100`), and `bestModel.transform(new_data)`. The spread across configs can be **20–40% RMSE** — hyperparameter tuning genuinely matters. The same script works for any tree-based model (swap the param names for SVM, etc.).

## 6.9 Extracting results, and where to go

Pull the **coefficients from the cross-validated best model** — carefully: it's `pipeline_model.stages[-1]`, *not* the earlier standalone `lr_model` (an easy mix-up). For a random forest, print **feature importances** for interpretability. **Summary:** transformers add columns via `transform`; estimators learn via `fit` then `transform`; ML pipelines are estimator-like (`fit` yields a pipeline model); all features must be assembled into one dense vector; use classification metrics (precision/recall/F1/AUC) or regression metrics; and cross-validate to generalize. From here, explore **regression** and **multi-class** problems.

## 6.10 Running Spark on HPC

For real horsepower, connect to the UConn **HPC** cluster: connect the **VPN** (Cisco AnyConnect), SSH to the login node, request resources with **`srun`** (classroom partition, ~4-hour timeout), then **`module load vscode`** and open a **VS Code tunnel** (authenticate via Microsoft account, `Ctrl+Click` the device-login link). Point at your Parquet folder with a wildcard and count rows — **44 million rows in ~20 seconds** vs. ~50s in Colab. When done, **`exit`** the compute and login nodes so you're not holding resources.

---

*This is the final chapter. Across the course you went from clunky Spark installs to a one-line `pip install pyspark`, from thousand-row prototypes to million-row datasets, and from raw text files to end-to-end ML pipelines. Thanks for taking the journey — now go build something with big data.*

## 📌 Lecture key points

*Distilled takeaways from the video lectures behind this chapter — click each to expand.*


:::{admonition} Intro to Databricks Community Edition
:class: note dropdown
- Databricks needs an explicit **cluster** (Compute → Create Compute), unlike Colab's auto-runtime.
- Upload to the **file store** (`/FileStore/tables/`); **attach the notebook to the cluster** before running.
- Read from **DBFS**; writes land in `/user/hive/warehouse/`; manage files with `dbutils.fs` (`ls`, `rm`).
- PySpark runs **natively** (no install); notebooks mix R/Python/Scala/SQL via `%` magics; export as `.ipynb`.
:::

:::{admonition} Imputing binary features and capping max values
:class: note dropdown
- **Drop null targets**; impute **binary** features with **0** (not the mean) — the mean is meaningless for 0/1.
- Fix string-typed numeric columns with an **`is_a_number` UDF**, drop the bad row, and **cast to `double`**.
- **Cap outliers**: recode below the 1st percentile up, above the 99th down (truncation), **preserving nulls**.
- Prefer PySpark percentile functions over hard-coded thresholds.
:::

:::{admonition} Working with the VectorAssembler and correlation
:class: note dropdown
- **`VectorAssembler`** (a transformer) merges numeric columns into one **vector-typed** column via `.transform()`.
- **`Correlation.corr`** returns a **1×1 DataFrame** holding a dense matrix — select `[0][0]` → array → labeled pandas heatmap.
- Drop **highly correlated** features (redundant info confuses the model).
- **Transformer** = no learned params (`transform`); **Estimator** = learns via `fit`, yields a model.
:::

:::{admonition} Transformers and Estimators with Imputer and MinMaxScaler
:class: note dropdown
- **`Imputer`** (estimator) learns column means on `fit`, fills nulls on `transform` (suffix `_i` to compare).
- **`MinMaxScaler`** (estimator) learns min/max on `fit`, rescales the **vector** column to [0, 1].
- The reusable recipe: split into **identifiers / continuous / binary / target**, impute, assemble, scale.
- Use PySpark for big data (pandas caps ~30–40 GB); PySpark keeps going at 70 GB+.
:::

:::{admonition} Building and updating our first pipeline
:class: note dropdown
- A **`Pipeline`** chains stages as **instructions** (no data): Imputer → assembler → scaler → **pre-ML assembler**.
- Combine scaled continuous **plus** binary/ratio columns into one final **`features`** vector (no multiple vector columns).
- `fit().transform()` runs it; update stages with **`setStages`**.
- Vectors print **sparse** (positions+values) or **dense**; ML metadata (column names per index) lives in the schema.
:::

:::{admonition} Adding a logistic regression classifier
:class: note dropdown
- Append **`LogisticRegression`** with `featuresCol`, `labelCol`, `predictionCol` as the pipeline's last stage.
- **`randomSplit`** into train/test with a seed; `fit` on train, `transform` on test — pipeline references column names, no `X_train`/`y_train`.
- Output has **`rawPrediction`**, **`probability`** (P(0), P(1)), and rounded **`prediction`**; logit → prob via `e^x/(1+e^x)`.
- Confident models cluster near 0.9/0.1 — a cue for **threshold tuning**; classifiers are hot-swappable.
:::

:::{admonition} Evaluating the model
:class: note dropdown
- Accuracy misleads on **imbalanced** data — use precision, recall, F1.
- **Recall** = TP/(TP+FN) (*R for Row/Recall*); **Precision** = TP/(TP+FP) (a column).
- **F1** = harmonic mean `2PR/(P+R)`, robust to one-class prediction; prefer a **weighted** F1.
- The **ROC/AUC** ranks models by confidence across thresholds; the 45° line is random guessing.
:::

:::{admonition} Where do we go from here
:class: note dropdown
- Pull coefficients from the **cross-validated best model** (`pipeline_model.stages[-1]`), not the standalone `lr_model`.
- For a random forest, print **feature importances** for interpretability.
- Summary: transformers add columns; estimators `fit` then `transform`; pipelines are estimator-like; assemble one dense vector.
- Next: explore **regression** and **multi-class** problems in PySpark.
:::

:::{admonition} Cross-validator for ML regression
:class: note dropdown
- **`CrossValidator`** + **`ParamGridBuilder`** tunes hyperparameters (e.g. `maxDepth`, `maxBins`) over k folds.
- 5 values × 5 values × 5 folds = **125 models**, scored by a `RegressionEvaluator` (RMSE).
- Pick the best via **`argmin`** on `avgMetrics` (lower RMSE); apply `bestModel.transform(new_data)`.
- Config spread can be **20–40% RMSE** — hyperparameter tuning genuinely matters; the script generalizes to any tree model.
:::

:::{admonition} A personal note from Dave
:class: note dropdown
- First time teaching the course — thanks to Jonathan Rioux (book), Databricks, and Colab.
- The class went from clunky installs to a one-line `pip install pyspark`.
- You worked with million-row datasets and prototyped on thousand-row ones.
- Goal: feel ready to interact with big data and carry these skills to the job.
:::

:::{admonition} Connecting to HPC the 2nd time
:class: note dropdown
- Connect the **VPN** (Cisco AnyConnect), SSH to the login node, request resources with **`srun`** (classroom partition).
- **`module load vscode`**, open a **VS Code tunnel**, authenticate via Microsoft account + device-login link.
- Counting a Parquet folder: **44M rows in ~20s** on HPC vs. ~50s in Colab.
- **`exit`** the compute and login nodes when done so you don't hold resources.
:::
