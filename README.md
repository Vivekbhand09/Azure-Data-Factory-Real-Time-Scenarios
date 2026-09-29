<div align="center">

![Azure Data Factory](https://img.shields.io/badge/Azure_Data_Factory-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![ADLS Gen2](https://img.shields.io/badge/ADLS_Gen2-0062AD?style=for-the-badge&logo=microsoftazure&logoColor=white)
![JSON](https://img.shields.io/badge/JSON-000000?style=for-the-badge&logo=json&logoColor=white)
![Data Engineering](https://img.shields.io/badge/Data_Engineering-FF6F00?style=for-the-badge)
![ETL](https://img.shields.io/badge/ETL_Pipelines-2E7D32?style=for-the-badge)
![Status](https://img.shields.io/badge/Status-Learning_Project-blueviolet?style=for-the-badge)

</div>

# 🏭 Azure Data Factory: Real-Time Scenarios

A hands-on collection of **6 real-world pipelines** I built in **Azure Data Factory (ADF)** while learning data engineering. Each scenario has a short theory section, then exactly how I implemented it (activities, settings and expressions).

> 💡 **About this repo:** This is a learning project. I am still growing my ADF knowledge, so each scenario also lists the **limitations** I found and how a production version could be improved.

---

## 📑 Table of Contents

1. [Tools and Concepts Used](#-tools-and-concepts-used)
2. [Scenario 1: Incremental Load Using File Last-Modified Date](#-scenario-1-incremental-load-using-file-last-modified-date)
3. [Scenario 2: Copy Only Missing Files (Reconciliation)](#-scenario-2-copy-only-missing-files-reconciliation)
4. [Scenario 3: Fetch Files Modified in the Last 1 Day](#-scenario-3-fetch-files-modified-in-the-last-1-day)
5. [Scenario 4: Delete Files Older Than 7 Days (Retention Cleanup)](#-scenario-4-delete-files-older-than-7-days-retention-cleanup)
6. [Scenario 5: Store the Number of Files in a Variable](#-scenario-5-store-the-number-of-files-in-a-variable)
7. [Scenario 6: Dynamic Column Mapping](#-scenario-6-dynamic-column-mapping)
8. [Summary of All Scenarios](#-summary-of-all-scenarios)
9. [Key Takeaways](#-key-takeaways)

---

## 🧰 Tools and Concepts Used

**☁️ Services:** Azure Data Factory, Azure Data Lake Storage Gen2 (ADLS Gen2)

| Concept | What it does (in simple words) |
|---|---|
| 📂 **Dataset** | Points to the data (a folder or a file) in storage |
| 🎛️ **Parameterised dataset** | A dataset with parameters (container, folder, file) so one dataset can be reused for many locations |
| 🔎 **Get Metadata** | Reads information about a folder or file (list of items, last modified time, etc.) |
| 🔁 **ForEach** | Loops over a list and runs activities once per item |
| 🧪 **Filter** | Keeps only the items in a list that satisfy a condition (like `WHERE` in SQL) |
| 🔀 **If Condition** | Runs different activities depending on whether a condition is true or false |
| 📝 **Set Variable** | Stores a value in a pipeline variable |
| 📋 **Copy Activity** | Copies data from a source to a sink (destination) |
| 🗑️ **Delete Activity** | Deletes files from storage |
| ⚙️ **Pipeline parameters** | Values passed into the pipeline when it runs (makes it reusable) |
| 🧮 **Expressions / functions** | Dynamic logic such as `utcNow()`, `addDays()`, `contains()`, `length()`, `if()` |

### 📦 Datasets used across scenarios

| Dataset | Parameters | Purpose |
|---|---|---|
| `ds_param_folderlevel` | `p_container`, `p_folder` | Represents a **folder** (used to list what is inside) |
| `ds_file_level` | `p_container`, `p_folder`, `p_file` | Represents a **single file** (used to read or copy that file) |

### 🧬 Common pattern in most scenarios

```
Get Metadata (discover files)  ->  Filter / If (decide what to process)  ->  ForEach (loop)  ->  Copy / Delete (do the work)
```

---

## 🔄 Scenario 1: Incremental Load Using File Last-Modified Date

### 🎯 Goal
Copy the **latest file** from a source folder instead of reloading everything each run.

### 📖 Theory
**Incremental loading** means processing only **new or changed** data instead of everything, every time. It saves time, cost and avoids duplicates.

Common ways to detect "what is new":

| Strategy | How it works |
|---|---|
| 💧 Watermark column (databases) | Only pull rows greater than the last stored value |
| 🕒 **File last-modified (used here)** | Compare each file's `lastModified` time to find the newest |
| 📡 Change Data Capture (CDC) | Database tracks inserts, updates and deletes |

### 🔀 Pipeline flow

```
Get Metadata1 (list all files)
        |
        v
ForEach (Sequential)
    |-- Get Metadata2 (lastModified of current file)
    |-- If Condition: is this file newer than temp_max_value?
            |-- True: Set Variable temp_max_value = this file's lastModified
        |
        v
Set Variable max_value = temp_max_value
        |
        v
Copy Activity (filter by Last Modified >= max_value)
```

### 🔧 Implementation

| Step | Activity | Settings |
|---|---|---|
| 0 | Variables | `temp_max_value` (working value) and `max_value` (final value) |
| 1 | **Get Metadata1** | Dataset `ds_param_folderlevel`, field list: **Child items** |
| 2 | **ForEach** | Items: `@activity('Get Metadata1').output.childItems`, **Sequential** |
| 3 | **Get Metadata2** (inside loop) | Dataset `ds_file_level`, `p_file = @item().name`, field list: **Last modified** |
| 4 | **If Condition** | See expression below |
| 5 | **Set Variable** (True branch) | `temp_max_value = @activity('Get Metadata2').output.lastModified` |
| 6 | **Set Variable** (after loop) | `max_value = @variables('temp_max_value')` |
| 7 | **Copy Activity** | Source: *Filter by last modified*, Start time = `@variables('max_value')`. Sink: `raw_gold_data` folder |

**If Condition expression**

```
@less(
  formatDateTime(variables('temp_max_value'),'yyyyMMddHHmmss'),
  formatDateTime(activity('Get Metadata2').output.lastModified,'yyyyMMddHHmmss')
)
```

- `formatDateTime(..., 'yyyyMMddHHmmss')` converts both timestamps into the same sortable format.
- `less(a, b)` returns true if `a < b`, meaning the current file is newer than the max seen so far.
- This is the classic **"running maximum"** idea: walk through a list and keep the largest value seen.

### 🐢 Why ForEach is Sequential
Each iteration reads and updates `temp_max_value`. Running in parallel would cause several iterations to read and write the variable at the same time and give wrong results.

### 🚧 Limitations and how to improve
- **ADF variables do not persist between runs.** They reset every time the pipeline starts.
- What this pipeline really does is **"copy the single newest file in the folder"**, not **"copy everything new since the last run"**. If two new files arrive between runs, only the newest is picked.
- Give `temp_max_value` an old default value so the first file always wins the comparison.
- 🚀 **Production-style improvement:**
  1. **Lookup** activity reads the last watermark from a control table (Azure SQL) or a JSON file
  2. **Get Metadata** lists files
  3. **Filter** keeps all files newer than the watermark
  4. **ForEach + Copy** copies each qualifying file
  5. **Stored Procedure / Copy** updates the watermark after a successful run

---

## 🔍 Scenario 2: Copy Only Missing Files (Reconciliation)

### 🎯 Goal
Copy only the files that exist in the **source** but are **missing in the destination**.

### 📖 Theory
This is a **set difference** problem:

```
Missing files = Files in Source  -  Files in Destination
```

**Real-world uses:** backfilling after a failed run, auditing silent copy failures, verifying a backup container is fully in sync.

### 🔀 Pipeline flow

```
Get Metadata (GetSource)  --\
                             >--  Filter (FilterMissing)  ->  ForEach  ->  Copy
Get Metadata (GetSink)    --/
```

### 🔧 Implementation

| Step | Activity | Settings |
|---|---|---|
| 1 | **GetSource** (Get Metadata) | Source folder, field list: **Child items** |
| 2 | **GetSink** (Get Metadata) | Destination folder, field list: **Child items** |
| 3 | **FilterMissing** (Filter) | See below |
| 4 | **ForEach** | Items: `@activity('FilterMissing').output.Value` |
| 5 | **Copy Activity** | Source and sink use `item().name` as the file parameter |

**Filter activity**

```
Items:     @activity('GetSource').output.childItems
Condition: @not(contains(activity('GetSink').output.childItems, item()))
```

**How to read it:** for each file in the **source**, check whether it exists in the **sink** list. `not(...)` keeps it only if it is **not** found. The result is the list of source files missing from the destination.

- The Filter activity returns its result in `output.Value`.
- `item()` is a full object like `{ "name": "sales_jan.csv", "type": "File" }`, so `contains()` compares the name **and** type.

### ⚡ Sequential vs Parallel
There is no shared variable here, so each copy is independent and the ForEach can run in **parallel** for speed.

### 🚧 Limitations
- Checks only whether a file **exists**, not whether its **content changed**. Combine with the timestamp logic from Scenario 1 to catch changed files.
- One direction only (source to destination). Files that exist only in the destination are not handled.
- Very large folders (tens of thousands of files) may need a different approach (Databricks, Synapse or Data Flow).

---

## 🕐 Scenario 3: Fetch Files Modified in the Last 1 Day

### 🎯 Goal
Get only the files that were added or modified in the last 24 hours.

### 📖 Theory
The **Get Metadata** activity has a **"Filter by last modified"** setting with a **Start time** and **End time**. When set, `childItems` returns only files whose `lastModified` falls in that window. The filtering happens inside Get Metadata itself.

### 🔧 Implementation

| Setting | Value |
|---|---|
| Activity | Get Metadata (folder-level dataset), field list: **Child items** |
| Filter by last modified, **Start time** | `@addDays(utcNow(), -1)` |
| Filter by last modified, **End time** | *(empty, meaning "up to now")* |

- `utcNow()` gives the current UTC time.
- `addDays(utcNow(), -1)` subtracts 24 hours.

The resulting list can then be passed to **ForEach + Copy** for processing.

### 📅 Rolling 24 hours vs previous calendar day

| | Rolling 24-hour window (what I built) | Previous calendar day |
|---|---|---|
| Start | `@addDays(utcNow(), -1)` | `@startOfDay(addDays(utcNow(), -1))` |
| End | empty (now) | `@startOfDay(utcNow())` |
| Meaning | Last 24 hours from the moment the pipeline runs | Yesterday from 00:00 to 24:00 |
| Best for | Monitoring recent activity | Daily batch reports |

### 🌍 Timezone note
`utcNow()` returns **UTC**. If a requirement is written in local time (for example IST, UTC+5:30), convert first:

```
@convertTimeZone(utcNow(), 'UTC', 'India Standard Time')
```

Storage `lastModified` values are in UTC, so comparing UTC to UTC is safe.

---

## 🧹 Scenario 4: Delete Files Older Than 7 Days (Retention Cleanup)

### 🎯 Goal
Automatically delete files older than a configurable number of days.

### 📖 Theory
A **retention policy** removes old files from a data lake to save storage cost and keep it clean. It usually runs on a schedule (daily or weekly).

### 🔀 Pipeline flow

```
Get Metadata1 (files older than 7 days)
        |
        v
ForEach
    |-- Get Metadata2 (lastModified of file)
    |-- If Condition (older than N days?)
            |-- True: Delete Activity
```

### 🔧 Implementation

| Step | Activity | Settings |
|---|---|---|
| 1 | **Get Metadata1** | Filter by last modified, **End time** = `@addDays(utcNow(), -7)`, Start time empty. Returns every file older than 7 days |
| 2 | **ForEach** | Items: `@activity('GetMetadata1').output.childItems` |
| 3 | **Get Metadata2** | Gets `lastModified` of the current file |
| 4 | **If Condition** | `@less(activity('Get Metadata2').output.lastModified, addDays(utcNow(), pipeline().parameters.p_last7days))` |
| 5 | **Delete Activity** | Runs in the True branch and deletes the current file |

**Parameter:** `p_last7days` (for example `-7`). Because the retention period is a parameter, the same pipeline works for daily, weekly or monthly cleanup by changing the value.

### 📌 Important points
- ➖ **The parameter must be negative.** `addDays()` needs `-7` to go back in time. A positive `7` calculates a date in the future and would match almost every file.
- 🛡️ **The inner check is a safety double-check.** Get Metadata1 already returns only old files, so Get Metadata2 and the If Condition are technically redundant. I kept them as an extra safeguard before an irreversible delete.
- 📜 **Enable logging** in the Delete Activity so there is a record of what was deleted.
- 🚀 **Safer alternatives for production:** archive files to a cooler tier or archive container first, and enable **Blob soft delete / versioning** as a recovery net.

---

## 🔢 Scenario 5: Store the Number of Files in a Variable

### 🎯 Goal
Count the files in a folder and keep the number in a variable.

### 📖 Theory
ADF has no dedicated "count files" activity. The standard approach is **Get Metadata (Child items) + `length()`**.

Useful for:
- ✅ **Validation:** was any file received today?
- 📝 **Logging:** how many files were processed
- 🔀 **Branching:** different logic for small and large loads
- 🚨 **Alerting:** raise an alert if the count is 0

### 🔀 Pipeline flow

```
Get Metadata (Child items)  ->  Set Variable (fileCount)  ->  If Condition (fileCount = 0 ?)
                                                                 |-- True: alert / stop
                                                                 |-- False: continue processing
```

### 🔧 Implementation

| Step | Activity | Settings |
|---|---|---|
| 1 | **Get Metadata1** | Folder dataset, field list: **Child items** |
| 2 | **Set Variable** | Variable `fileCount` = `@string(length(activity('Get Metadata1').output.childItems))` |

- `length()` returns the number of elements in an array.
- ADF pipeline variables support **String, Boolean and Array** types, so the count is stored as a string. Convert with `int(variables('fileCount'))` when comparing numbers.
- Example check: `@equals(int(variables('fileCount')), 0)`

### 📁 Important nuance: files vs folders
`childItems` contains **both files and subfolders**. If the folder has subfolders, plain `length()` overcounts.

For a **files-only** count:
1. Add a **Filter** activity: Items = `@activity('Get Metadata1').output.childItems`, Condition = `@equals(item().type, 'File')`
2. Count the result: `@length(activity('Filter1').output.Value)`

### 🤔 Why it matters
Without this check, a pipeline can "succeed" while processing **zero files** because an upstream system failed to deliver data. A count check makes that failure visible.

`length()` is also handy for counting Lookup rows: `@length(activity('Lookup1').output.value)`.

---

## 🧩 Scenario 6: Dynamic Column Mapping

### 🎯 Goal
Use **one Copy Activity** inside a ForEach to copy files that have **different schemas** (demand files and reserves files), each with its own column mapping.

### 📖 Theory
**Column mapping** tells the Copy Activity which source column goes to which sink column. It is needed to rename columns, skip columns, change data types or reorder columns. Without explicit mapping, ADF auto-maps by matching names.

Behind the scenes, mapping is stored as a **`TabularTranslator`** JSON object:

```json
{
  "type": "TabularTranslator",
  "mappings": [
    { "source": { "name": "id" },         "sink": { "name": "ID" } },
    { "source": { "name": "demand_qty" }, "sink": { "name": "DemandQuantity" } },
    { "source": { "name": "region" },     "sink": { "name": "Region" } }
  ]
}
```

🔑 **Key idea:** this JSON is just an object, so instead of hard-coding it inside the Copy Activity, it can be supplied **dynamically** at runtime.

### 🔀 Pipeline flow

```
Get Metadata (Child items)  ->  ForEach  ->  ONE Copy Activity
                                                |-- Source / Sink: ds_file_level with @item().name
                                                |-- Mapping: chosen dynamically per file
```

### 🔧 Implementation

| Step | What I did |
|---|---|
| 1 | Created two **Object** pipeline parameters: `p_demand` and `p_reserves` |
| 2 | Built the mapping once per file type in the UI, opened the Copy Activity **code view**, copied the `TabularTranslator` JSON and pasted it as each parameter's default value |
| 3 | **Get Metadata** to list files, then **ForEach** over `childItems` |
| 4 | Inside the loop, **one Copy Activity** with source and sink using `ds_file_level` and `p_file = @item().name` |
| 5 | In the **Mapping** section, added dynamic content (below) |

**Dynamic mapping expression**

```
@if(
  contains(item().name, 'demand'),
  pipeline().parameters.p_demand,
  pipeline().parameters.p_reserves
)
```

If the file name contains `demand`, use the demand mapping. Otherwise use the reserves mapping.

### ✨ Why this approach
- One Copy Activity instead of one per file type
- Changing a mapping means editing one parameter
- Fully explicit control over renames, types and columns

### 🚧 Limitations and improvements
- **Case-sensitive:** `Demand_Report.csv` would fall into the `else` branch and silently get the wrong mapping. Use `contains(toLower(item().name), 'demand')`.
- **Not great for many file types:** nested `if()` gets messy. Better to keep the file pattern and mapping JSON in a **config table or file**, read it with a **Lookup**, and pick the matching row with a **Filter** activity.
- **Fragile naming dependency:** a folder-per-type structure is more reliable than file name matching.
- **Schema changes are not automatic:** if a source file gets a new column, the mapping JSON must be updated by hand.
- Make sure parameter names are spelled identically in the parameters list and the expression (`p_reserves`).

---

## 📊 Summary of All Scenarios

| # | Scenario | Question it answers | Key technique |
|---|---|---|---|
| 1 | 🔄 Incremental Load | What is the newest file? | Running max with Set Variable + If inside a sequential ForEach |
| 2 | 🔍 Missing Files | What is in source but not in destination? | Filter activity with `not(contains())` |
| 3 | 🕐 Files by Date Range | Which files changed in a time window? | Get Metadata "Filter by last modified" |
| 4 | 🧹 Delete Old Files | Which files are older than N days? | Get Metadata end-time filter + Delete Activity + parameter |
| 5 | 🔢 File Count | How many files are in the folder? | `length()` on `childItems` |
| 6 | 🧩 Dynamic Mapping | How do I map columns per file type in one Copy? | Object parameters + `if(contains())` in Mapping |

---

## 🧠 Key Takeaways

- 🔎 **Get Metadata is the foundation.** Most real pipelines first discover what exists, then decide what to process.
- 🎛️ **Parameterise everything.** Parameterised datasets and pipeline parameters make pipelines reusable.
- ⚡ **Sequential vs parallel:** use Sequential when iterations share state (Scenario 1), and Parallel when items are independent (Scenarios 2 and 4).
- 💾 **Variables do not persist across runs.** Real incremental loads store the watermark in an external table or file.
- ⏱️ **Time handling needs care:** rolling window vs calendar day, UTC vs local time, and the sign in `addDays()`.
- 🛡️ **Irreversible actions need safeguards:** logging, archiving and soft delete for delete pipelines.
- 🚧 **Know the limitations.** Understanding what a pattern does not handle is as important as building it.

---
