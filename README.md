## Snowflake Utilities: Codelab Markdown → Snowflake Notebook Converter

This repository contains tools to convert Google Codelab-style Markdown (.md) into Snowflake Notebooks (.ipynb). It supports running locally, or fully inside Snowflake via a Python stored procedure that writes the notebook to a stage and creates a Notebook object.

### What it does
- Reads a Codelab Markdown file (local, URL, or stage when inside Snowflake)
- Converts it into a Snowflake Notebook with these rules:
  - The first cell contains only the codelab header (author, id, tags, etc.) and is collapsed
  - Notebook name is taken from the first H1 (#)
  - Each H2 (##) starts a new Markdown cell; the H2 header line is retained at the top of that cell and used as the cell name
  - A Duration line in each H2 section is normalized to “Duration: N minute(s)” and placed immediately after the H2 header with a blank line separation
  - Code fences become separate cells; language is auto-detected (SQL vs Python), and named like “<Section> SQL - Query 1” or “<Section> Python code 1”
  - Text after code fences begins a new Markdown cell
  - HTML comments are removed; relative links/images are rewritten to absolute using the codelab header `id` as the base
  - HTML <img> tags are converted to Markdown images

References: CREATE NOTEBOOK syntax is used per Snowflake docs ([CREATE NOTEBOOK](https://docs.snowflake.com/en/sql-reference/sql/create-notebook)).

---

## Repository layout
- `tools/codelab_to_notebook.py` — Python converter. Supports:
  - Input: local path, URL, or @stage path (stage requires a Snowflake session)
  - Output: local path or @stage path. When writing to a stage (with a Snowflake session present), the script uploads the `.ipynb` via PUT and issues a `CREATE NOTEBOOK <name> FROM '<stage_root>' MAIN_FILE = '<relative_path>' [QUERY_WAREHOUSE = ...];` command.
- `sql/snowflake_create_sp_convert_codelab.sql` — SQL to create a Python stored procedure `CONVERT_CODELAB_TO_NOTEBOOK` in Snowflake with the same functionality (stage input/output, PUT, then CREATE NOTEBOOK). It also includes a permissive external access integration example for fetching markdown over HTTPS.

---

## Local usage (CLI)
Requirements:
- Python 3.10+
- If you plan to use @stage input/output locally, your environment must provide a Snowflake Snowpark session (typically not available; for stage I/O prefer the stored procedure below).

Examples:

1) Convert from URL to local file:
```bash
python3 tools/codelab_to_notebook.py \
  https://raw.githubusercontent.com/Snowflake-Labs/sfquickstarts/refs/heads/master/site/sfguides/src/zero_to_snowflake/zero_to_snowflake.md \
  "Zero to Snowflake.ipynb"
```

2) Convert from local file to local output:
```bash
python3 tools/codelab_to_notebook.py path/to/guide.md out.ipynb
```

3) Convert from URL and write to a stage (requires a Snowflake session in the environment):
```bash
python3 tools/codelab_to_notebook.py \
  https://example.com/guide.md \
  @MY_DB.MY_SCHEMA.MY_STAGE/notebooks
```
When writing to a stage, the script:
- Writes the notebook to a temp file
- PUTs it to the stage
- Issues `CREATE NOTEBOOK <safe_name> FROM '@MY_DB.MY_SCHEMA.MY_STAGE' MAIN_FILE = 'notebooks/<file>.ipynb'` and optionally sets `QUERY_WAREHOUSE` if provided within a Snowflake session.

---

## In-Snowflake usage (recommended for stage I/O)

### 1) Create the stored procedure
Run the SQL file (adjust role/DB/schema as needed). The file creates:
- A `NETWORK RULE` and `EXTERNAL ACCESS INTEGRATION` (example: full internet egress)
- A Python stored procedure `CONVERT_CODELAB_TO_NOTEBOOK`

```sql
-- Set DB/SCHEMA as appropriate
USE DATABASE <your_db>;
USE SCHEMA <your_schema>;

-- Create the procedure and required integration
!source sql/snowflake_create_sp_convert_codelab.sql
```

The procedure signature is:
```sql
CONVERT_CODELAB_TO_NOTEBOOK(
  SOURCE_PATH STRING,            -- URL, local path (when supported), or @stage path
  OUTPUT_STAGE_PATH STRING,      -- @stage destination directory
  MAIN_FILE_NAME STRING,         -- desired notebook file name, e.g. 'MyNotebook.ipynb'
  QUERY_WAREHOUSE STRING         -- optional, recommended for running SQL in notebook
)
```

### 2) Call the procedure
```sql
CALL CONVERT_CODELAB_TO_NOTEBOOK(
  '@MY_DB.MY_SCHEMA.MY_STAGE/zero_to_snowflake/zero_to_snowflake.md',
  '@MY_DB.MY_SCHEMA.MY_STAGE/notebooks',
  'Zero_to_Snowflake.ipynb',
  'MY_WH'
);
```
What it does:
- Reads the markdown (from stage or URL)
- Converts to `.ipynb`
- Writes the file to the specified stage via PUT
- Executes `CREATE NOTEBOOK <safe_name> FROM '@MY_DB.MY_SCHEMA.MY_STAGE' MAIN_FILE = 'notebooks/Zero_to_Snowflake.ipynb' QUERY_WAREHOUSE = MY_WH;`

Privileges: ensure your role has `USAGE` on the database and schema, plus `CREATE NOTEBOOK` on the schema (see Snowflake docs: [CREATE NOTEBOOK](https://docs.snowflake.com/en/sql-reference/sql/create-notebook)).

---

## Behavior details
- Header cell is collapsed and contains only the pre-H1 header block from the codelab markdown.
- H1 sets notebook name/file.
- Each H2 opens a new Notebook Markdown cell; the H2 header line is preserved at top and also used as the cell name. If duplicate names occur, numeric suffixes are appended.
- Duration lines under H2 are standardized to “Duration: N minute(s)” and appear immediately after the H2 header, with a blank line before any other content (including images).
- Fenced code blocks split into separate cells, with language detection (SQL vs Python) and named by section and sequence.
- Content after code blocks starts a new Markdown cell.
- H3 headings remain within the same H2 cell; they do not create additional cells.
- Relative links/images are rewritten to absolute URLs using the header `id` as base. HTML `<img>` tags are converted to Markdown images.
- HTML comments are removed.

---

## Troubleshooting
- “Cannot open file '@.../file' with mode 'w'” inside Snowflake: writing to stage is done via PUT; ensure you’re using the stored procedure or running in an environment with a Snowpark session.
- “syntax error ... unexpected 'FROM'” on CREATE NOTEBOOK: ensure the statement includes a notebook object name before `FROM`, e.g. `CREATE NOTEBOOK mynotebook FROM '...' MAIN_FILE = '...'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-notebook)).
- Permissions: The role must have `CREATE NOTEBOOK` on the schema and `USAGE` on the database and schema.
- External access: If fetching markdown over HTTPS in the stored procedure, ensure the `EXTERNAL ACCESS INTEGRATION` is set and allowed.

---

## License
Apache-2.0


