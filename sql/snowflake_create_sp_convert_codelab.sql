-- Creates a Python stored procedure to convert a Google Codelab markdown file
-- into a Snowflake Notebook. Supports inputs/outputs via Snowflake stages and
-- optionally sets QUERY_WAREHOUSE on the created Notebook.

CREATE OR REPLACE NETWORK RULE full_internet_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('0.0.0.0');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION full_internet_access_integration
  ALLOWED_NETWORK_RULES = (full_internet_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = ALL
  ENABLED = true;
  
CREATE OR REPLACE PROCEDURE CONVERT_CODELAB_TO_NOTEBOOK(
    SOURCE_PATH STRING,
    OUTPUT_STAGE_PATH STRING,
    MAIN_FILE_NAME STRING,
    QUERY_WAREHOUSE STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
EXTERNAL_ACCESS_INTEGRATIONS = (FULL_INTERNET_ACCESS_INTEGRATION)
HANDLER = 'run'
AS
$$
import re
import os
import json
from urllib.parse import urlparse
from urllib.request import urlopen
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.files import SnowflakeFile

def is_stage_path(path: str) -> bool:
    return isinstance(path, str) and path.strip().startswith("@")

def fetch_text(source: str):
    if is_stage_path(source):
        with SnowflakeFile.open(source, "r") as f:
            return f.read()
    if source.startswith("http://") or source.startswith("https://"):
        with urlopen(source) as resp:
            return resp.read().decode("utf-8", errors="replace")
    with open(source, "r", encoding="utf-8") as f:
        return f.read()

def sanitize_filename(name: str) -> str:
    name = name.strip().replace("/", "-")
    return re.sub(r"[\\:*?\"<>|]", "-", name)

def extract_header_and_body(md: str):
    lines = md.splitlines()
    header_lines = []
    i = 0
    while i < len(lines) and not lines[i].startswith("# "):
        header_lines.append(lines[i])
        i += 1
    body = "\n".join(lines[i:])
    header_text = "\n".join(header_lines).strip()
    return header_text, body

def parse_id_from_header(header_text: str) -> str:
    for line in header_text.splitlines():
        m = re.match(r"^\s*id\s*:\s*(.+?)\s*$", line)
        if m:
            return m.group(1).strip()
    return ""

def build_base_url(content_id: str) -> str:
    if not content_id:
        return ""
    return (
        "https://raw.githubusercontent.com/Snowflake-Labs/sfquickstarts/refs/heads/"
        "master/site/sfguides/src/" + content_id.strip("/") + "/"
    )

def is_absolute_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        return parsed.scheme in ("http", "https")
    except Exception:
        return False

def rewrite_relative_url(url: str, base_url: str) -> str:
    if not url or not base_url:
        return url
    url = url.strip()
    if is_absolute_url(url) or url.startswith("mailto:") or url.startswith("#"):
        return url
    if url.startswith("./"):
        return base_url + url[2:]
    if not url.startswith("/"):
        return base_url + url
    return url

def replace_markdown_urls(text: str, base_url: str) -> str:
    text = re.sub(r"<!--[\s\S]*?-->", "", text)

    def img_sub(m):
        alt = m.group(1)
        url = m.group(2)
        title = m.group(3) or ""
        new_url = rewrite_relative_url(url, base_url)
        title_part = f' "{title}"' if title else ""
        return f"![{alt}]({new_url}{title_part})"

    def link_sub(m):
        text = m.group(1)
        url = m.group(2)
        title = m.group(3) or ""
        new_url = rewrite_relative_url(url, base_url)
        title_part = f' "{title}"' if title else ""
        return f"[{text}]({new_url}{title_part})"

    def html_img_any(m):
        tag = m.group(0)
        src_m = re.search(r"src\s*=\s*'([^']+)'", tag, re.IGNORECASE) or re.search(r' src\s*=\s*"([^"]+)"', tag, re.IGNORECASE)
        if not src_m:
            return tag
        alt_m = re.search(r"alt\s*=\s*'([^']*)'", tag, re.IGNORECASE) or re.search(r' alt\s*=\s*"([^"]*)"', tag, re.IGNORECASE)
        title_m = re.search(r"title\s*=\s*'([^']*)'", tag, re.IGNORECASE) or re.search(r' title\s*=\s*"([^"]*)"', tag, re.IGNORECASE)
        src_val = src_m.group(1)
        alt_val = alt_m.group(1) if alt_m else "image"
        title_val = title_m.group(1) if title_m else "Image"
        full_url = rewrite_relative_url(src_val, base_url)
        return f"![{alt_val}]({full_url} \"{title_val}\")"

    text = re.sub(r"<img[^>]*?>", html_img_any, text, flags=re.IGNORECASE)
    text = re.sub(r"!\[([^\]]*)\]\(([^\s\)]+)(?:\s+\"([^\"]*)\")?\)", img_sub, text)
    text = re.sub(r"\[([^\]]+)\]\(([^\s\)]+)(?:\s+\"([^\"]*)\")?\)", link_sub, text)
    return text

def detect_code_language(explicit_lang: str, code_text: str) -> str:
    if explicit_lang:
        lang = explicit_lang.strip().lower()
        if lang in ("sql", "snowflake-sql"):
            return "sql"
        if lang in ("python", "py"):
            return "python"
    sample = code_text.strip()
    if re.search(r"\bSELECT\b|\bCREATE\b|\bWITH\b|\bINSERT\b|\bUPDATE\b|\bDELETE\b", sample, re.IGNORECASE):
        return "sql"
    if re.search(r"\bimport\b|\bdef\b|\bclass\b|print\(\)|from\s+\w+\s+import", sample):
        return "python"
    return "python"

def unique_cell_name(base_name: str, used: set) -> str:
    name = base_name
    suffix = 2
    while name in used:
        name = f"{base_name} {suffix}"
        suffix += 1
    used.add(name)
    return name

def create_markdown_cell(name: str, text: str, collapsed: bool = False):
    return {"cell_type": "markdown", "metadata": {"name": name, "collapsed": collapsed}, "source": text}

def create_code_cell(name: str, language: str, code: str):
    return {"cell_type": "code", "metadata": {"language": language, "name": name}, "source": code, "execution_count": None, "outputs": []}

def build_notebook(md_body: str, base_url: str, header_text: str):
    lines = md_body.splitlines()
    i = 0
    cells = []
    used_names = set()

    notebook_title = "Untitled"
    if i < len(lines) and lines[i].startswith("# "):
        notebook_title = lines[i][2:].strip()
        i += 1

    header_cell_name = unique_cell_name("Notebook Header", used_names)
    header_rendered = replace_markdown_urls(header_text, base_url)
    cells.append(create_markdown_cell(header_cell_name, header_rendered, collapsed=True))

    current_section_title = None
    section_markdown_buffer = []
    section_markdown_count = 0
    code_counts_by_section_lang = {}
    duration_for_section = None

    def flush_section_markdown(force_new_name=False):
        nonlocal section_markdown_buffer, section_markdown_count
        if current_section_title is None or not section_markdown_buffer:
            section_markdown_buffer = []
            return
        content = "\n".join(section_markdown_buffer).strip("\n")
        if not content:
            section_markdown_buffer = []
            return
        section_markdown_count += 1
        base_name = current_section_title if section_markdown_count == 1 and not force_new_name else f"{current_section_title} (cont. {section_markdown_count})"
        cell_name = unique_cell_name(base_name, used_names)
        content = replace_markdown_urls(content, base_url)
        cells.append(create_markdown_cell(cell_name, content, collapsed=False))
        section_markdown_buffer = []

    def start_new_section(title: str):
        nonlocal current_section_title, section_markdown_buffer, section_markdown_count
        flush_section_markdown()
        current_section_title = title.strip()
        section_markdown_buffer = [f"## {current_section_title}"]
        section_markdown_count = 0

    def inject_duration_if_present():
        nonlocal duration_for_section, section_markdown_buffer
        if current_section_title is None:
            return
        if duration_for_section is None:
            buf = []
            found = None
            duration_pattern = re.compile(r"Duration:\s*(\d+)")
            header_line = None
            for idx, ln in enumerate(section_markdown_buffer):
                if idx == 0 and re.match(r"^\s*##\s+", ln):
                    header_line = ln
                    continue
                if found is None and duration_pattern.search(ln):
                    m = duration_pattern.search(ln)
                    found = int(m.group(1))
                    ln = duration_pattern.sub("", ln).strip()
                    if ln:
                        buf.append(ln)
                    continue
                buf.append(ln)
            if found is not None:
                unit = "minute" if found == 1 else "minutes"
                duration_line = f"Duration: {found} {unit}"
                if header_line is not None:
                    section_markdown_buffer = [header_line, duration_line, ""] + buf
                else:
                    section_markdown_buffer = [duration_line, ""] + buf
                duration_for_section = found
            else:
                duration_for_section = -1

    code_fence_re = re.compile(r"^```(\w+)?\s*$")
    in_code = False
    code_lang_hint = None
    code_lines = []

    while i < len(lines):
        line = lines[i]
        fence_match = code_fence_re.match(line)
        if fence_match:
            if not in_code:
                inject_duration_if_present()
                flush_section_markdown()
                in_code = True
                code_lang_hint = fence_match.group(1) or ""
                code_lines = []
            else:
                in_code = False
                code_text = "\n".join(code_lines)
                language = detect_code_language(code_lang_hint, code_text)
                sec_key = current_section_title or "Global"
                key = (sec_key, language)
                code_counts_by_section_lang[key] = code_counts_by_section_lang.get(key, 0) + 1
                idx = code_counts_by_section_lang[key]
                base_name = f"{sec_key} SQL - Query {idx}" if language == "sql" else f"{sec_key} Python code {idx}"
                cell_name = unique_cell_name(base_name, used_names)
                cells.append(create_code_cell(cell_name, language, code_text))
                code_lines = []
            i += 1
            continue
        if in_code:
            code_lines.append(line)
            i += 1
            continue
        if line.startswith("## "):
            flush_section_markdown()
            duration_for_section = None
            title = line[3:].strip()
            start_new_section(title)
            i += 1
            continue
        elif line.startswith("# "):
            flush_section_markdown()
            duration_for_section = None
            title = line[2:].strip()
            start_new_section(title)
            i += 1
            continue
        section_markdown_buffer.append(line)
        i += 1
        inject_duration_if_present()

    flush_section_markdown()

    notebook = {
        "metadata": {"kernelspec": {"display_name": "Streamlit Notebook", "name": "streamlit"}},
        "nbformat_minor": 5,
        "nbformat": 4,
        "cells": cells,
    }

    duration_line_pattern = re.compile(r"^Duration:\s*(\d+)\s*(minutes?|)?\s*$", flags=re.IGNORECASE | re.MULTILINE)
    for c in notebook["cells"]:
        if c.get("cell_type") != "markdown":
            continue
        src = c.get("source", "")
        def repl(m):
            n = int(m.group(1))
            unit = "minute" if n == 1 else "minutes"
            return f"Duration: {n} {unit}\n"
        src2 = duration_line_pattern.sub(repl, src)
        src2 = re.sub(r"^(Duration:\s*\d+\s+(?:minute|minutes))\n(?!\n)", r"\1\n\n", src2, flags=re.IGNORECASE | re.MULTILINE)
        c["source"] = src2
    return notebook_title, notebook

def _split_stage_root_and_rel(stage_path: str, filename: str) -> tuple[str, str]:
    sp = stage_path.strip()
    assert sp.startswith("@")
    if "/" in sp:
        stage_root = sp.split("/", 1)[0]
        subdir = sp.split("/", 1)[1]
        rel = f"{subdir.rstrip('/')}/{filename}"
    else:
        stage_root = sp
        rel = filename
    return stage_root, rel

def convert(source_md: str, output_stage_path: str, main_file_name: str | None = None, query_warehouse: str | None = None):
    session = get_active_session()
    raw = fetch_text(source_md)
    header_text, body = extract_header_and_body(raw)
    content_id = parse_id_from_header(header_text)
    base_url = build_base_url(content_id) if content_id else ""
    title, nb = build_notebook(body, base_url, header_text)
    filename = main_file_name or (sanitize_filename(title) + ".ipynb")
    full_stage_path = output_stage_path.rstrip("/") + "/" + filename
    with SnowflakeFile.open(full_stage_path, "w") as f:
        f.write(json.dumps(nb, ensure_ascii=False, indent=1))
    stage_root, rel_main = _split_stage_root_and_rel(output_stage_path, filename)
    create_stmt = f"CREATE NOTEBOOK FROM '{stage_root}'\n MAIN_FILE = '{rel_main}'"
    if query_warehouse:
        create_stmt += f"\n QUERY_WAREHOUSE = {query_warehouse}"
    create_stmt += ";"
    session.sql(create_stmt).collect()
    return full_stage_path

def run(SOURCE_PATH: str, OUTPUT_STAGE_PATH: str, MAIN_FILE_NAME: str, QUERY_WAREHOUSE: str) -> str:
    return convert(SOURCE_PATH, OUTPUT_STAGE_PATH, MAIN_FILE_NAME or None, QUERY_WAREHOUSE or None)
$$;


