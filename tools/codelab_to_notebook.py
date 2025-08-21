#!/usr/bin/env python3
import sys
import os
import re
import json
import tempfile
from urllib.parse import urlparse
from urllib.request import urlopen

try:
    # Optional Snowflake imports (available when running inside Snowflake)
    from snowflake.snowpark.context import get_active_session  # type: ignore
    from snowflake.snowpark.files import SnowflakeFile  # type: ignore
except Exception:  # noqa: BLE001
    get_active_session = None
    SnowflakeFile = None


def is_stage_path(path: str) -> bool:
    return isinstance(path, str) and path.strip().startswith("@")


def fetch_text(source: str, session=None) -> str:
    # Stage path
    if is_stage_path(source):
        if session is None and get_active_session is not None:
            session = get_active_session()
        if SnowflakeFile is None or session is None:
            raise RuntimeError("Reading from a stage requires a Snowflake session.")
        with SnowflakeFile.open(source, "r") as f:  # type: ignore[attr-defined]
            return f.read()
    # URL
    if source.startswith("http://") or source.startswith("https://"):
        with urlopen(source) as resp:
            return resp.read().decode("utf-8", errors="replace")
    # Local file
    with open(source, "r", encoding="utf-8") as f:
        return f.read()


def sanitize_filename(name: str) -> str:
    name = name.strip().replace("/", "-")
    return re.sub(r"[\\:*?\"<>|]", "-", name)


def extract_header_and_body(md: str):
    lines = md.splitlines()
    header_lines = []
    i = 0
    # Collect header lines until first H1
    while i < len(lines) and not lines[i].startswith("# "):
        header_lines.append(lines[i])
        i += 1
    body = "\n".join(lines[i:])
    header_text = "\n".join(header_lines).strip()
    return header_text, body


def parse_id_from_header(header_text: str) -> str:
    # Look for a line starting with id:
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
    # Treat other non-absolute, non-rooted as relative (e.g., assets/..., images/...)
    if not url.startswith("/"):
        return base_url + url
    # If it starts with '/', assume it's site-rooted and leave it as-is
    return url


def replace_markdown_urls(text: str, base_url: str) -> str:
    # Remove HTML comments entirely
    text = re.sub(r"<!--[\s\S]*?-->", "", text)

    # Images: ![alt](url "title")
    def img_sub(m):
        alt = m.group(1)
        url = m.group(2)
        title = m.group(3) or ""
        new_url = rewrite_relative_url(url, base_url)
        title_part = f' "{title}"' if title else ""
        return f"![{alt}]({new_url}{title_part})"

    # Links: [text](url "title")
    def link_sub(m):
        text = m.group(1)
        url = m.group(2)
        title = m.group(3) or ""
        new_url = rewrite_relative_url(url, base_url)
        title_part = f' "{title}"' if title else ""
        return f"[{text}]({new_url}{title_part})"

    # Convert <img ...> to markdown image first, so subsequent rewriting applies
    def html_img_to_md(match):
        attrs = match.group(1)
        # Support both single and double quoted attrs
        def get_attr(name: str):
            m = re.search(rf"{name}\\s*=\\s*([\"'])((?:(?!\\1).)*)\\1", attrs)
            return m.group(2) if m else None
        src_val = get_attr("src")
        if not src_val:
            return match.group(0)
        alt_val = get_attr("alt") or "image"
        title_val = get_attr("title") or "Image"
        full_url = rewrite_relative_url(src_val, base_url)
        return f"![{alt_val}]({full_url} \"{title_val}\")"

    # Convert HTML <img> tags (robust capture)
    def html_img_any(m):
        tag = m.group(0)
        # Extract src, alt, title with support for single/double quotes
        src_m = re.search(r"src\\s*=\\s*'([^']+)'", tag, re.IGNORECASE) or re.search(r' src\\s*=\\s*"([^"]+)"', tag, re.IGNORECASE)
        if not src_m:
            return tag
        alt_m = re.search(r"alt\\s*=\\s*'([^']*)'", tag, re.IGNORECASE) or re.search(r' alt\\s*=\\s*"([^"]*)"', tag, re.IGNORECASE)
        title_m = re.search(r"title\\s*=\\s*'([^']*)'", tag, re.IGNORECASE) or re.search(r' title\\s*=\\s*"([^"]*)"', tag, re.IGNORECASE)
        src_val = src_m.group(1)
        alt_val = alt_m.group(1) if alt_m else "image"
        title_val = title_m.group(1) if title_m else "Image"
        full_url = rewrite_relative_url(src_val, base_url)
        return f"![{alt_val}]({full_url} \"{title_val}\")"

    text = re.sub(r"<img[^>]*?>", html_img_any, text, flags=re.IGNORECASE)

    # Rewrite markdown images
    text = re.sub(r"!\[([^\]]*)\]\(([^\s\)]+)(?:\s+\"([^\"]*)\")?\)", img_sub, text)
    # Rewrite markdown links
    text = re.sub(r"\[([^\]]+)\]\(([^\s\)]+)(?:\s+\"([^\"]*)\")?\)", link_sub, text)
    return text


def detect_code_language(explicit_lang: str, code_text: str) -> str:
    if explicit_lang:
        lang = explicit_lang.strip().lower()
        if lang in ("sql", "snowflake-sql"):
            return "sql"
        if lang in ("python", "py"):
            return "python"
    # Heuristic detection
    sample = code_text.strip()
    if re.search(r"\\bSELECT\\b|\\bCREATE\\b|\\bWITH\\b|\\bINSERT\\b|\\bUPDATE\\b|\\bDELETE\\b", sample, re.IGNORECASE):
        return "sql"
    if re.search(r"\\bimport\\b|\\bdef\\b|\\bclass\\b|print\\(\\)|from\\s+\\w+\\s+import", sample):
        return "python"
    # Default to python if unknown
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
    return {
        "cell_type": "markdown",
        "metadata": {"name": name, "collapsed": collapsed},
        "source": text,
    }


def create_code_cell(name: str, language: str, code: str):
    return {
        "cell_type": "code",
        "metadata": {"language": language, "name": name},
        "source": code,
        "execution_count": None,
        "outputs": [],
    }


def build_notebook(md_body: str, base_url: str, header_text: str):
    lines = md_body.splitlines()
    i = 0
    cells = []
    used_names = set()

    # First-level heading determines notebook name
    notebook_title = "Untitled"
    if i < len(lines) and lines[i].startswith("# "):
        notebook_title = lines[i][2:].strip()
        i += 1

    # Add header-only collapsed cell
    header_cell_name = unique_cell_name("Notebook Header", used_names)
    header_rendered = replace_markdown_urls(header_text, base_url)
    cells.append(create_markdown_cell(header_cell_name, header_rendered, collapsed=True))

    current_section_title = None
    section_markdown_buffer = []
    section_markdown_count = 0
    code_counts_by_section_lang = {}

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
        if section_markdown_count == 1 and not force_new_name:
            base_name = current_section_title
        else:
            base_name = f"{current_section_title} (cont. {section_markdown_count})"
        cell_name = unique_cell_name(base_name, used_names)
        # Rewrite relative URLs inside markdown
        content = replace_markdown_urls(content, base_url)
        cells.append(create_markdown_cell(cell_name, content, collapsed=False))
        section_markdown_buffer = []

    # Helper to start a new section on encountering H2
    def start_new_section(title: str):
        nonlocal current_section_title, section_markdown_buffer, section_markdown_count
        # Finish previous section's pending markdown
        flush_section_markdown()
        current_section_title = title.strip()
        # Initialize buffer with the header line (to be retained in the cell)
        section_markdown_buffer = [f"## {current_section_title}"]
        section_markdown_count = 0

    # Track whether we've injected the Duration line at the top of the first markdown cell per section
    duration_for_section = None

    def inject_duration_if_present():
        nonlocal duration_for_section, section_markdown_buffer
        if current_section_title is None:
            return
        if duration_for_section is None:
            # Attempt to find and remove a Duration occurrence from the top region of the buffer
            buf = []
            found = None
            duration_pattern = re.compile(r"Duration:\s*(\d+)")
            header_line = None
            for idx, ln in enumerate(section_markdown_buffer):
                # Capture and skip the first header line if present (retain order later)
                if idx == 0 and re.match(r"^\s*##\s+", ln):
                    header_line = ln
                    continue
                if found is None and duration_pattern.search(ln):
                    m = duration_pattern.search(ln)
                    found = int(m.group(1))
                    # Remove just the duration token from the line, keep remaining content
                    ln = duration_pattern.sub("", ln).strip()
                    if ln:
                        buf.append(ln)
                    continue
                buf.append(ln)
            if found is not None:
                unit = "minute" if found == 1 else "minutes"
                duration_line = f"Duration: {found} {unit}"
                if header_line is not None:
                    # Place Duration immediately after header
                    section_markdown_buffer = [header_line, duration_line, ""] + buf
                else:
                    section_markdown_buffer = [duration_line, ""] + buf
                duration_for_section = found
            else:
                duration_for_section = -1

    # Process body line-by-line, handling code fences and headings
    code_fence_re = re.compile(r"^```(\w+)?\s*$")
    in_code = False
    code_lang_hint = None
    code_lines = []

    while i < len(lines):
        line = lines[i]

        # Start or end of code fence
        fence_match = code_fence_re.match(line)
        if fence_match:
            if not in_code:
                # Starting code block: ensure any preceding text becomes its own cell
                inject_duration_if_present()
                flush_section_markdown()
                in_code = True
                code_lang_hint = fence_match.group(1) or ""
                code_lines = []
            else:
                # Ending code block: create a code cell
                in_code = False
                code_text = "\n".join(code_lines)
                language = detect_code_language(code_lang_hint, code_text)
                sec_key = current_section_title or "Global"
                key = (sec_key, language)
                code_counts_by_section_lang[key] = code_counts_by_section_lang.get(key, 0) + 1
                idx = code_counts_by_section_lang[key]
                if language == "sql":
                    base_name = f"{sec_key} SQL - Query {idx}"
                else:
                    base_name = f"{sec_key} Python code {idx}"
                cell_name = unique_cell_name(base_name, used_names)
                cells.append(create_code_cell(cell_name, language, code_text))
                code_lines = []
            i += 1
            continue

        if in_code:
            code_lines.append(line)
            i += 1
            continue

        # Headings
        if line.startswith("## "):
            # New H2 section
            # Flush any accumulated markdown for previous section
            flush_section_markdown()
            duration_for_section = None
            title = line[3:].strip()
            start_new_section(title)
            # Initialize buffer with title as a markdown heading? Spec says cell starts at H2, but do we include the H2 line itself? It says each 2nd level heading indicates the start of a new Notebook cell, use heading text to set the name. Content of cell should include the Duration and other text, not necessarily repeat the header line. We'll omit duplicating the header line inside the cell.
            i += 1
            continue
        elif line.startswith("# "):
            # Additional H1 encountered; treat as plain text separator
            # Flush current markdown before switching contexts
            flush_section_markdown()
            # Start a pseudo-section for this H1
            duration_for_section = None
            title = line[2:].strip()
            start_new_section(title)
            i += 1
            continue

        # Regular content lines; accumulate into section buffer
        section_markdown_buffer.append(line)
        i += 1

        # Ensure duration appears at top of first section cell when we first add content lines after H2
        inject_duration_if_present()

    # End of file: flush any remaining markdown
    flush_section_markdown()

    notebook = {
        "metadata": {
            "kernelspec": {"display_name": "Streamlit Notebook", "name": "streamlit"}
        },
        "nbformat_minor": 5,
        "nbformat": 4,
        "cells": cells,
    }

    # Post-process: normalize all Duration lines to include units and a blank line after
    duration_line_pattern = re.compile(
        r"^Duration:\s*(\d+)\s*(minutes?|)?\s*$",
        flags=re.IGNORECASE | re.MULTILINE,
    )
    for c in notebook["cells"]:
        if c.get("cell_type") != "markdown":
            continue
        src = c.get("source", "")

        def repl(m: re.Match) -> str:
            n = int(m.group(1))
            unit = "minute" if n == 1 else "minutes"
            return f"Duration: {n} {unit}\n"

        src2 = duration_line_pattern.sub(repl, src)
        # Ensure exactly one blank line after Duration line (i.e., two newlines total)
        src2 = re.sub(
            r"^(Duration:\s*\d+\s+(?:minute|minutes))\n(?!\n)",
            r"\1\n\n",
            src2,
            flags=re.IGNORECASE | re.MULTILINE,
        )
        c["source"] = src2
    return notebook_title, notebook


def _split_stage_root_and_rel(stage_path: str, filename: str) -> tuple[str, str]:
    # stage_path like @db.schema.stage or @db.schema.stage/sub/dir
    sp = stage_path.strip()
    assert sp.startswith("@")
    # Find first slash after stage name
    if "/" in sp:
        stage_root = sp.split("/", 1)[0]
        subdir = sp.split("/", 1)[1]
        rel = f"{subdir.rstrip('/')}/{filename}"
    else:
        stage_root = sp
        rel = filename
    return stage_root, rel


def convert(source_md: str, output_path: str = None, main_file_name: str | None = None, query_warehouse: str | None = None, session=None):
    raw = fetch_text(source_md, session=session)
    header_text, body = extract_header_and_body(raw)
    content_id = parse_id_from_header(header_text)
    base_url = build_base_url(content_id) if content_id else ""

    title, nb = build_notebook(body, base_url, header_text)
    filename = main_file_name or (sanitize_filename(title) + ".ipynb")

    # Stage output
    if output_path and is_stage_path(output_path):
        if session is None and get_active_session is not None:
            session = get_active_session()
        if SnowflakeFile is None or session is None:
            raise RuntimeError("Writing to a stage requires a Snowflake session.")
        # Write to a temporary local file, then PUT to stage
        tmp_dir = tempfile.mkdtemp()
        local_path = os.path.join(tmp_dir, filename)
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(nb, f, ensure_ascii=False, indent=1)
        stage_dir = output_path.rstrip("/") + "/"
        session.file.put(local_path, stage_dir, overwrite=True, auto_compress=False)
        full_stage_path = stage_dir + filename

        # Issue CREATE NOTEBOOK FROM @stage MAIN_FILE='relpath'
        stage_root, rel_main = _split_stage_root_and_rel(output_path, filename)
        create_stmt = f"CREATE NOTEBOOK FROM '{stage_root}'\n MAIN_FILE = '{rel_main}'"
        if query_warehouse:
            create_stmt += f"\n QUERY_WAREHOUSE = {query_warehouse}"
        create_stmt += ";"
        session.sql(create_stmt).collect()
        return full_stage_path

    # Local file output
    filename_local = filename if not output_path else os.path.basename(output_path)
    out_file = output_path or os.path.join(os.getcwd(), filename_local)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(nb, f, ensure_ascii=False, indent=1)
    return out_file


def main():
    if len(sys.argv) < 2:
        print("Usage: codelab_to_notebook.py <markdown_file_or_url> [output_file]", file=sys.stderr)
        sys.exit(1)
    source = sys.argv[1]
    output = sys.argv[2] if len(sys.argv) > 2 else None
    out_file = convert(source, output)
    print(out_file)


if __name__ == "__main__":
    main()


