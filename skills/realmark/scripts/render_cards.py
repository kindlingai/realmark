#!/usr/bin/env python3
"""Render RealMark cards (.json, .ansi.txt, or .card.txt) into 2x Retina PNGs,
individual + combined landscape PDFs, and a static HTML gallery.

Preserves RealMark's built-in true-color ANSI theme (`ripal.py --render --color always`)
and enforces exact 1ch cell widths on Braille plot characters so box borders stay
pixel-aligned across fonts.

Requires: playwright (`pip install playwright && playwright install chromium`)
"""

import argparse
import html
import pathlib
import re
import subprocess
import sys

try:
    from playwright.sync_api import sync_playwright
except ImportError as exc:
    raise SystemExit(
        "playwright is required to render PNG/PDF cards: "
        "pip install playwright && playwright install chromium"
    ) from exc

ANSI_RE = re.compile(r"\x1b\[([0-9;]*)m")
DEFAULT_RIPAL = pathlib.Path(__file__).resolve().parents[3] / "ripal.py"


def extract_card_lines(raw: str) -> str:
    """Keep verbatim card box plus numbered footnotes, stripping local file path footers."""
    lines = raw.splitlines()
    start = 0
    for idx, line in enumerate(lines):
        plain = ANSI_RE.sub("", line)
        if "╭" in plain or "┌" in plain:
            start = idx
            break
    kept = []
    for line in lines[start:]:
        plain = ANSI_RE.sub("", line).strip()
        if plain.startswith(("Results:", "Card:", "Probe:")):
            continue
        kept.append(line)
    while kept and not ANSI_RE.sub("", kept[-1]).strip():
        kept.pop()
    return "\n".join(kept)


def render_segment(seg: str, fg: str | None, bold: bool, dim: bool, has_ansi: bool) -> str:
    styles = []
    if fg:
        styles.append(f"color:{fg}")
    if bold:
        styles.append("font-weight:700")
    if dim:
        styles.append("opacity:0.78")
    style_attr = f' style="{";".join(styles)}"' if styles else ""

    out = []
    buf = []

    def flush() -> None:
        if not buf:
            return
        txt = html.escape("".join(buf))
        buf.clear()
        out.append(f"<span{style_attr}>{txt}</span>" if style_attr else txt)

    for ch in seg:
        cp = ord(ch)
        if 0x2800 <= cp <= 0x28FF:
            flush()
            cls = "br" if has_ansi else "br br-default"
            out.append(f'<span class="{cls}"{style_attr}>{html.escape(ch)}</span>')
        elif not has_ansi and ch in "╭╮╰╯─│└┌┐┘":
            flush()
            out.append(f'<span class="bx">{html.escape(ch)}</span>')
        else:
            buf.append(ch)
    flush()
    return "".join(out)


def ansi_to_html(text: str) -> str:
    """Convert ANSI SGR codes (including 24-bit RGB) to cell-aligned HTML spans."""
    has_ansi = "\x1b[" in text
    out_lines = []
    for line in text.splitlines():
        fg = None
        bold = False
        dim = False
        parts = []
        pos = 0
        for match in ANSI_RE.finditer(line):
            seg = line[pos:match.start()]
            if seg:
                parts.append(render_segment(seg, fg, bold, dim, has_ansi))
            codes = [int(c) for c in match.group(1).split(";") if c.isdigit()] if match.group(1) else [0]
            i = 0
            while i < len(codes):
                code = codes[i]
                if code == 0:
                    fg, bold, dim = None, False, False
                elif code == 1:
                    bold = True
                elif code == 2:
                    dim = True
                elif code == 22:
                    bold, dim = False, False
                elif code == 39:
                    fg = None
                elif code == 38 and i + 4 < len(codes) and codes[i + 1] == 2:
                    fg = f"rgb({codes[i+2]},{codes[i+3]},{codes[i+4]})"
                    i += 4
                elif code == 48 and i + 4 < len(codes) and codes[i + 1] == 2:
                    i += 4
                i += 1
            pos = match.end()
        tail = line[pos:]
        if tail:
            parts.append(render_segment(tail, fg, bold, dim, has_ansi))
        out_lines.append("".join(parts))
    return "\n".join(out_lines)


def load_card_text(path: pathlib.Path, ripal_py: pathlib.Path) -> str:
    if path.suffix == ".json":
        if not ripal_py.exists():
            raise SystemExit(f"ripal.py not found at {ripal_py}; pass --ripal /path/to/ripal.py")
        proc = subprocess.run(
            [sys.executable, "-I", str(ripal_py), "--render", str(path), "--color", "always"],
            capture_output=True,
            text=True,
            check=True,
        )
        return extract_card_lines(proc.stdout)
    return extract_card_lines(path.read_text())


CARD_CSS = """
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #141210;
    padding: 24px;
    display: inline-block;
    font-family: "DejaVu Sans Mono", "Liberation Mono", ui-monospace, monospace;
    -webkit-font-smoothing: antialiased;
  }
  .term {
    background: #1c1917;
    border: 1px solid #35302c;
    border-radius: 12px;
    overflow: hidden;
    box-shadow: 0 18px 44px rgba(0,0,0,0.55);
    display: inline-block;
  }
  .bar {
    background: #231f1c;
    border-bottom: 1px solid #35302c;
    padding: 10px 16px;
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .dot { width: 11px; height: 11px; border-radius: 50%; display: inline-block; }
  .r { background: #ff5f56; }
  .y { background: #ffbd2e; }
  .g { background: #27c93f; }
  .title {
    margin-left: 10px;
    color: #e6e0d6;
    font-size: 13px;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    font-weight: 600;
  }
  .badge {
    margin-left: auto;
    color: #eab531;
    background: rgba(234, 181, 49, 0.12);
    border: 1px solid rgba(234, 181, 49, 0.32);
    border-radius: 999px;
    padding: 2px 10px;
    font-size: 11.5px;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    font-weight: 600;
  }
  pre {
    padding: 18px 22px 20px 22px;
    color: #e6e0d6;
    font-family: "DejaVu Sans Mono", "Liberation Mono", ui-monospace, monospace;
    font-size: 13.5px;
    line-height: 1.18;
    white-space: pre;
    font-variant-ligatures: none;
  }
  .br { display: inline-block; width: 1ch; text-align: center; }
  .br-default { color: #ff7019; }
  .bx { color: #92897b; }
"""


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--ripal",
        type=pathlib.Path,
        default=DEFAULT_RIPAL,
        help="Path to ripal.py (default: repo root ripal.py)",
    )
    ap.add_argument("--out-dir", type=pathlib.Path, required=True)
    ap.add_argument("--title", default="RealMark Results")
    ap.add_argument("--subtitle", default="")
    ap.add_argument("--badge", default="RealMark")
    ap.add_argument("--note", default="")
    ap.add_argument("--pdf-name", default="realmark-all-cards.pdf")
    ap.add_argument(
        "--card",
        action="append",
        required=True,
        help="Format: slug|Title|path_to_json_or_card_txt (repeatable)",
    )
    args = ap.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    items = []
    for spec in args.card:
        parts = spec.split("|", 2)
        if len(parts) != 3:
            raise SystemExit(f"Invalid --card spec (expected slug|Title|path): {spec}")
        slug, title, src = parts
        raw_card = load_card_text(pathlib.Path(src), args.ripal)
        items.append((slug, title, ansi_to_html(raw_card)))

    pdf_page_css = """
      @page { size: 11in 8.5in; margin: 0; }
      html, body {
        background: #141210; padding: 0; display: block;
        -webkit-print-color-adjust: exact; print-color-adjust: exact;
      }
      .page {
        width: 11in; height: 8.5in; background: #141210;
        display: flex; align-items: center; justify-content: center;
        page-break-after: always; break-after: page;
      }
      .page:last-child { page-break-after: auto; break-after: auto; }
      pre { font-size: 12.6px; }
    """

    with sync_playwright() as p:
        browser = p.chromium.launch()
        png_page = browser.new_page(viewport={"width": 1600, "height": 1400}, device_scale_factor=2)
        pdf_page = browser.new_page()
        all_pdf_blocks = []

        for slug, title, body_html in items:
            card_div = f"""
            <div class="term">
              <div class="bar">
                <span class="dot r"></span><span class="dot y"></span><span class="dot g"></span>
                <span class="title">{html.escape(title)}</span>
                <span class="badge">{html.escape(args.badge)}</span>
              </div>
              <pre>{body_html}</pre>
            </div>"""

            png_doc = f"<!doctype html><html><head><meta charset='utf-8'><style>{CARD_CSS}</style></head><body>{card_div}</body></html>"
            png_page.set_content(png_doc)
            png_page.locator("body").screenshot(path=str(args.out_dir / f"{slug}.png"))

            blk = f'<div class="page">{card_div}</div>'
            all_pdf_blocks.append(blk)
            pdf_doc = f"<!doctype html><html><head><meta charset='utf-8'><style>{CARD_CSS}{pdf_page_css}</style></head><body>{blk}</body></html>"
            pdf_page.set_content(pdf_doc)
            pdf_page.pdf(
                path=str(args.out_dir / f"{slug}.pdf"),
                width="11in",
                height="8.5in",
                print_background=True,
                margin={"top": "0", "right": "0", "bottom": "0", "left": "0"},
            )

        combined_doc = f"<!doctype html><html><head><meta charset='utf-8'><style>{CARD_CSS}{pdf_page_css}</style></head><body>{''.join(all_pdf_blocks)}</body></html>"
        pdf_page.set_content(combined_doc)
        pdf_page.pdf(
            path=str(args.out_dir / args.pdf_name),
            width="11in",
            height="8.5in",
            print_background=True,
            margin={"top": "0", "right": "0", "bottom": "0", "left": "0"},
        )
        browser.close()

    cards_html = []
    for slug, title, _ in items:
        cards_html.append(f"""
        <div class="card">
          <div class="card-head">
            <div class="card-title">{html.escape(title)}</div>
            <div class="actions">
              <a class="btn" href="{slug}.png" target="_blank">Open PNG</a>
              <a class="btn btn-png" href="{slug}.png" download="{slug}.png">Download PNG</a>
              <a class="btn btn-pdf" href="{slug}.pdf" download="{slug}.pdf">Download PDF</a>
            </div>
          </div>
          <a href="{slug}.png" target="_blank"><img src="{slug}.png" alt="{html.escape(title)}" loading="lazy"></a>
        </div>""")

    note_html = f'<div class="note">{html.escape(args.note)}</div>' if args.note else ""
    index_html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(args.title)}</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ margin: 0; background: #0d0c0b; color: #e6e0d6; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; padding: 28px 20px 64px; }}
  .wrap {{ max-width: 1120px; margin: 0 auto; }}
  header {{ border-bottom: 1px solid #2b2623; padding-bottom: 18px; margin-bottom: 24px; display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; flex-wrap: wrap; }}
  h1 {{ margin: 0 0 6px; font-size: 25px; color: #f8f5f0; }}
  .lead {{ margin: 0; color: #92897b; font-size: 14.5px; }}
  .note {{ background: rgba(255,112,25,0.1); border: 1px solid rgba(255,112,25,0.3); border-radius: 10px; padding: 12px 16px; margin-bottom: 24px; font-size: 13.5px; color: #e6e0d6; line-height: 1.5; }}
  .stack {{ display: flex; flex-direction: column; gap: 22px; }}
  .card {{ background: #161412; border: 1px solid #2b2623; border-radius: 12px; padding: 14px; }}
  .card-head {{ display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 12px; flex-wrap: wrap; }}
  .card-title {{ font-weight: 600; font-size: 15px; color: #f8f5f0; }}
  .actions {{ display: flex; gap: 8px; flex-wrap: wrap; }}
  .btn {{ text-decoration: none; color: #e6e0d6; background: #26221f; border: 1px solid #3a342f; border-radius: 7px; padding: 6px 12px; font-size: 12.5px; font-weight: 600; }}
  .btn:hover {{ background: #35302c; }}
  .btn-png {{ color: #1c1917; background: #eab531; border-color: #eab531; }}
  .btn-png:hover {{ background: #f3c859; }}
  .btn-pdf {{ color: #fff; background: #ff7019; border-color: #ff7019; }}
  .btn-pdf:hover {{ background: #ff863d; }}
  .btn-lg {{ padding: 10px 16px; font-size: 13.5px; border-radius: 9px; }}
  img {{ width: 100%; height: auto; display: block; border-radius: 8px; }}
</style>
</head>
<body>
  <div class="wrap">
    <header>
      <div>
        <h1>{html.escape(args.title)}</h1>
        <p class="lead">{html.escape(args.subtitle)}</p>
      </div>
      <div class="actions">
        <a class="btn btn-pdf btn-lg" href="{html.escape(args.pdf_name)}" download="{html.escape(args.pdf_name)}">Download All Cards (PDF)</a>
      </div>
    </header>
    {note_html}
    <div class="stack">{''.join(cards_html)}</div>
  </div>
</body>
</html>"""
    (args.out_dir / "index.html").write_text(index_html)
    print(f"Rendered {len(items)} card(s) + {args.pdf_name} + index.html -> {args.out_dir}")


if __name__ == "__main__":
    main()
