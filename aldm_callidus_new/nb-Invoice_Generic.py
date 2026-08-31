# Databricks notebook source
# MAGIC %md
# MAGIC # Generic monthly and mid-monthly invoice PDF generator
# MAGIC
# MAGIC Called once per invoice. `invoice_type` controls both the Snowflake
# MAGIC source and the invoice body:
# MAGIC - `MONTHLY`: sales period, OTHER/MBB/HBB sections and section totals
# MAGIC - `MIDMONTHLY` (also accepts `MID_MONTHLY`): flat rows and invoice totals

# COMMAND ----------

# MAGIC %pip install reportlab snowflake-connector-python pandas PyPDF2

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Aldm Repository/ALDM/databricks/config/config"

# COMMAND ----------

dbutils.widgets.text("invoice_no", "")
dbutils.widgets.text("invoice_type", "")
dbutils.widgets.text("extract_filename_format", "")
dbutils.widgets.text("interface_home", "CALLIDUS/INVOICE")
dbutils.widgets.text("fs_dir_path", "callidus/INVOICE/")

invoice_no = dbutils.widgets.get("invoice_no").strip()
period = dbutils.widgets.get("period").strip()
invoice_type_input = dbutils.widgets.get("invoice_type").strip()
extract_filename_format = dbutils.widgets.get(
    "extract_filename_format"
).strip()
interface_home = dbutils.widgets.get("interface_home").strip()
fs_dir_path = dbutils.widgets.get("fs_dir_path").strip()


def normalise_invoice_type(value):
    normalised = value.upper().replace("-", "").replace("_", "").replace(" ", "")
    aliases = {
        "MONTHLY": "MONTHLY",
        "MIDMONTHLY": "MIDMONTHLY",
    }
    if normalised not in aliases:
        raise ValueError(
            "Widget 'invoice_type' must be MONTHLY or MIDMONTHLY "
            f"(MID_MONTHLY is also accepted); received: {value!r}"
        )
    return aliases[normalised]


invoice_type = normalise_invoice_type(invoice_type_input)

if not invoice_no:
    raise ValueError("Widget 'invoice_no' is required.")
if not extract_filename_format:
    raise ValueError("Widget 'extract_filename_format' is required.")

# COMMAND ----------

import os
import re

import pandas as pd
import snowflake.connector
from PyPDF2 import PdfMerger
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfgen import canvas
from reportlab.platypus import Paragraph, Table, TableStyle

if extract_filename_format != os.path.basename(extract_filename_format):
    raise ValueError(
        "extract_filename_format must be a file name, not a path."
    )
if not extract_filename_format.lower().endswith(".pdf"):
    raise ValueError("extract_filename_format must end with '.pdf'.")

# COMMAND ----------

# The table name is selected from a fixed map; no caller input is interpolated
# into SQL identifiers.
SOURCE_TABLES = {
    "MONTHLY": f"{sfDatabase}.PRS_CEG_EXT.CALLIDUS_INVOICE_PDF_MONTHLY",
    "MIDMONTHLY":  f"{sfDatabase}.PRS_CEG_EXT.CALLIDUS_INVOICE_PDF_MIDMONTHLY",
}
source_table = SOURCE_TABLES[invoice_type]

sf_options_py = {
    "user": sfUser,
    "private_key": pem_private_key,
    "account":"THREEMOBILE.west-europe.azure",
    "database": sfDatabase,
    "warehouse": sfWarehouse,
    "schema": sfNondoxTgtSchema,
    "disable_ocsp_checks": "True",
}

invoice_query = f"""
SELECT *
FROM {source_table}
WHERE INVOICE_NO = %(invoice_no)s
  AND PERIOD_NAME = %(period)s
ORDER BY PARTICIPANT_NAME, INVOICE_NO, EARNING_GROUP
"""

with snowflake.connector.connect(**sf_options_py) as conn:
    invoice_df = pd.read_sql(
        invoice_query,
        conn,
        params={"invoice_no": invoice_no, "period": period},
    )

if invoice_df.empty:
    raise ValueError(
        f"No {invoice_type} records found for invoice_no '{invoice_no}' "
        f"in {source_table}."
    )

returned_invoice_numbers = {
    str(value).strip()
    for value in invoice_df["INVOICE_NO"].dropna().unique()
}
if returned_invoice_numbers != {invoice_no}:
    raise ValueError(
        "Snowflake returned unexpected invoice numbers: "
        f"{sorted(returned_invoice_numbers)}"
    )

print("Invoice type  :", invoice_type)
print("Invoice number:", invoice_no)
print("Source table  :", source_table)
print("Rows loaded   :", len(invoice_df))

# COMMAND ----------

output_path = f"{mft_out}/aldm/outbound_1/{interface_home}"
invoice_pdf_path = os.path.join(output_path, extract_filename_format)
disclaimer_path = os.path.join(
    output_path,
    "DISCLAIMER",
    "DISCLAIMER_1.pdf",
)
individual_pdf_path = os.path.join(
    output_path,
    "INDIVIDUAL",
    extract_filename_format,
)
logo_path = os.path.join(output_path, "DISCLAIMER", "logo.png")
os.makedirs(output_path, exist_ok=True)

COMPANY_NAME = "Three"
COMPANY_ADDRESS = [
    "Vodafone House,",
    "The Connection,",
    "Newbury,",
    "Berkshire,",
    "RG14 2FN",
    "United Kingdom",
]
COMPANY_WEBSITE = "Three.co.uk"
COMPANY_VAT_NUMBER = "GB 760729222"
REGISTERED_OFFICE = [
    "Registered Office : Hutchison 3G UK Limited",
    "Vodafone House, The Connection, Newbury,",
    "Berkshire, RG14 2FN",
    "Registered Number: 3885486 England & Wales",
]
VAT_NOTE = "<b>Note:</b> The VAT shown is your output tax<br/>due to HMRC."

PAGE_SIZE = A4
PAGE_WIDTH, PAGE_HEIGHT = PAGE_SIZE
LEFT = 18
RIGHT = 577
CONTENT_WIDTH = RIGHT - LEFT
CENTRE = (LEFT + RIGHT) / 2
HEADER_TOP = 18
HEADER_HEIGHT = 107
META_TOP = 140
FOOTER_RULE_TOP = 768
FONT = "Helvetica"
FONT_BOLD = "Helvetica-Bold"

DETAILS_GUTTER = 5
DETAILS_LEFT = 336.5
PARTY_WIDTH = DETAILS_LEFT - DETAILS_GUTTER - LEFT
DETAILS_LABEL_WIDTH = 130
DETAILS_VALUE_LEFT = DETAILS_LEFT + DETAILS_LABEL_WIDTH
DETAILS_VALUE_WIDTH = RIGHT - DETAILS_VALUE_LEFT

NET_COLUMN_LEFT = DETAILS_LEFT + pdfmetrics.stringWidth(
    "Paym", FONT_BOLD, 10
)
VAT_COLUMN_LEFT = DETAILS_VALUE_LEFT + pdfmetrics.stringWidth(
    ": 30 day", FONT, 10
)
DESCRIPTION_WIDTH = NET_COLUMN_LEFT - LEFT
NET_WIDTH = VAT_COLUMN_LEFT - NET_COLUMN_LEFT
VAT_WIDTH = CONTENT_WIDTH - DESCRIPTION_WIDTH - NET_WIDTH
NET_RIGHT_EDGE = NET_COLUMN_LEFT + pdfmetrics.stringWidth(
    "Net (£ value)", FONT_BOLD, 12
)
VAT_RIGHT_EDGE = VAT_COLUMN_LEFT + pdfmetrics.stringWidth(
    "VAT Rate", FONT_BOLD, 12
)
NET_RIGHT_PADDING = NET_COLUMN_LEFT + NET_WIDTH - NET_RIGHT_EDGE
VAT_RIGHT_PADDING = VAT_WIDTH - pdfmetrics.stringWidth(
    "VAT Rate", FONT_BOLD, 12
)

TOTAL_LABEL_WIDTH = 160
TOTAL_CURRENCY_WIDTH = 26
TOTAL_AMOUNT_WIDTH = 88
TOTALS_WIDTH = (
    TOTAL_LABEL_WIDTH + TOTAL_CURRENCY_WIDTH + TOTAL_AMOUNT_WIDTH
)
TOTALS_LEFT = NET_RIGHT_EDGE - TOTALS_WIDTH
NOTE_LEFT = 24
NOTE_WIDTH = 182


def y_from_top(value):
    return PAGE_HEIGHT - value


def safe_text(value):
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def xml_text(value):
    return (
        safe_text(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def line_amount(value):
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return ""
    text = f"{abs(amount):,.2f}"
    return f"({text})" if amount < 0 else text


def percent(value):
    try:
        return f"{float(value):g}%"
    except (TypeError, ValueError):
        return ""


def format_date(value):
    parsed = pd.to_datetime(value, errors="coerce")
    return "" if pd.isna(parsed) else parsed.strftime("%d/%m/%Y")


def calc_totals(items):
    values = pd.to_numeric(items["VALUE"], errors="coerce").fillna(0)
    rates = pd.to_numeric(items["VAT_RATE"], errors="coerce").fillna(0)
    net = round(float(values.sum()), 2)
    vat = round(float((values * rates / 100).sum()), 2)
    return net, vat, round(net + vat, 2)


def document_title(net_total):
    return "Self Billed Invoice" if net_total >= 0 else "Self Credit"


def period_columns(columns):
    start = None
    end = None
    for column in columns:
        key = str(column).upper().replace(" ", "_").replace("-", "_")
        if "PERIO" not in key:
            continue
        if start is None and ("START" in key or "FROM" in key):
            start = column
        if end is None and ("END" in key or key.endswith("_TO")):
            end = column
    return start, end


ISO_DATE = re.compile(r"^(\d{1,4})-(\d{1,2})-(\d{1,2})")


def period_date(value):
    text = safe_text(value)
    if not text:
        return ""
    match = ISO_DATE.match(text)
    if match:
        year, month, day = (int(part) for part in match.groups())
    else:
        parsed = pd.to_datetime(text, errors="coerce", dayfirst=True)
        if pd.isna(parsed):
            return text
        year, month, day = parsed.year, parsed.month, parsed.day
    if year < 100:
        year += 2000
    return f"{day}/{month}/{year}"


def sales_period_text(row):
    start_column, end_column = period_columns(row.index)
    start = period_date(row[start_column]) if start_column else ""
    end = period_date(row[end_column]) if end_column else ""
    if start and end:
        return f"{start} to {end}"
    if start or end:
        return start or end
    for column in ("SALES_PERIOD", "PERIOD", "COMMISSION_PERIOD"):
        if column in row.index and safe_text(row[column]):
            return safe_text(row[column])
    return ""


def section_key(earning_group):
    name = safe_text(earning_group).upper()
    if name.startswith("HBB"):
        return "HBB"
    if name.startswith("MBB"):
        return "MBB"
    return "OTHER"

# COMMAND ----------

STYLES = {
    "party": ParagraphStyle(
        "party", fontName=FONT_BOLD, fontSize=10, leading=12.5
    ),
    "title": ParagraphStyle(
        "title",
        fontName=FONT_BOLD,
        fontSize=12,
        leading=14,
        alignment=TA_CENTER,
        spaceAfter=8,
    ),
    "label": ParagraphStyle(
        "label", fontName=FONT_BOLD, fontSize=10, leading=12
    ),
    "value": ParagraphStyle(
        "value", fontName=FONT, fontSize=10, leading=12
    ),
    "header": ParagraphStyle(
        "header", fontName=FONT_BOLD, fontSize=12, leading=14
    ),
    "period": ParagraphStyle(
        "period", fontName=FONT_BOLD, fontSize=10, leading=12
    ),
    "row": ParagraphStyle(
        "row", fontName=FONT, fontSize=10, leading=11.5
    ),
    "row_right": ParagraphStyle(
        "row_right",
        fontName=FONT,
        fontSize=10,
        leading=11.5,
        alignment=TA_RIGHT,
    ),
    "total_label": ParagraphStyle(
        "total_label",
        fontName=FONT,
        fontSize=11,
        leading=13,
        alignment=TA_RIGHT,
    ),
    "total_label_bold": ParagraphStyle(
        "total_label_bold",
        fontName=FONT_BOLD,
        fontSize=11,
        leading=13,
        alignment=TA_RIGHT,
    ),
    "total_currency": ParagraphStyle(
        "total_currency", fontName=FONT, fontSize=10, leading=13
    ),
    "total_amount": ParagraphStyle(
        "total_amount",
        fontName=FONT_BOLD,
        fontSize=10,
        leading=13,
        alignment=TA_RIGHT,
    ),
    "note": ParagraphStyle(
        "note", fontName=FONT, fontSize=10, leading=12
    ),
}

NO_PADDING = [
    ("LEFTPADDING", (0, 0), (-1, -1), 0),
    ("RIGHTPADDING", (0, 0), (-1, -1), 0),
    ("TOPPADDING", (0, 0), (-1, -1), 0),
    ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ("VALIGN", (0, 0), (-1, -1), "TOP"),
]


def draw_company_header(pdf):
    pdf.setLineWidth(1)
    pdf.rect(
        LEFT,
        y_from_top(HEADER_TOP + HEADER_HEIGHT),
        CONTENT_WIDTH,
        HEADER_HEIGHT,
    )
    pdf.setFont(FONT_BOLD, 11)
    text_top = 34
    for line in [COMPANY_NAME] + COMPANY_ADDRESS:
        pdf.drawString(30, y_from_top(text_top), line)
        text_top += 13.5
    pdf.setFont(FONT_BOLD, 10)
    pdf.drawCentredString(CENTRE, y_from_top(69), COMPANY_WEBSITE)
    pdf.drawCentredString(
        CENTRE,
        y_from_top(100),
        f"Vat Reg. No:  {COMPANY_VAT_NUMBER}",
    )
    if logo_path and os.path.isfile(logo_path):
        pdf.drawImage(
            logo_path,
            RIGHT - 142,
            y_from_top(106),
            width=130,
            height=70,
            preserveAspectRatio=True,
            anchor="c",
            mask="auto",
        )


def draw_footer(pdf):
    pdf.setLineWidth(1)
    pdf.line(
        LEFT,
        y_from_top(FOOTER_RULE_TOP),
        RIGHT,
        y_from_top(FOOTER_RULE_TOP),
    )
    pdf.setFont(FONT_BOLD, 10)
    text_top = 784
    for line in REGISTERED_OFFICE:
        pdf.drawString(310, y_from_top(text_top), line)
        text_top += 12


def address_paragraph(row):
    address_lines = [
        xml_text(row.get(f"PARTICIPANT_ADDRESS_LINE{i}"))
        for i in range(1, 6)
    ]
    content = [f"<b>{xml_text(row['PARTICIPANT_NAME'])}</b>", ""]
    content.extend(address_lines)
    content.extend(
        [
            "",
            "<b>Supplier VAT Reg. No:"
            f"{xml_text(row['SUPPLIER_VAT_REGISTRATION_NO'])}</b>",
        ]
    )
    return Paragraph("<br/>".join(content), STYLES["party"])


def build_metadata_table(row, title):
    fields = [
        ("Invoice No", row["INVOICE_NO"]),
        ("Date", format_date(row["INVOICE_DATE"])),
        ("Your Ref", row["YOUR_REF"]),
        ("Contact", row["CONTACT"]),
        ("Accounts Payable Ref", row["ACCOUNT_PAYABLE_REF"]),
        ("Account No", row["ACCOUNT_NO"]),
        ("Payment Terms", row["PAYMENT_TERMS"]),
    ]
    details = Table(
        [
            [
                Paragraph(xml_text(label), STYLES["label"]),
                Paragraph(f": {xml_text(value)}", STYLES["value"]),
            ]
            for label, value in fields
        ],
        colWidths=[DETAILS_LABEL_WIDTH, DETAILS_VALUE_WIDTH],
    )
    details.setStyle(
        TableStyle(
            NO_PADDING
            + [("BOTTOMPADDING", (0, 0), (-1, -1), 6)]
        )
    )
    table = Table(
        [
            [
                address_paragraph(row),
                [Paragraph(xml_text(title), STYLES["title"]), details],
            ]
        ],
        colWidths=[PARTY_WIDTH, CONTENT_WIDTH - PARTY_WIDTH],
    )
    table.setStyle(
        TableStyle(
            NO_PADDING
            + [("LEFTPADDING", (1, 0), (1, 0), DETAILS_GUTTER)]
        )
    )
    return table


def make_items_table(items, include_period=False):
    rows = [
        [
            Paragraph("Description", STYLES["header"]),
            Paragraph("Net (£ value)", STYLES["header"]),
            Paragraph("VAT Rate", STYLES["header"]),
        ]
    ]
    if include_period:
        rows.append(
            [
                Paragraph(
                    "Sales Period : "
                    f"{xml_text(sales_period_text(items.iloc[0]))}",
                    STYLES["period"],
                ),
                "",
                "",
            ]
        )
    rows.extend(
        [
            Paragraph(xml_text(row["EARNING_GROUP"]), STYLES["row"]),
            Paragraph(line_amount(row["VALUE"]), STYLES["row_right"]),
            Paragraph(percent(row["VAT_RATE"]), STYLES["row_right"]),
        ]
        for _, row in items.iterrows()
    )
    table = Table(
        rows,
        colWidths=[DESCRIPTION_WIDTH, NET_WIDTH, VAT_WIDTH],
        repeatRows=1,
    )
    data_start = 2 if include_period else 1
    table.setStyle(
        TableStyle(
            NO_PADDING
            + [
                ("LINEABOVE", (0, 0), (-1, 0), 1, colors.black),
                (
                    "LINEBELOW",
                    (0, data_start - 1),
                    (-1, data_start - 1),
                    1,
                    colors.black,
                ),
                ("LEFTPADDING", (0, 0), (0, -1), 18),
                (
                    "RIGHTPADDING",
                    (1, data_start),
                    (1, -1),
                    NET_RIGHT_PADDING,
                ),
                (
                    "RIGHTPADDING",
                    (2, data_start),
                    (2, -1),
                    VAT_RIGHT_PADDING,
                ),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]
        )
    )
    return table


def make_totals_table(net, vat, total, invoice_total):
    final_label = "Invoice Total" if invoice_total else "Total"
    rows = [
        ("Total Goods &amp; Services", net, STYLES["total_label"]),
        ("Total VAT", vat, STYLES["total_label"]),
        (final_label, total, STYLES["total_label_bold"]),
    ]
    table = Table(
        [
            [
                Paragraph(label, style),
                Paragraph("GB£", STYLES["total_currency"]),
                Paragraph(line_amount(value), STYLES["total_amount"]),
            ]
            for label, value, style in rows
        ],
        colWidths=[
            TOTAL_LABEL_WIDTH,
            TOTAL_CURRENCY_WIDTH,
            TOTAL_AMOUNT_WIDTH,
        ],
    )
    table.setStyle(
        TableStyle(
            NO_PADDING
            + [
                ("TOPPADDING", (0, 0), (-1, -1), 1.5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 1.5),
                ("RIGHTPADDING", (0, 0), (0, -1), 2),
                ("LEFTPADDING", (1, 0), (1, -1), 3),
            ]
        )
    )
    return table

# COMMAND ----------

def monthly_sections(items):
    tagged = items.copy()
    tagged["_SECTION"] = tagged["EARNING_GROUP"].map(section_key)
    sections = []
    for key in ("OTHER", "MBB", "HBB"):
        chunk = (
            tagged[tagged["_SECTION"] == key]
            .drop(columns=["_SECTION"])
            .reset_index(drop=True)
        )
        if not chunk.empty:
            sections.append(chunk)
    return sections


def render_groups():
    if invoice_type == "MONTHLY":
        return monthly_sections(invoice_df)
    return [invoice_df.reset_index(drop=True)]


def draw_page_start(pdf, header, title, page_number, total_pages=None):
    draw_company_header(pdf)
    draw_footer(pdf)
    metadata = build_metadata_table(header, title)
    metadata_height = metadata.wrap(CONTENT_WIDTH, PAGE_HEIGHT)[1]
    metadata.drawOn(
        pdf,
        LEFT,
        y_from_top(META_TOP) - metadata_height,
    )
    label_top = META_TOP + metadata_height + 12
    page_label = (
        f"Page {page_number} of {total_pages}"
        if total_pages
        else f"Page {page_number}"
    )
    pdf.setFont(FONT, 10)
    # pdf.drawRightString(RIGHT, y_from_top(label_top + 8), page_label)
    pdf.drawString(LEFT, 18, page_label)
    return label_top + 14


ITEMS_LIMIT = FOOTER_RULE_TOP - 40


def measure_items(rows, include_period):
    table = make_items_table(rows, include_period)
    return table, table.wrap(CONTENT_WIDTH, PAGE_HEIGHT)[1]


def fit_rows(group, start, top, include_period, reserve):
    """
    Largest chunk of `group` from `start` that fits below `top`.

    The totals block only follows the chunk that completes the section, so the
    reserved space is applied to that chunk alone. At least one row is always
    returned so the caller keeps making progress.
    """
    end = start + 1
    table, height = measure_items(group.iloc[start:end], include_period)

    while end < len(group):
        next_end = end + 1
        candidate, candidate_height = measure_items(
            group.iloc[start:next_end], include_period
        )
        extra = reserve if next_end == len(group) else 0
        if top + candidate_height + extra > ITEMS_LIMIT:
            break
        end, table, height = next_end, candidate, candidate_height

    extra = reserve if end == len(group) else 0
    return end, table, height, top + height + extra <= ITEMS_LIMIT


def render_invoice(pdf_file, total_pages=None):
    groups = render_groups()
    header = invoice_df.iloc[0]
    invoice_totals = calc_totals(invoice_df)
    title = document_title(invoice_totals[0])
    pdf = canvas.Canvas(pdf_file, pagesize=PAGE_SIZE)
    pdf.setTitle(f"{invoice_type} Self Billed Invoice")
    pdf.setAuthor("Hutchison 3G UK Limited")

    page_number = 1
    top = draw_page_start(
        pdf, header, title, page_number, total_pages
    )
    page_top = top
    first_table = True

    def new_page():
        nonlocal page_number, top, page_top
        pdf.showPage()
        page_number += 1
        top = draw_page_start(
            pdf, header, title, page_number, total_pages
        )
        page_top = top

    for group in groups:
        show_section_totals = invoice_type == "MONTHLY"
        group_totals = calc_totals(group)
        totals_table = make_totals_table(
            *group_totals,
            invoice_total=False,
        )
        totals_height = totals_table.wrap(
            TOTALS_WIDTH, PAGE_HEIGHT
        )[1]

        reserve = totals_height + 20 if show_section_totals else 0

        start = 0
        while start < len(group):
            include_period = invoice_type == "MONTHLY" and first_table

            # Rather than splitting a section, move the whole of what is left
            # of it onto a page where it fits with its totals.
            _, whole_height = measure_items(
                group.iloc[start:], include_period
            )
            if (
                top > page_top
                and top + whole_height + reserve > ITEMS_LIMIT
                and page_top + whole_height + reserve <= ITEMS_LIMIT
            ):
                new_page()

            end, table, table_height, fits = fit_rows(
                group, start, top, include_period, reserve
            )

            # Only break the page when the chunk cannot fit here; refit against
            # the fresh page so it takes as many rows as the page allows.
            if not fits and top > page_top:
                new_page()
                end, table, table_height, _ = fit_rows(
                    group, start, top, include_period, reserve
                )

            table.drawOn(
                pdf,
                LEFT,
                y_from_top(top) - table_height,
            )
            top += table_height
            first_table = False
            start = end

        if show_section_totals:
            if top + totals_height + 18 > FOOTER_RULE_TOP - 40:
                new_page()
            pdf.setDash(1, 2)
            pdf.line(
                TOTALS_LEFT,
                y_from_top(top),
                VAT_RIGHT_EDGE,
                y_from_top(top),
            )
            pdf.setDash()
            top += 2
            totals_table.drawOn(
                pdf,
                TOTALS_LEFT,
                y_from_top(top) - totals_height,
            )
            top += totals_height
            pdf.line(
                LEFT,
                y_from_top(top),
                RIGHT,
                y_from_top(top),
            )
            top += 10

    grand_table = make_totals_table(
        *invoice_totals,
        invoice_total=True,
    )
    grand_height = grand_table.wrap(TOTALS_WIDTH, PAGE_HEIGHT)[1]
    if top + grand_height + 35 > FOOTER_RULE_TOP - 20:
        new_page()

    pdf.line(LEFT, y_from_top(top), RIGHT, y_from_top(top))
    top += 7
    grand_table.drawOn(
        pdf,
        TOTALS_LEFT,
        y_from_top(top) - grand_height,
    )
    note = Paragraph(VAT_NOTE, STYLES["note"])
    note_height = note.wrap(NOTE_WIDTH, PAGE_HEIGHT)[1]
    note.drawOn(
        pdf,
        NOTE_LEFT,
        y_from_top(top + 14) - note_height,
    )
    top += grand_height + 3
    pdf.line(LEFT, y_from_top(top), RIGHT, y_from_top(top))
    pdf.save()
    return page_number


invoice_pages = render_invoice(invoice_pdf_path)
render_invoice(invoice_pdf_path, total_pages=invoice_pages)

if (
    not os.path.isfile(invoice_pdf_path)
    or os.path.getsize(invoice_pdf_path) == 0
):
    raise RuntimeError(
        f"Invoice PDF was not created: {invoice_pdf_path}"
    )

if not os.path.isfile(disclaimer_path):
    raise FileNotFoundError(
        f"Ready-made disclaimer PDF not found: {disclaimer_path}"
    )

os.makedirs(os.path.dirname(individual_pdf_path), exist_ok=True)
merger = PdfMerger()
try:
    merger.append(invoice_pdf_path)
    merger.append(disclaimer_path)
    with open(individual_pdf_path, "wb") as output_file:
        merger.write(output_file)
finally:
    merger.close()

if (
    not os.path.isfile(individual_pdf_path)
    or os.path.getsize(individual_pdf_path) == 0
):
    raise RuntimeError(
        f"Merged PDF was not created: {individual_pdf_path}"
    )

print("Invoice PDF created successfully")
print("Invoice type       :", invoice_type)
print("Invoice-only file  :", invoice_pdf_path)
print("Merged individual  :", individual_pdf_path)
print("Invoice pages      :", invoice_pages)
print("Rows               :", len(invoice_df))

dbutils.notebook.exit("Success")