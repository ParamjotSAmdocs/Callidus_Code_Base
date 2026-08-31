# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "/Workspace/Repos/Aldm Repository/ALDM/databricks/config/config"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sequence Generator Function

# COMMAND ----------

def file_seq_generator(lv_ext_filename,month_str):
    from pathlib import Path
    import datetime

    ext_filename_pttrn = lv_ext_filename
    lv_ext_filename_base = Path(lv_ext_filename).stem
    lv_ext_file_ext = Path(lv_ext_filename).suffix
    if not month_str:
        now = datetime.datetime.now()
        month_str = (now.replace(day=1) - datetime.timedelta(days=1)).strftime("%m%Y")
    lv_ext_filename_base = lv_ext_filename_base.replace("MMYYYY", month_str)

    # Query for current sequence number for this file/month
    select_seq_sql = f"""
    SELECT SEQ_NUM FROM ALDM_OPER.EXTRACT_SEQ_TRACK
    WHERE EXTRACT_NAME = '{lv_ext_filename}'
        AND EXTRACT_MONTH = '{month_str}'
    """
    df = spark.sql(select_seq_sql)
    row = df.first()
    if row:
        # If exists, increment sequence and update table
        seq_num = row[0] + 1
        update_seq_sql = f"""
        UPDATE ALDM_OPER.EXTRACT_SEQ_TRACK
        SET SEQ_NUM = {seq_num}
        WHERE EXTRACT_NAME = '{lv_ext_filename}'
            AND EXTRACT_MONTH = '{month_str}'
        """
        spark.sql(update_seq_sql)
    else:
        # If not exists, insert new row with seq_num = 1
        seq_num = 1
        insert_seq_sql = f"""
        INSERT INTO ALDM_OPER.EXTRACT_SEQ_TRACK (EXTRACT_NAME, EXTRACT_MONTH, SEQ_NUM)
        VALUES ('{lv_ext_filename}', '{month_str}', {seq_num})
        """
        spark.sql(insert_seq_sql)

    # Format sequence as 4-digit string and build final filename
    seq_str = f"{seq_num:04d}"
    lv_ext_filename_base = lv_ext_filename_base.replace('<SEQ>', seq_str)
    lv_ext_filename_final = f"{lv_ext_filename_base}{lv_ext_file_ext}"
    # print(lv_ext_filename)
    # print(lv_ext_filename_final, seq_str)
    return (lv_ext_filename_final,ext_filename_pttrn,month_str,seq_str)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge the PDFs, add the disclaimer, and archive the final PDF at the MFT end, ensuring that the page numbers are resequenced correctly.

# COMMAND ----------

# MAGIC %pip install PyPDF2

# COMMAND ----------

import os
import re
import shutil
import tempfile

from PyPDF2 import PdfMerger, PdfReader, PdfWriter
from PyPDF2.generic import (
    ArrayObject,
    ByteStringObject,
    ContentStream,
    DecodedStreamObject,
    DictionaryObject,
    NameObject,
    TextStringObject,
)


# Matches the labels the source invoices draw themselves, for example
# "Page 1", "Page 1 of 1" or "Page 2 of 3".
PAGE_NUMBER_PATTERN = re.compile(
    r"^\s*Page\s+\d+(?:\s+of\s+\d+)?\s*$",
    re.IGNORECASE,
)

# Run types whose combined PDF ends with the ready-made disclaimer page.
DISCLAIMER_FILE_TYPES = {"MONTHLY", "MIDMONTHLY"}


def needs_disclaimer(file_type):
    """True for MONTHLY and MID_MONTHLY, whichever spelling is passed in."""
    normalised = (
        str(file_type)
        .upper()
        .replace("_", "")
        .replace("-", "")
    )
    return normalised in DISCLAIMER_FILE_TYPES


def decode_pdf_text(value):
    if isinstance(value, TextStringObject):
        return str(value)

    if isinstance(value, ByteStringObject):
        return bytes(value).decode("latin-1", errors="ignore")

    if isinstance(value, bytes):
        return value.decode("latin-1", errors="ignore")

    return ""


def get_operation_text(operands, operator):
    """Get text drawn by a PDF text operation."""

    if operator == b"Tj" and operands:
        return decode_pdf_text(operands[0])

    if operator == b"TJ" and operands:
        return "".join(
            decode_pdf_text(item)
            for item in operands[0]
            if isinstance(
                item,
                (TextStringObject, ByteStringObject, bytes),
            )
        )

    if operator == b"'" and operands:
        return decode_pdf_text(operands[0])

    if operator == b'"' and len(operands) >= 3:
        return decode_pdf_text(operands[2])

    return ""


def remove_existing_page_number(page, reader):
    """Remove Page X and Page X of Y text from one page."""

    contents = page.get_contents()

    if contents is None:
        return page, False

    content_stream = ContentStream(contents, reader)
    filtered_operations = []
    removed = False

    for operands, operator in content_stream.operations:
        text = get_operation_text(
            operands,
            operator,
        ).strip()

        # Drop the operation that draws the old label; keep everything else.
        if (
            text
            and PAGE_NUMBER_PATTERN.fullmatch(text)
        ):
            removed = True
            continue

        filtered_operations.append((operands, operator))

    content_stream.operations = filtered_operations
    page[NameObject("/Contents")] = content_stream

    return page, removed


def create_page_number_font(writer):
    """Create a standard Helvetica font for page numbers."""

    font = DictionaryObject()
    font.update(
        {
            NameObject("/Type"): NameObject("/Font"),
            NameObject("/Subtype"): NameObject("/Type1"),
            NameObject("/BaseFont"): NameObject("/Helvetica"),
            NameObject("/Encoding"): NameObject("/WinAnsiEncoding"),
        }
    )

    return writer._add_object(font)


def add_font_to_page(page, font_reference):
    """Register the page-number font in a page's resources."""

    resources_reference = page.get("/Resources")

    if resources_reference is None:
        resources = DictionaryObject()
        page[NameObject("/Resources")] = resources
    else:
        resources = resources_reference.get_object()

    fonts_reference = resources.get("/Font")

    if fonts_reference is None:
        fonts = DictionaryObject()
        resources[NameObject("/Font")] = fonts
    else:
        fonts = fonts_reference.get_object()

    fonts[NameObject("/FPageNumber")] = font_reference


def append_page_number_stream(
    page,
    writer,
    page_number,
    total_numbered_pages,
):
    """Add a new combined-document page number."""

    add_font_to_page(
        page,
        writer._page_number_font_reference,
    )

    label = f"Page {page_number} of {total_numbered_pages}"

    # Escape PDF string characters.
    safe_label = (
        label
        .replace("\\", "\\\\")
        .replace("(", "\\(")
        .replace(")", "\\)")
    )

    # Place the new number at bottom-left.
    stream_data = (
        "q\n"
        "BT\n"
        "/FPageNumber 10 Tf\n"
        "0 g\n"
        "18 18 Td\n"
        f"({safe_label}) Tj\n"
        "ET\n"
        "Q\n"
    ).encode("latin-1")

    number_stream = DecodedStreamObject()
    number_stream.set_data(stream_data)
    number_stream_reference = writer._add_object(number_stream)

    existing_contents = page.get("/Contents")

    if existing_contents is None:
        page[NameObject("/Contents")] = number_stream_reference

    elif isinstance(existing_contents, ArrayObject):
        existing_contents.append(number_stream_reference)

    else:
        page[NameObject("/Contents")] = ArrayObject(
            [
                existing_contents,
                number_stream_reference,
            ]
        )


def renumber_final_pdf(
    source_pdf,
    destination_pdf,
):
    """
    Remove old invoice numbering and add one combined sequence.

    The final page is assumed to be DISCLAIMER_1.pdf and is not numbered.
    """

    with open(source_pdf, "rb") as source_file:
        reader = PdfReader(source_file)
        writer = PdfWriter()

        total_pages = len(reader.pages)

        if total_pages < 2:
            raise ValueError(
                "The combined PDF must contain at least one "
                "invoice page and one disclaimer page."
            )

        # Step 7: the last page is the disclaimer, so it is left out of
        # both the numbering and the total.
        total_numbered_pages = total_pages - 1

        writer._page_number_font_reference = (
            create_page_number_font(writer)
        )

        removed_count = 0
        added_count = 0

        for page_index, source_page in enumerate(
            reader.pages,
            start=1,
        ):
            # Step 5: drop the label the invoice drew for itself.
            cleaned_page, removed = (
                remove_existing_page_number(
                    source_page,
                    reader,
                )
            )

            if removed:
                removed_count += 1

            writer.add_page(cleaned_page)
            output_page = writer.pages[-1]

            # Step 6: draw the combined sequence, skipping the disclaimer.
            if page_index <= total_numbered_pages:
                append_page_number_stream(
                    output_page,
                    writer,
                    page_index,
                    total_numbered_pages,
                )
                added_count += 1

        if reader.metadata:
            writer.add_metadata(
                {
                    str(key): str(value)
                    for key, value in reader.metadata.items()
                    if value is not None
                }
            )

        with open(destination_pdf, "wb") as output_file:
            writer.write(output_file)

    return removed_count, added_count, total_numbered_pages


def merge_invoice_pdfs(
    input_dir,
    pdf_files,
    disclaimer_pdf,
    output_pdf,
):
    """
    Merge the invoices into one PDF.

    disclaimer_pdf is appended as the final page when a path is given, and
    ignored when it is None.
    """

    merger = PdfMerger()

    try:
        for filename in pdf_files:
            pdf_path = os.path.join(
                input_dir,
                filename,
            )
            merger.append(pdf_path)
            print(f"Added invoice: {filename}")

        if disclaimer_pdf:
            if not os.path.isfile(disclaimer_pdf):
                raise FileNotFoundError(
                    f"Disclaimer PDF not found: {disclaimer_pdf}"
                )

            merger.append(disclaimer_pdf)
            print("Added disclaimer: DISCLAIMER_1.pdf")

        with open(output_pdf, "wb") as output_file:
            merger.write(output_file)

    finally:
        merger.close()


def archive_invoice_pdfs(
    input_dir,
    archive_dir,
    pdf_files,
):
    """Move successfully processed invoices to archive."""

    for filename in pdf_files:
        source_path = os.path.join(
            input_dir,
            filename,
        )
        archive_path = os.path.join(
            archive_dir,
            filename,
        )

        if os.path.exists(archive_path):
            os.remove(archive_path)

        shutil.move(source_path, archive_path)
        print(f"Archived: {filename}")


def merge_and_archive_pdfs_mft(
    input_directory,
    output_directory,
    output_filename,
    file_type,
):
    """
    Build the combined PDF for one run and archive its source invoices.

    MONTHLY and MID_MONTHLY runs append DISCLAIMER_1.pdf and are renumbered
    as one document. Every other run type is only merged and archived, so
    its pages keep whatever numbering the source PDFs already had.
    """

    input_dir = (
        f"{mft_out}/aldm/outbound_1/{input_directory}"
    )
    output_dir = (
        f"{mft_out}/aldm/outbound_1/{output_directory}"
    )
    output_pdf = os.path.join(
        output_dir,
        output_filename,
    )
    disclaimer_pdf = os.path.join(
        input_dir,
        "DISCLAIMER",
        "DISCLAIMER_1.pdf",
    )
    archive_dir = os.path.join(
        input_dir,
        "archive",
    )

    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)

    # Only MONTHLY and MID_MONTHLY get the disclaimer and the renumbering.
    is_invoice_run = needs_disclaimer(file_type)

    # Merge all PDF files directly under input_dir.
    # Hidden temporary files and the output file are excluded.
    pdf_files = sorted(
        filename
        for filename in os.listdir(input_dir)
        if filename.lower().endswith(".pdf")
        and not filename.startswith(".")
        and filename != output_filename
    )

    if not pdf_files:
        print("No invoice PDF files found.")
        return

    temporary_numbered_pdf = None

    try:
        # Steps 1-3: merge the invoices, append DISCLAIMER_1.pdf for the
        # invoice run types, and save the initial combined PDF.
        merge_invoice_pdfs(
            input_dir,
            pdf_files,
            disclaimer_pdf if is_invoice_run else None,
            output_pdf,
        )

        print(
            f"Initial combined PDF created: {output_pdf}"
        )

        if is_invoice_run:
            # Step 4: reopen the combined PDF and build a renumbered copy
            # beside it. Steps 5-7 happen inside renumber_final_pdf.
            file_descriptor, temporary_numbered_pdf = (
                tempfile.mkstemp(
                    prefix="renumbered_",
                    suffix=".pdf",
                    dir=output_dir,
                )
            )
            os.close(file_descriptor)

            removed, added, numbered_pages = (
                renumber_final_pdf(
                    output_pdf,
                    temporary_numbered_pdf,
                )
            )

            # Step 8: overwrite the combined PDF with the renumbered copy.
            os.replace(
                temporary_numbered_pdf,
                output_pdf,
            )
            temporary_numbered_pdf = None

            print(f"Final combined PDF: {output_pdf}")
            print(f"Old page labels removed: {removed}")
            print(f"New page labels added: {added}")
            print(f"Numbered invoice pages: {numbered_pages}")
            print("Disclaimer page was not numbered.")
        else:
            # Other run types keep the merged file exactly as it is, so
            # steps 4 to 8 are skipped entirely.
            print(
                f"{file_type} run: no disclaimer and no renumbering."
            )

        # Step 9: archive inputs only after the output file is final.
        archive_invoice_pdfs(
            input_dir,
            archive_dir,
            pdf_files,
        )

        print(
            "Merge and archive completed successfully."
        )

    except Exception as exc:
        raise RuntimeError(
            f"PDF processing failed: {exc}"
        ) from exc

    finally:
        if (
            temporary_numbered_pdf
            and os.path.exists(temporary_numbered_pdf)
        ):
            os.remove(temporary_numbered_pdf)

# COMMAND ----------

# import os
# import re
# import shutil
# import tempfile

# from PyPDF2 import PdfMerger, PdfReader, PdfWriter
# from PyPDF2.generic import (
#     ArrayObject,
#     ByteStringObject,
#     ContentStream,
#     DecodedStreamObject,
#     DictionaryObject,
#     NameObject,
#     NumberObject,
#     TextStringObject,
# )


# PAGE_NUMBER_PATTERN = re.compile(
#     r"^\s*Page\s+\d+(?:\s+of\s+\d+)?\s*$",
#     re.IGNORECASE,
# )


# def decode_pdf_text(value):
#     if isinstance(value, TextStringObject):
#         return str(value)

#     if isinstance(value, ByteStringObject):
#         return bytes(value).decode("latin-1", errors="ignore")

#     if isinstance(value, bytes):
#         return value.decode("latin-1", errors="ignore")

#     return ""


# def get_operation_text(operands, operator):
#     """Get text drawn by a PDF text operation."""

#     if operator == b"Tj" and operands:
#         return decode_pdf_text(operands[0])

#     if operator == b"TJ" and operands:
#         return "".join(
#             decode_pdf_text(item)
#             for item in operands[0]
#             if isinstance(
#                 item,
#                 (TextStringObject, ByteStringObject, bytes),
#             )
#         )

#     if operator == b"'" and operands:
#         return decode_pdf_text(operands[0])

#     if operator == b'"' and len(operands) >= 3:
#         return decode_pdf_text(operands[2])

#     return ""


# def remove_existing_page_number(page, reader):
#     """Remove Page X and Page X of Y text from one page."""

#     contents = page.get_contents()

#     if contents is None:
#         return page, False

#     content_stream = ContentStream(contents, reader)
#     filtered_operations = []
#     removed = False

#     for operands, operator in content_stream.operations:
#         text = get_operation_text(
#             operands,
#             operator,
#         ).strip()

#         if (
#             text
#             and PAGE_NUMBER_PATTERN.fullmatch(text)
#         ):
#             removed = True
#             continue

#         filtered_operations.append((operands, operator))

#     content_stream.operations = filtered_operations
#     page[NameObject("/Contents")] = content_stream

#     return page, removed


# def create_page_number_font(writer):
#     """Create a standard Helvetica font for page numbers."""

#     font = DictionaryObject()
#     font.update(
#         {
#             NameObject("/Type"): NameObject("/Font"),
#             NameObject("/Subtype"): NameObject("/Type1"),
#             NameObject("/BaseFont"): NameObject("/Helvetica"),
#             NameObject("/Encoding"): NameObject("/WinAnsiEncoding"),
#         }
#     )

#     return writer._add_object(font)


# def add_font_to_page(page, font_reference):
#     """Register the page-number font in a page's resources."""

#     resources_reference = page.get("/Resources")

#     if resources_reference is None:
#         resources = DictionaryObject()
#         page[NameObject("/Resources")] = resources
#     else:
#         resources = resources_reference.get_object()

#     fonts_reference = resources.get("/Font")

#     if fonts_reference is None:
#         fonts = DictionaryObject()
#         resources[NameObject("/Font")] = fonts
#     else:
#         fonts = fonts_reference.get_object()

#     fonts[NameObject("/FPageNumber")] = font_reference


# def append_page_number_stream(
#     page,
#     writer,
#     page_number,
#     total_numbered_pages,
# ):
#     """Add a new combined-document page number."""

#     add_font_to_page(
#         page,
#         writer._page_number_font_reference,
#     )

#     label = f"Page {page_number} of {total_numbered_pages}"

#     # Escape PDF string characters.
#     safe_label = (
#         label
#         .replace("\\", "\\\\")
#         .replace("(", "\\(")
#         .replace(")", "\\)")
#     )

#     # Place the new number at bottom-left.
#     stream_data = (
#         "q\n"
#         "BT\n"
#         "/FPageNumber 10 Tf\n"
#         "0 g\n"
#         "18 18 Td\n"
#         f"({safe_label}) Tj\n"
#         "ET\n"
#         "Q\n"
#     ).encode("latin-1")

#     number_stream = DecodedStreamObject()
#     number_stream.set_data(stream_data)
#     number_stream_reference = writer._add_object(number_stream)

#     existing_contents = page.get("/Contents")

#     if existing_contents is None:
#         page[NameObject("/Contents")] = number_stream_reference

#     elif isinstance(existing_contents, ArrayObject):
#         existing_contents.append(number_stream_reference)

#     else:
#         page[NameObject("/Contents")] = ArrayObject(
#             [
#                 existing_contents,
#                 number_stream_reference,
#             ]
#         )


# def renumber_final_pdf(
#     source_pdf,
#     destination_pdf,
# ):
#     """
#     Remove old invoice numbering and add one combined sequence.

#     The final page is assumed to be DISCLAIMER_1.pdf and is not numbered.
#     """

#     with open(source_pdf, "rb") as source_file:
#         reader = PdfReader(source_file)
#         writer = PdfWriter()

#         total_pages = len(reader.pages)

#         if total_pages < 2:
#             raise ValueError(
#                 "The combined PDF must contain at least one "
#                 "invoice page and one disclaimer page."
#             )

#         # Last page is the disclaimer and is excluded.
#         total_numbered_pages = total_pages - 1

#         writer._page_number_font_reference = (
#             create_page_number_font(writer)
#         )

#         removed_count = 0
#         added_count = 0

#         for page_index, source_page in enumerate(
#             reader.pages,
#             start=1,
#         ):
#             cleaned_page, removed = (
#                 remove_existing_page_number(
#                     source_page,
#                     reader,
#                 )
#             )

#             if removed:
#                 removed_count += 1

#             writer.add_page(cleaned_page)
#             output_page = writer.pages[-1]

#             # Do not number the final disclaimer page.
#             if page_index <= total_numbered_pages:
#                 append_page_number_stream(
#                     output_page,
#                     writer,
#                     page_index,
#                     total_numbered_pages,
#                 )
#                 added_count += 1

#         if reader.metadata:
#             writer.add_metadata(
#                 {
#                     str(key): str(value)
#                     for key, value in reader.metadata.items()
#                     if value is not None
#                 }
#             )

#         with open(destination_pdf, "wb") as output_file:
#             writer.write(output_file)

#     return removed_count, added_count, total_numbered_pages


# def merge_invoice_pdfs(
#     input_dir,
#     pdf_files,
#     disclaimer_pdf,
#     output_pdf,
# ):
#     """Merge invoices and append the disclaimer."""

#     merger = PdfMerger()

#     try:
#         for filename in pdf_files:
#             pdf_path = os.path.join(
#                 input_dir,
#                 filename,
#             )
#             merger.append(pdf_path)
#             print(f"Added invoice: {filename}")

#         if not os.path.isfile(disclaimer_pdf):
#             raise FileNotFoundError(
#                 f"Disclaimer PDF not found: {disclaimer_pdf}"
#             )

#         merger.append(disclaimer_pdf)
#         print("Added disclaimer: DISCLAIMER_1.pdf")

#         with open(output_pdf, "wb") as output_file:
#             merger.write(output_file)

#     finally:
#         merger.close()


# def archive_invoice_pdfs(
#     input_dir,
#     archive_dir,
#     pdf_files,
# ):
#     """Move successfully processed invoices to archive."""

#     for filename in pdf_files:
#         source_path = os.path.join(
#             input_dir,
#             filename,
#         )
#         archive_path = os.path.join(
#             archive_dir,
#             filename,
#         )

#         if os.path.exists(archive_path):
#             os.remove(archive_path)

#         shutil.move(source_path, archive_path)
#         print(f"Archived: {filename}")


# def merge_and_archive_pdfs_mft(
#     input_directory,
#     output_directory,
#     output_filename,
#     file_type,
# ):
#     input_dir = (
#         f"{mft_out}/aldm/outbound_1/{input_directory}"
#     )
#     output_dir = (
#         f"{mft_out}/aldm/outbound_1/{output_directory}"
#     )
#     output_pdf = os.path.join(
#         output_dir,
#         output_filename,
#     )
#     disclaimer_pdf = os.path.join(
#         input_dir,
#         "DISCLAIMER",
#         "DISCLAIMER_1.pdf",
#     )
#     archive_dir = os.path.join(
#         input_dir,
#         "archive",
#     )

#     os.makedirs(input_dir, exist_ok=True)
#     os.makedirs(output_dir, exist_ok=True)
#     os.makedirs(archive_dir, exist_ok=True)

#     # Merge all PDF files directly under input_dir.
#     # Hidden temporary files and the output file are excluded.
#     pdf_files = sorted(
#         filename
#         for filename in os.listdir(input_dir)
#         if filename.lower().endswith(".pdf")
#         and not filename.startswith(".")
#         and filename != output_filename
#     )

#     if not pdf_files:
#         print("No invoice PDF files found.")
#         return

#     temporary_numbered_pdf = None

#     try:
#         # Step 1: Create the original combined PDF at output_dir.
#         merge_invoice_pdfs(
#             input_dir,
#             pdf_files,
#             disclaimer_pdf,
#             output_pdf,
#         )

#         print(
#             f"Initial combined PDF created: {output_pdf}"
#         )

#         # Step 2: Create a cleaned and renumbered temporary copy.
#         file_descriptor, temporary_numbered_pdf = (
#             tempfile.mkstemp(
#                 prefix="renumbered_",
#                 suffix=".pdf",
#                 dir=output_dir,
#             )
#         )
#         os.close(file_descriptor)

#         removed, added, numbered_pages = (
#             renumber_final_pdf(
#                 output_pdf,
#                 temporary_numbered_pdf,
#             )
#         )

#         # Step 3: Overwrite the initial combined PDF.
#         os.replace(
#             temporary_numbered_pdf,
#             output_pdf,
#         )
#         temporary_numbered_pdf = None

#         print(f"Final combined PDF: {output_pdf}")
#         print(f"Old page labels removed: {removed}")
#         print(f"New page labels added: {added}")
#         print(f"Numbered invoice pages: {numbered_pages}")
#         print("Disclaimer page was not numbered.")

#         # Step 4: Archive inputs only after successful renumbering.
#         archive_invoice_pdfs(
#             input_dir,
#             archive_dir,
#             pdf_files,
#         )

#         print(
#             "Merge, renumbering and archive completed successfully."
#         )

#     except Exception as exc:
#         raise RuntimeError(
#             f"PDF processing failed: {exc}"
#         ) from exc

#     finally:
#         if (
#             temporary_numbered_pdf
#             and os.path.exists(temporary_numbered_pdf)
#         ):
#             os.remove(temporary_numbered_pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### archive and move the files from MFT to FS

# COMMAND ----------

# MAGIC %pip install --upgrade azure-storage-file-share

# COMMAND ----------

import importlib
importlib.invalidate_caches()
from azure.storage.fileshare import ShareFileClient, ShareDirectoryClient

# COMMAND ----------

def archive_files_at_azure_share(fs_conn_str, fs_name, fs_output_dir, file_pattern):
    """
    Move files starting with file_pattern at Azure File Share output directory to an 'archive' folder.
    Args:
        fs_conn_str (str): Azure File Share connection string
        fs_name (str): Azure File Share name
        fs_output_dir (str): Azure File Share output directory path
        file_pattern (str): Pattern to match files (should be prefix)
    """
    from azure.storage.fileshare import ShareFileClient, ShareDirectoryClient

    archive_dir = fs_output_dir.rstrip('/') + '/archive'
    dir_client = ShareDirectoryClient.from_connection_string(
        conn_str=fs_conn_str, share_name=fs_name, directory_path=fs_output_dir.strip('/')
    )
    archive_client = ShareDirectoryClient.from_connection_string(
        conn_str=fs_conn_str, share_name=fs_name, directory_path=archive_dir.strip('/')
    )
    try:
        archive_client.create_directory()  # Ensure archive directory exists
    except Exception as e:
        if "ResourceAlreadyExists" not in str(e):
            raise

    file_list = dir_client.list_directories_and_files()
    for item in file_list:
        if item['is_directory']:
            continue
        filename = item['name']
        if filename.startswith(file_pattern):
            src_file_path = fs_output_dir.rstrip('/') + '/' + filename
            dst_file_path = archive_dir.rstrip('/') + '/' + filename
            src_file_client = ShareFileClient.from_connection_string(
                conn_str=fs_conn_str, share_name=fs_name, file_path=src_file_path
            )
            dst_file_client = ShareFileClient.from_connection_string(
                conn_str=fs_conn_str, share_name=fs_name, file_path=dst_file_path
            )
            try:
                dst_file_client.get_file_properties()
                dst_file_client.delete_file()
            except Exception:
                pass
            try:
                file_content = src_file_client.download_file().readall()
                dst_file_client.upload_file(file_content)
                src_file_client.delete_file()
            except Exception as e:
                raise RuntimeError(f"Failed to archive file '{filename}': {e}")

def move_files_to_azure_share(fs_conn_str, fs_name, mft_input_dir, fs_output_dir, file_pattern):
    """
    Move files starting with file_pattern from MFT input directory to Azure File Share output directory,
    then remove those files from the input directory.
    Args:
        fs_conn_str (str): Azure File Share connection string
        fs_name (str): Azure File Share name
        mft_input_dir (str): Local input directory (MFT)
        fs_output_dir (str): Azure File Share output directory path
        file_pattern (str): Pattern to match files (should be prefix)
    """
    from azure.storage.fileshare import ShareFileClient
    import os

    if not os.path.isdir(mft_input_dir):
        raise FileNotFoundError(f"Input directory '{mft_input_dir}' does not exist.")

    files_to_move = [f for f in os.listdir(mft_input_dir) if f.startswith(file_pattern)]
    if not files_to_move:
        raise FileNotFoundError(f"No files starting with pattern '{file_pattern}' found in '{mft_input_dir}'.")

    for filename in files_to_move:
        local_path = os.path.join(mft_input_dir, filename)
        remote_path = fs_output_dir.rstrip('/') + '/' + filename

        share_client = ShareFileClient.from_connection_string(
            conn_str=fs_conn_str, share_name=fs_name, file_path=remote_path
        )
        try:
            share_client.get_file_properties()
            share_client.delete_file()
        except Exception:
            pass
        with open(local_path, "rb") as source_file:
            try:
                share_client.upload_file(source_file)
            except Exception as e:
                if "ResourceAlreadyExists" in str(e):
                    share_client.delete_file()
                    with open(local_path, "rb") as retry_file:
                        share_client.upload_file(retry_file)
                else:
                    raise
        os.remove(local_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update monthly and Mid Monthly latest seq in ALDM_OPER.EXTRACT_SEQ_TRACK

# COMMAND ----------

def upd_Invoice_type_latest_trigger(invoice_type, seq_str, month_str):
    check_sql = f"""
        SELECT SEQ_NUM FROM ALDM_OPER.EXTRACT_SEQ_TRACK
        WHERE EXTRACT_NAME = '{invoice_type}'
          AND EXTRACT_MONTH = '{month_str}'
    """
    df = spark.sql(check_sql)
    row = df.first()
    if row is None:
        insert_sql = f"""
            INSERT INTO ALDM_OPER.EXTRACT_SEQ_TRACK (EXTRACT_NAME, EXTRACT_MONTH, SEQ_NUM)
            VALUES ('{invoice_type}', '{month_str}', {seq_str})
        """
        spark.sql(insert_sql)
    else:
        update_sql = f"""
            UPDATE ALDM_OPER.EXTRACT_SEQ_TRACK
            SET SEQ_NUM = {seq_str}
            WHERE EXTRACT_NAME = '{invoice_type}'
              AND EXTRACT_MONTH = '{month_str}'
        """
        spark.sql(update_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove the old files if present at MFT location

# COMMAND ----------

import os

def remove_files_by_extension(mft_location, file_extension):
    """
    Remove all files with the given file_extension at the specified mft_location directory.
    Args:
        mft_location (str): Directory path to search for files.
        file_extension (str): File extension to remove (e.g., '.pdf').
    """
    if not os.path.isdir(mft_location):
        raise FileNotFoundError(f"Directory '{mft_location}' does not exist.")
    files_to_remove = [f for f in os.listdir(mft_location) if f.lower().endswith(file_extension.lower())]
    for filename in files_to_remove:
        file_path = os.path.join(mft_location, filename)
        try:
            os.remove(file_path)
        except Exception as e:
            print(f"Failed to remove {filename}: {e}")