# Databricks notebook source
# MAGIC %md
# MAGIC Shared Callidus helpers. Do **not** put `%pip install` in this notebook.
# MAGIC `%run` executes every cell, so pip here starts cluster-scoped library
# MAGIC install and the caller then fails on `spark.sql` with:
# MAGIC `Library installation has not been finished`.
# MAGIC
# MAGIC Install packages on the compute, in the notebook Environment panel, or
# MAGIC in the **caller** notebook as the first cell, then `restartPython()`.
# MAGIC
# MAGIC PyPDF2 is imported optionally so CSV extract notebooks that never touch
# MAGIC PDFs can `%run` this notebook without installing it.

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Aldm Repository/ALDM/databricks/config/config"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark SQL helper (serverless / Spark Connect)

# COMMAND ----------

def _run_spark_sql(sql_text, spark_session=None):
    """Run spark.sql after cluster-scoped libraries finish installing."""
    import time

    session = spark_session or spark
    last_error = None
    for attempt in range(1, 13):
        try:
            return session.sql(sql_text)
        except Exception as exc:
            message = str(exc)
            if "Library installation has not been finished" not in message:
                raise
            print(
                f"Spark libraries still installing "
                f"(attempt {attempt}/12). Waiting 15 seconds..."
            )
            time.sleep(15)
            last_error = exc
    raise last_error

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sequence Generator Function

# COMMAND ----------

def file_seq_generator(lv_ext_filename, month_str=None):
    from pathlib import Path
    import datetime

    ext_filename_pttrn = lv_ext_filename
    lv_ext_filename_base = Path(lv_ext_filename).stem
    lv_ext_file_ext = Path(lv_ext_filename).suffix
    if not month_str:
        now = datetime.datetime.now()
        month_str = (now.replace(day=1) - datetime.timedelta(days=1)).strftime("%m%Y")
    lv_ext_filename_base = lv_ext_filename_base.replace("MMYYYY", month_str)

    select_seq_sql = f"""
    SELECT SEQ_NUM FROM ALDM_OPER.EXTRACT_SEQ_TRACK
    WHERE EXTRACT_NAME = '{lv_ext_filename}'
        AND EXTRACT_MONTH = '{month_str}'
    """
    df = _run_spark_sql(select_seq_sql)
    row = df.first()
    if row:
        seq_num = row[0] + 1
        update_seq_sql = f"""
        UPDATE ALDM_OPER.EXTRACT_SEQ_TRACK
        SET SEQ_NUM = {seq_num}
        WHERE EXTRACT_NAME = '{lv_ext_filename}'
            AND EXTRACT_MONTH = '{month_str}'
        """
        _run_spark_sql(update_seq_sql)
    else:
        seq_num = 1
        insert_seq_sql = f"""
        INSERT INTO ALDM_OPER.EXTRACT_SEQ_TRACK (EXTRACT_NAME, EXTRACT_MONTH, SEQ_NUM)
        VALUES ('{lv_ext_filename}', '{month_str}', {seq_num})
        """
        _run_spark_sql(insert_seq_sql)

    seq_str = f"{seq_num:04d}"
    lv_ext_filename_base = lv_ext_filename_base.replace("<SEQ>", seq_str)
    lv_ext_filename_final = f"{lv_ext_filename_base}{lv_ext_file_ext}"
    return (lv_ext_filename_final, ext_filename_pttrn, month_str, seq_str)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge the PDFs, add the disclaimer, and archive the final PDF

# COMMAND ----------

import os
import re
import shutil
import tempfile

try:
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
except ImportError:
    PdfMerger = PdfReader = PdfWriter = None
    ArrayObject = ByteStringObject = ContentStream = None
    DecodedStreamObject = DictionaryObject = NameObject = TextStringObject = None


PAGE_NUMBER_PATTERN = re.compile(
    r"^\s*Page\s+\d+(?:\s+of\s+\d+)?\s*$",
    re.IGNORECASE,
)

DISCLAIMER_FILE_TYPES = {"MONTHLY", "MIDMONTHLY"}


def needs_disclaimer(file_type):
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
    add_font_to_page(
        page,
        writer._page_number_font_reference,
    )

    label = f"Page {page_number} of {total_numbered_pages}"

    safe_label = (
        label
        .replace("\\", "\\\\")
        .replace("(", "\\(")
        .replace(")", "\\)")
    )

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
    with open(source_pdf, "rb") as source_file:
        reader = PdfReader(source_file)
        writer = PdfWriter()

        total_pages = len(reader.pages)

        if total_pages < 2:
            raise ValueError(
                "The combined PDF must contain at least one "
                "invoice page and one disclaimer page."
            )

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
    if PdfMerger is None:
        raise ImportError(
            "PyPDF2 is required for PDF merging. Install it in the caller "
            "notebook's first cell or on the compute."
        )

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

    is_invoice_run = needs_disclaimer(file_type)

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
            print(
                f"{file_type} run: no disclaimer and no renumbering."
            )

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

# MAGIC %md
# MAGIC ### Archive and move files from MFT to Azure File Share

# COMMAND ----------

def archive_files_at_azure_share(fs_conn_str, fs_name, fs_output_dir, file_pattern):
    from azure.storage.fileshare import ShareFileClient, ShareDirectoryClient

    archive_dir = fs_output_dir.rstrip("/") + "/archive"
    dir_client = ShareDirectoryClient.from_connection_string(
        conn_str=fs_conn_str, share_name=fs_name, directory_path=fs_output_dir.strip("/")
    )
    archive_client = ShareDirectoryClient.from_connection_string(
        conn_str=fs_conn_str, share_name=fs_name, directory_path=archive_dir.strip("/")
    )
    try:
        archive_client.create_directory()
    except Exception as e:
        if "ResourceAlreadyExists" not in str(e):
            raise

    file_list = dir_client.list_directories_and_files()
    for item in file_list:
        if item["is_directory"]:
            continue
        filename = item["name"]
        if filename.startswith(file_pattern):
            src_file_path = fs_output_dir.rstrip("/") + "/" + filename
            dst_file_path = archive_dir.rstrip("/") + "/" + filename
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
    from azure.storage.fileshare import ShareFileClient
    import os

    if not os.path.isdir(mft_input_dir):
        raise FileNotFoundError(f"Input directory '{mft_input_dir}' does not exist.")

    files_to_move = [f for f in os.listdir(mft_input_dir) if f.startswith(file_pattern)]
    if not files_to_move:
        raise FileNotFoundError(
            f"No files starting with pattern '{file_pattern}' found in '{mft_input_dir}'."
        )

    for filename in files_to_move:
        local_path = os.path.join(mft_input_dir, filename)
        remote_path = fs_output_dir.rstrip("/") + "/" + filename

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
    df = _run_spark_sql(check_sql)
    row = df.first()
    if row is None:
        insert_sql = f"""
            INSERT INTO ALDM_OPER.EXTRACT_SEQ_TRACK (EXTRACT_NAME, EXTRACT_MONTH, SEQ_NUM)
            VALUES ('{invoice_type}', '{month_str}', {seq_str})
        """
        _run_spark_sql(insert_sql)
    else:
        update_sql = f"""
            UPDATE ALDM_OPER.EXTRACT_SEQ_TRACK
            SET SEQ_NUM = {seq_str}
            WHERE EXTRACT_NAME = '{invoice_type}'
              AND EXTRACT_MONTH = '{month_str}'
        """
        _run_spark_sql(update_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove the old files if present at MFT location

# COMMAND ----------

import os

def remove_files_by_extension(mft_location, file_extension):
    if not os.path.isdir(mft_location):
        raise FileNotFoundError(f"Directory '{mft_location}' does not exist.")
    files_to_remove = [
        f for f in os.listdir(mft_location)
        if f.lower().endswith(file_extension.lower())
    ]
    for filename in files_to_remove:
        file_path = os.path.join(mft_location, filename)
        try:
            os.remove(file_path)
        except Exception as e:
            print(f"Failed to remove {filename}: {e}")
