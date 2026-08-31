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

def file_seq_generator(lv_ext_filename):
    from pathlib import Path
    import datetime

    ext_filename_pttrn = lv_ext_filename
    lv_ext_filename_base = Path(lv_ext_filename).stem
    lv_ext_file_ext = Path(lv_ext_filename).suffix
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
# MAGIC ### Merge disclaimer.pdf and archive pdf at MFT end

# COMMAND ----------

# %pip install PyPDF2

# import os
# from PyPDF2 import PdfMerger
# import shutil

# def merge_and_archive_pdfs_mft(input_directory, output_directory, output_filename, file_type):
#     input_dir = f"{mft_out}/aldm/outbound_1/{input_directory}"
#     output_dir = f"{mft_out}/aldm/outbound_1/{output_directory}"
#     output_pdf = os.path.join(output_dir, output_filename)
#     disclaimer_pdf = os.path.join(input_dir, "DISCLAIMER", "DISCLAIMER_1.pdf")
#     archive_dir = os.path.join(input_dir, "archive")

#     os.makedirs(input_dir, exist_ok=True)
#     os.makedirs(output_dir, exist_ok=True)

#     try:
#         pdf_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.pdf')]
#         pdf_files.sort()
#         pdf_paths = [os.path.join(input_dir, f) for f in pdf_files]

#         merger = PdfMerger()
#         for pdf in pdf_paths:
#             try:
#                 merger.append(pdf)
#             except Exception as e:
#                 print(f"Failed to append {pdf}: {e}")

#         # Append disclaimer.pdf at the end only if file_type is 'MONTHLY' or 'MID_MONTHLY' and it exists
#         if file_type in ('MONTHLY', 'MID_MONTHLY') and os.path.exists(disclaimer_pdf):
#             try:
#                 merger.append(disclaimer_pdf)
#             except Exception as e:
#                 print(f"Failed to append disclaimer: {e}")

#         if not pdf_paths and os.path.exists(disclaimer_pdf):
#             print("No PDF files to merge. Exiting function.")
#             return

#         with open(output_pdf, "wb") as fout:
#             merger.write(fout)
#         merger.close()
#         print("PDF merge completed successfully.")

#         # Move only PDFs with {file_type} pattern in their name to /archive
#         os.makedirs(archive_dir, exist_ok=True)
#         pdf_files_to_move = [f for f in pdf_files if file_type and file_type in f]
#         for pdf_file in pdf_files_to_move:
#             src = os.path.join(input_dir, pdf_file)
#             dst = os.path.join(archive_dir, pdf_file)
#             try:
#                 shutil.move(src, dst)
#                 print(f"Successfully moved {pdf_file} to archive.")
#             except Exception as e:
#                 print(f"Failed to move {pdf_file} to archive: {e}")

#         print("All PDF files processed and archived successfully.")

#     except Exception as e:
#         print(f"PDF merge failed: {e}")

# COMMAND ----------

# MAGIC %pip install PyPDF2
# MAGIC import io
# MAGIC import os
# MAGIC import re
# MAGIC import shutil
# MAGIC import tempfile
# MAGIC
# MAGIC from PyPDF2 import PdfMerger, PdfReader, PdfWriter
# MAGIC from reportlab.lib import colors
# MAGIC from reportlab.pdfbase.pdfmetrics import stringWidth
# MAGIC from reportlab.pdfgen import canvas
# MAGIC
# MAGIC
# MAGIC PAGE_NUMBER_PATTERN = re.compile(
# MAGIC     r"^\s*Page\s+\d+(?:\s+of\s+\d+)?\s*$",
# MAGIC     re.IGNORECASE,
# MAGIC )
# MAGIC
# MAGIC
# MAGIC def _existing_page_number_boxes(page):
# MAGIC     """
# MAGIC     Locate page-number text drawn into an individual invoice.
# MAGIC
# MAGIC     PyPDF2 supplies the text matrix in PDF coordinates. Returning these boxes
# MAGIC     lets the overlay hide the old invoice-local number before the continuous
# MAGIC     combined number is applied.
# MAGIC     """
# MAGIC     boxes = []
# MAGIC
# MAGIC     def visitor(text, _cm, text_matrix, font_dict, font_size):
# MAGIC         clean_text = (text or "").strip()
# MAGIC         if not PAGE_NUMBER_PATTERN.match(clean_text):
# MAGIC             return
# MAGIC
# MAGIC         font_name = "Helvetica"
# MAGIC         if font_dict:
# MAGIC             base_font = str(font_dict.get("/BaseFont", "Helvetica"))
# MAGIC             font_name = base_font.lstrip("/").split("+")[-1]
# MAGIC
# MAGIC         try:
# MAGIC             width = stringWidth(clean_text, font_name, font_size)
# MAGIC         except Exception:
# MAGIC             width = max(len(clean_text) * font_size * 0.55, 55)
# MAGIC
# MAGIC         boxes.append(
# MAGIC             (
# MAGIC                 float(text_matrix[4]) - 2,
# MAGIC                 float(text_matrix[5]) - 2,
# MAGIC                 width + 4,
# MAGIC                 float(font_size) + 5,
# MAGIC             )
# MAGIC         )
# MAGIC
# MAGIC     try:
# MAGIC         page.extract_text(visitor_text=visitor)
# MAGIC     except TypeError:
# MAGIC         # Older PyPDF2 versions do not support visitor_text. Continuous page
# MAGIC         # numbers are still added; upgrading PyPDF2 enables old-label removal.
# MAGIC         pass
# MAGIC
# MAGIC     return boxes
# MAGIC
# MAGIC
# MAGIC def _add_continuous_page_numbers(source_pdf, output_pdf):
# MAGIC     """
# MAGIC     Replace invoice-local labels with continuous numbering for the merged PDF.
# MAGIC
# MAGIC     The final number is placed at bottom-left so it does not overlap the
# MAGIC     registered-office footer on the right. The disclaimer is included in
# MAGIC     `total_pages`.
# MAGIC     """
# MAGIC     reader = PdfReader(source_pdf)
# MAGIC     writer = PdfWriter()
# MAGIC     total_pages = len(reader.pages)
# MAGIC
# MAGIC     for page_index, page in enumerate(reader.pages, start=1):
# MAGIC         page_width = float(page.mediabox.width)
# MAGIC         page_height = float(page.mediabox.height)
# MAGIC         overlay_buffer = io.BytesIO()
# MAGIC         overlay = canvas.Canvas(
# MAGIC             overlay_buffer,
# MAGIC             pagesize=(page_width, page_height),
# MAGIC         )
# MAGIC
# MAGIC         # Cover any old "Page 1 of N" label found in an input invoice.
# MAGIC         overlay.setFillColor(colors.white)
# MAGIC         for x, y, width, height in _existing_page_number_boxes(page):
# MAGIC             overlay.rect(
# MAGIC                 x,
# MAGIC                 y,
# MAGIC                 width,
# MAGIC                 height,
# MAGIC                 stroke=0,
# MAGIC                 fill=1,
# MAGIC             )
# MAGIC
# MAGIC         # Add one global sequence across invoices and the disclaimer.
# MAGIC         overlay.setFillColor(colors.black)
# MAGIC         overlay.setFont("Helvetica", 9)
# MAGIC         overlay.drawString(
# MAGIC             18,
# MAGIC             18,
# MAGIC             f"Page {page_index} of {total_pages}",
# MAGIC         )
# MAGIC         overlay.save()
# MAGIC         overlay_buffer.seek(0)
# MAGIC
# MAGIC         number_layer = PdfReader(overlay_buffer).pages[0]
# MAGIC         page.merge_page(number_layer)
# MAGIC         writer.add_page(page)
# MAGIC
# MAGIC     with open(output_pdf, "wb") as output_file:
# MAGIC         writer.write(output_file)
# MAGIC
# MAGIC     return total_pages
# MAGIC
# MAGIC
# MAGIC def merge_and_archive_pdfs_mft(
# MAGIC     input_directory,
# MAGIC     output_directory,
# MAGIC     output_filename,
# MAGIC     file_type,
# MAGIC ):
# MAGIC     input_dir = f"{mft_out}/aldm/outbound_1/{input_directory}"
# MAGIC     output_dir = f"{mft_out}/aldm/outbound_1/{output_directory}"
# MAGIC     output_pdf = os.path.join(output_dir, output_filename)
# MAGIC     disclaimer_pdf = os.path.join(
# MAGIC         input_dir,
# MAGIC         "DISCLAIMER",
# MAGIC         "DISCLAIMER_1.pdf",
# MAGIC     )
# MAGIC     archive_dir = os.path.join(input_dir, "archive")
# MAGIC
# MAGIC     os.makedirs(input_dir, exist_ok=True)
# MAGIC     os.makedirs(output_dir, exist_ok=True)
# MAGIC
# MAGIC     normalised_type = (
# MAGIC         str(file_type).upper().replace("_", "").replace("-", "")
# MAGIC     )
# MAGIC     append_disclaimer = normalised_type in {
# MAGIC         "MONTHLY",
# MAGIC         "MIDMONTHLY",
# MAGIC     }
# MAGIC
# MAGIC     pdf_files = sorted(
# MAGIC         filename
# MAGIC         for filename in os.listdir(input_dir)
# MAGIC         if filename.lower().endswith(".pdf")
# MAGIC     )
# MAGIC
# MAGIC     if not pdf_files:
# MAGIC         print("No invoice PDF files to merge. Exiting function.")
# MAGIC         return
# MAGIC
# MAGIC     pdf_paths = [
# MAGIC         os.path.join(input_dir, filename)
# MAGIC         for filename in pdf_files
# MAGIC     ]
# MAGIC     temp_merged_pdf = None
# MAGIC
# MAGIC     try:
# MAGIC         fd, temp_merged_pdf = tempfile.mkstemp(
# MAGIC             prefix="combined_invoices_",
# MAGIC             suffix=".pdf",
# MAGIC             dir=output_dir,
# MAGIC         )
# MAGIC         os.close(fd)
# MAGIC
# MAGIC         merger = PdfMerger()
# MAGIC         try:
# MAGIC             for pdf_path in pdf_paths:
# MAGIC                 merger.append(pdf_path)
# MAGIC
# MAGIC             if append_disclaimer:
# MAGIC                 if not os.path.isfile(disclaimer_pdf):
# MAGIC                     raise FileNotFoundError(
# MAGIC                         f"Disclaimer PDF not found: {disclaimer_pdf}"
# MAGIC                     )
# MAGIC                 merger.append(disclaimer_pdf)
# MAGIC
# MAGIC             with open(temp_merged_pdf, "wb") as merged_file:
# MAGIC                 merger.write(merged_file)
# MAGIC         finally:
# MAGIC             merger.close()
# MAGIC
# MAGIC         total_pages = _add_continuous_page_numbers(
# MAGIC             temp_merged_pdf,
# MAGIC             output_pdf,
# MAGIC         )
# MAGIC         print(
# MAGIC             f"PDF merge completed successfully: {output_pdf} "
# MAGIC             f"({total_pages} pages)."
# MAGIC         )
# MAGIC
# MAGIC         # Archive only after the final numbered PDF has been written.
# MAGIC         os.makedirs(archive_dir, exist_ok=True)
# MAGIC         pdf_files_to_move = [
# MAGIC             filename
# MAGIC             for filename in pdf_files
# MAGIC             if file_type and str(file_type) in filename
# MAGIC         ]
# MAGIC         for pdf_file in pdf_files_to_move:
# MAGIC             src = os.path.join(input_dir, pdf_file)
# MAGIC             dst = os.path.join(archive_dir, pdf_file)
# MAGIC             if os.path.exists(dst):
# MAGIC                 os.remove(dst)
# MAGIC             shutil.move(src, dst)
# MAGIC             print(f"Successfully moved {pdf_file} to archive.")
# MAGIC
# MAGIC         print("All PDF files processed and archived successfully.")
# MAGIC
# MAGIC     except Exception as exc:
# MAGIC         raise RuntimeError(f"PDF merge failed: {exc}") from exc
# MAGIC     finally:
# MAGIC         if temp_merged_pdf and os.path.exists(temp_merged_pdf):
# MAGIC             os.remove(temp_merged_pdf)

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