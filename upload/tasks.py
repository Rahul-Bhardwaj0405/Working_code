
# import pandas as pd
# import logging
# from io import StringIO, BytesIO
# from .models import BookingData, RefundData  # Import models
# from celery import shared_task

# # Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# @shared_task
# def process_uploaded_files(file_content, file_name, bank_name, year, month, booking_or_refund):
#     logging.info(f"Starting to process file: {file_name}, Bank Name: {bank_name}, Booking/Refund: {booking_or_refund}")
    
#     try:
#         # Initialize an empty DataFrame
#         df = pd.DataFrame()

#         # Set possible delimiters for CSV and text files
#         possible_delimiters = [',', ';', '\t', '|', ' ', '.']
#         logging.info("Attempting to read the file content into a DataFrame.")

#         # Read the file content into a DataFrame based on file type
#         if file_name.endswith('.csv') or file_name.endswith('.txt'):
#             file_str = file_content.decode(errors='ignore')
#             delimiter = next((delim for delim in possible_delimiters if delim in file_str), ',')
#             df = pd.read_csv(StringIO(file_str), delimiter=delimiter)
#             logging.info(f"CSV/TXT file read successfully with delimiter '{delimiter}'.")

#         elif file_name.endswith(('.xlsx', '.xls')):
#             df = pd.read_excel(BytesIO(file_content), engine='openpyxl')
#             logging.info(f"Excel file read successfully: {file_name}.")
        
#         else:
#             logging.error(f"Unsupported file type: {file_name}")
#             return

#         # Log DataFrame structure
#         logging.info(f"DataFrame columns: {df.columns.tolist()}")
#         logging.info(f"DataFrame head:\n{df.head()}")

#         # Clean the column names
#         df.columns = df.columns.str.strip().str.replace(r'\W+', '', regex=True)
#         logging.info(f"Cleaned column names: {df.columns.tolist()}")

#         # Check for required columns
#         if 'CREDITEDON' not in df.columns or 'BOOKINGAMOUNT' not in df.columns:
#             logging.error("Required columns 'CREDITEDON' or 'BOOKINGAMOUNT' are missing.")
#             return

#         # Example processing for the bank 'karur_vysya'
#         if bank_name == 'karur_vysya':
#             sale_total = df.shape[0]  # Count the number of rows
#             logging.info(f"Sale total (count of rows): {sale_total}")

#             # Extract date
#             credited_on_dates = pd.to_datetime(df['CREDITEDON'], errors='coerce')

#             # Log the content of credited_on_dates for debugging
#             logging.info(f"CREDITEDON after conversion: {credited_on_dates}")

#             if credited_on_dates.notna().any():
#                 date = credited_on_dates.iloc[0].date()  # Extract the first valid date
#                 logging.info(f"Date extracted: {date}")
#             else:
#                 logging.error("No valid dates found in CREDITEDON column.")
#                 return


#             # Extract total sale amount
#             if 'BOOKINGAMOUNT' in df.columns:
#                 sale_amount = df['BOOKINGAMOUNT'].sum()
#                 logging.info(f"Total sale amount calculated: {sale_amount}")
#             else:
#                 logging.error("Column 'BOOKINGAMOUNT' is missing in the data.")
#                 return

#             # Check for duplicates and save data
#             if not BookingData.objects.filter(bank_name=bank_name, year=year, month=month, date=date, sale_amount=sale_amount).exists():
#                 BookingData.objects.create(
#                     bank_name=bank_name,
#                     year=year,
#                     month=month,
#                     date=date,
#                     sale_amount=sale_amount,
#                     sale_total=sale_total
#                 )
#                 logging.info(f"Booking data saved successfully for file: {file_name}.")
#             else:
#                 logging.info(f"Duplicate entry found. Data not saved for file: {file_name}.")

#         else:
#             logging.error(f"Unsupported bank: {bank_name}")

#     except Exception as e:
#         logging.error(f"Error processing file {file_name}: {e}")


import pandas as pd
from datetime import datetime
import logging
from io import StringIO, BytesIO
from .models import BookingData, RefundData  # Import models
from celery import shared_task

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@shared_task
def process_uploaded_files(file_content, file_name, bank_name, year, month, booking_or_refund):
    logging.info(f"Starting to process file: {file_name}, Bank Name: {bank_name}, Booking/Refund: {booking_or_refund}")
    
    try:
        # Initialize an empty DataFrame
        df = pd.DataFrame()

        # Set possible delimiters for CSV and text files
        possible_delimiters = [',', ';', '\t', '|', ' ', '.']
        logging.info("Attempting to read the file content into a DataFrame.")

        # Read the file content into a DataFrame based on file type
        if file_name.endswith('.csv') or file_name.endswith('.txt'):
            file_str = file_content.decode(errors='ignore')
            delimiter = next((delim for delim in possible_delimiters if delim in file_str), ',')
            df = pd.read_csv(StringIO(file_str), delimiter=delimiter, error_bad_lines=False, na_filter=False)
            logging.info(f"CSV/TXT file read successfully with delimiter '{delimiter}'.")

        elif file_name.endswith(('.xlsx', '.xls')):
            try:
                df = pd.read_excel(BytesIO(file_content), engine='openpyxl')
                logging.info(f"Excel file read successfully: {file_name}.")
            except Exception as e:
                logging.warning(f"Failed to read with 'openpyxl' engine, trying 'xlrd'. Error: {e}")
                df = pd.read_excel(BytesIO(file_content), engine='xlrd')  # Fallback to xlrd
                logging.info(f"Excel file read successfully with 'xlrd': {file_name}.")
        
        else:
            logging.error(f"Unsupported file type: {file_name}")
            return

        # Log DataFrame structure
        logging.info(f"DataFrame columns: {df.columns.tolist()}")
        logging.info(f"DataFrame head:\n{df.head()}")

        # Clean the column names
        df.columns = df.columns.str.strip().str.replace(r'\W+', '', regex=True)
        logging.info(f"Cleaned column names: {df.columns.tolist()}")

        # Check for required columns
        if 'CREDITEDON' not in df.columns or 'BOOKINGAMOUNT' not in df.columns:
            logging.error("Required columns 'CREDITEDON' or 'BOOKINGAMOUNT' are missing.")
            return

        # Example processing for the bank 'karur_vysya'
        if bank_name == 'karur_vysya':
            sale_total = df.shape[0]  # Count the number of rows
            logging.info(f"Sale total (count of rows): {sale_total}")

            # Extract date with error handling for failed conversions
            try:
                credited_on_dates = pd.to_datetime(df['CREDITEDON'], errors='coerce')
            except Exception as e:
                logging.warning(f"Error converting 'CREDITEDON' to datetime: {e}")
                credited_on_dates = pd.Series([None] * df.shape[0])

            # Log the content of credited_on_dates for debugging
            logging.info(f"CREDITEDON after conversion: {credited_on_dates}")

            if credited_on_dates.notna().any():
                date = credited_on_dates.iloc[0].date()  # Extract the first valid date
                logging.info(f"Date extracted: {date}")
            else:
                logging.error("No valid dates found in CREDITEDON column.")
                return

            # Extract total sale amount with error handling for numeric issues
            try:
                if 'BOOKINGAMOUNT' in df.columns:
                    sale_amount = pd.to_numeric(df['BOOKINGAMOUNT'], errors='coerce').sum()
                    logging.info(f"Total sale amount calculated: {sale_amount}")
                else:
                    logging.error("Column 'BOOKINGAMOUNT' is missing in the data.")
                    return
            except Exception as e:
                logging.error(f"Error calculating total sale amount: {e}")
                return

            # Check for duplicates and save data
            if not BookingData.objects.filter(bank_name=bank_name, year=year, month=month, date=date, sale_amount=sale_amount).exists():
                BookingData.objects.create(
                    bank_name=bank_name,
                    year=year,
                    month=month,
                    date=date,
                    sale_amount=sale_amount,
                    sale_total=sale_total
                )
                logging.info(f"Booking data saved successfully for file: {file_name}.")
            else:
                logging.info(f"Duplicate entry found. Data not saved for file: {file_name}.")

        else:
            logging.error(f"Unsupported bank: {bank_name}")

    except Exception as e:
        logging.error(f"Error processing file {file_name}: {e}")
