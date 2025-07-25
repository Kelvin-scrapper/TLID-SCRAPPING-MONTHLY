# ---------------------------------------------------
# ENHANCED SCRAPER WITH AUTOMATIC TLID MAPPING
# ---------------------------------------------------
import os
import time
import pandas as pd
import json
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

# ---------------------------------------------------
# SCRIPT CONFIGURATION
# ---------------------------------------------------
TARGET_URL = "https://www.tii.org.tw/tii/english/rd/importantIndices/"
SECTION_HEADER_TEXT = "Life Insurance Industry"

# --- Setup directories ---
script_dir = os.path.abspath(os.path.dirname(__file__))
download_dir = os.path.join(script_dir, "downloads")
output_dir = os.path.join(script_dir, "processed_data")

for directory in [download_dir, output_dir]:
    if not os.path.exists(directory):
        os.makedirs(directory)

# ---------------------------------------------------
# TLID MAPPING CONFIGURATION
# ---------------------------------------------------
TLID_MAPPING = {
    'TLID.BANKDEP.M': {
        'english': 'Bank Deposits',
        'chinese': '銀行存款',
        'excel_pattern': 'Bank Deposits'
    },
    'TLID.SECUR.M': {
        'english': 'Securities',
        'chinese': '有價證券',
        'excel_pattern': 'Securities'
    },
    'TLID.GOVTREASBONDS.M': {
        'english': 'Government & Treasury Bonds',
        'chinese': '公債及國庫券',
        'excel_pattern': 'Government & Treasury Bonds'
    },
    'TLID.FINBONDS.M': {
        'english': 'Financial bond, deposit receipt, bank draft and promissory note',
        'chinese': '金融債券、存單、匯票與本票',
        'excel_pattern': 'Financial bond, deposit receipt, bank draft'
    },
    'TLID.STOCKS.M': {
        'english': 'Stocks',
        'chinese': '股票',
        'excel_pattern': 'Stocks'
    },
    'TLID.CORPBONDS.M': {
        'english': 'Corporation Bonds',
        'chinese': '公司債',
        'excel_pattern': 'Corporation Bonds'
    },
    'TLID.FUNDBENCERT.M': {
        'english': 'Funds & Benefit Certificates',
        'chinese': '基金及受益憑證',
        'excel_pattern': 'Funds & Benefit Certificates'
    },
    'TLID.SECPROD.M': {
        'english': 'Securitized products and other',
        'chinese': '證劵化商品及其他',
        'excel_pattern': 'Securitized products and other'
    },
    'TLID.REALEST.M': {
        'english': 'Real Estates',
        'chinese': '不動產',
        'excel_pattern': 'Real Estates'
    },
    'TLID.INVEST.M': {
        'english': 'Investment',
        'chinese': '投資用',
        'excel_pattern': 'Investment'
    },
    'TLID.PRIVUSE.M': {
        'english': 'Private Use',
        'chinese': '自用',
        'excel_pattern': 'Private Use'
    },
    'TLID.LOANPOL.M': {
        'english': 'Loan to Policy-holders',
        'chinese': '壽險貸款',
        'excel_pattern': 'Loan to Policy-holders'
    },
    'TLID.LOANS.M': {
        'english': 'Loans',
        'chinese': '放款',
        'excel_pattern': 'Loans'
    },
    'TLID.FORINEST.M': {
        'english': 'Foreign Investments',
        'chinese': '國外投資',
        'excel_pattern': 'Foreign Investments'
    },
    'TLID.AUTPROJ.M': {
        'english': 'Authorized Projects or Public Investment',
        'chinese': '專案運用及公共投資',
        'excel_pattern': 'Authorized Projects or Public Investment'
    },
    'TLID.INVINSENT.M': {
        'english': 'Investment on Insurance Enterprise',
        'chinese': '投資保險相關事業',
        'excel_pattern': 'Investment on Insurance Enterprise'
    },
    'TLID.DERIV.M': {
        'english': 'Derivatives',
        'chinese': '從事衍生性商品交易',
        'excel_pattern': 'Derivatives'
    },
    'TLID.OTHERUTILCAP.M': {
        'english': 'Other utilizations of capital (Approved)',
        'chinese': '其他經核准之資金運用',
        'excel_pattern': 'Other utilizations of capital'
    },
    'TLID.TOTALAMCAPINV.M': {
        'english': 'Total Amount of Capital Invested',
        'chinese': '資金運用總額',
        'excel_pattern': 'Total Amount of Capital Invested'
    }
}

# Define the exact order of TLID codes
tlid_order = [
    'TLID.BANKDEP.M',
    'TLID.SECUR.M', 
    'TLID.GOVTREASBONDS.M',
    'TLID.FINBONDS.M',
    'TLID.STOCKS.M',
    'TLID.CORPBONDS.M',
    'TLID.FUNDBENCERT.M',
    'TLID.SECPROD.M',
    'TLID.REALEST.M',
    'TLID.INVEST.M',
    'TLID.PRIVUSE.M',
    'TLID.LOANPOL.M',
    'TLID.LOANS.M',
    'TLID.FORINEST.M',
    'TLID.AUTPROJ.M',
    'TLID.INVINSENT.M',
    'TLID.DERIV.M',
    'TLID.OTHERUTILCAP.M',
    'TLID.TOTALAMCAPINV.M'
]

# ---------------------------------------------------
# MAPPING FUNCTIONS
# ---------------------------------------------------

def find_row_by_pattern(df, pattern, column_index=0):
    """Find row index by matching pattern in specified column"""
    for idx, row in df.iterrows():
        cell_value = str(row.iloc[column_index]) if pd.notna(row.iloc[column_index]) else ""
        if pattern.lower() in cell_value.lower():
            return idx
    return None

def find_latest_amount_column(df):
    """Find the column with '2025/04' header"""
    print("  Scanning for 2025/04 column...")
    
    total_cols = len(df.columns)
    print(f"  Total columns: {total_cols}")
    
    # Look at the header structure to understand the layout
    print("  Header structure analysis:")
    for row_idx in range(min(6, len(df))):
        row_data = df.iloc[row_idx]
        # Show the rightmost columns where 2025/04 should be
        rightmost_data = [str(cell)[:25] for cell in row_data.iloc[-10:].values]
        print(f"    Row {row_idx + 1} (last 10 cols): {rightmost_data}")
    
    # Strategy: Find the column with exactly "2025/04"
    amount_col_idx = None
    
    print("  Looking specifically for '2025/04'...")
    
    # Scan all columns for the exact "2025/04" pattern
    for col_idx in range(1, total_cols):
        # Check this column in multiple header rows
        for row_idx in range(min(6, len(df))):
            cell_value = str(df.iloc[row_idx, col_idx]).strip()
            
            # Look for exact "2025/04" or variations
            if (cell_value == "2025/04" or 
                cell_value == "2025-04" or 
                "2025/04" in cell_value or
                "2025-04" in cell_value):
                
                print(f"    ✓ Found 2025/04: '{cell_value}' at row {row_idx + 1}, col {col_idx}")
                
                # Verify this column has numeric data by checking a few data rows
                has_numeric_data = False
                sample_values = []
                
                for data_row in range(5, min(25, len(df))):  # Check actual data rows
                    try:
                        test_val = df.iloc[data_row, col_idx]
                        if pd.notna(test_val):
                            sample_values.append(f"Row {data_row + 1}: {test_val}")
                            if isinstance(test_val, (int, float)) and test_val != 0:
                                has_numeric_data = True
                    except:
                        continue
                
                print(f"    Sample values from col {col_idx}:")
                for sample in sample_values[:5]:  # Show first 5 samples
                    print(f"      {sample}")
                
                if has_numeric_data:
                    amount_col_idx = col_idx
                    print(f"    ✓ Using col {col_idx} - has 2025/04 and numeric data")
                    break
        
        if amount_col_idx:
            break
    
    # If not found with exact match, try broader search
    if not amount_col_idx:
        print("  Broader search for 2025 patterns...")
        for col_idx in range(total_cols - 10, total_cols):  # Focus on rightmost 10 columns
            for row_idx in range(min(6, len(df))):
                cell_value = str(df.iloc[row_idx, col_idx]).strip()
                
                if "2025" in cell_value and ("04" in cell_value or "/04" in cell_value):
                    print(f"    Found 2025 pattern: '{cell_value}' at row {row_idx + 1}, col {col_idx}")
                    amount_col_idx = col_idx
                    break
            
            if amount_col_idx:
                break
    
    latest_period = "2025-04" if amount_col_idx else None
    print(f"  Final result: period={latest_period}, column={amount_col_idx}")
    
    return latest_period, amount_col_idx

def extract_data_columns(df, row_index, start_col=1):
    """Extract numerical data with full precision from the 2025/04 column"""
    if row_index is None:
        return {}
    
    # Find the 2025/04 column
    latest_period, amount_col_idx = find_latest_amount_column(df)
    
    if latest_period is None or amount_col_idx is None:
        print(f"  ⚠ Could not find 2025/04 column for row {row_index + 1}")
        return {}
    
    row_data = {}
    row = df.iloc[row_index]
    
    try:
        # Get the value from the identified 2025/04 column
        raw_value = row.iloc[amount_col_idx] if amount_col_idx < len(row) else None
        
        print(f"    Raw value at col {amount_col_idx}: {raw_value} (type: {type(raw_value)})")
        
        # Handle different data types and ensure full precision is captured
        if pd.notna(raw_value):
            if isinstance(raw_value, (int, float)):
                # Store the full precision number (as shown in formula bar)
                row_data[f"{latest_period}_amount"] = float(raw_value)
                print(f"    ✓ Extracted {latest_period}: {raw_value} (full precision: {float(raw_value)})")
            elif isinstance(raw_value, str):
                # Try to convert string to number, preserving precision
                try:
                    # Remove commas, spaces, and other formatting but preserve decimals
                    cleaned_val = raw_value.replace(',', '').replace(' ', '').replace('　', '')
                    
                    # Check if it's a valid number string
                    if cleaned_val.replace('.', '').replace('-', '').isdigit():
                        # Use float to preserve decimal precision
                        numeric_val = float(cleaned_val)
                        row_data[f"{latest_period}_amount"] = numeric_val
                        print(f"    ✓ Extracted {latest_period}: {numeric_val} (converted from string, full precision)")
                    else:
                        print(f"    ⚠ String value not convertible to number: '{raw_value}'")
                except ValueError as e:
                    print(f"    ⚠ Could not convert string to number: '{raw_value}' - {e}")
            else:
                # Try to convert any other type to float
                try:
                    numeric_val = float(str(raw_value).replace(',', '').replace(' ', ''))
                    row_data[f"{latest_period}_amount"] = numeric_val
                    print(f"    ✓ Extracted {latest_period}: {numeric_val} (converted from {type(raw_value)})")
                except:
                    print(f"    ⚠ Could not convert {type(raw_value)} to number: {raw_value}")
        else:
            print(f"    ⚠ No data or NaN value at column {amount_col_idx}")
            
    except (IndexError, ValueError, TypeError) as e:
        print(f"    ✗ Error extracting data: {e}")
    
    return row_data

def process_excel_file(file_path):
    """Process the downloaded Excel file and apply TLID mapping with full precision"""
    print(f"\n--- PROCESSING EXCEL FILE: {file_path} ---")
    
    try:
        # Read Excel file with specific settings to preserve precision
        # Use openpyxl engine for better precision handling
        df = pd.read_excel(file_path, header=None, engine='openpyxl', dtype=object)
        print(f"SUCCESS: Loaded Excel file with {len(df)} rows and {len(df.columns)} columns")
        
        # Convert numeric columns but preserve precision
        for col in df.columns:
            for row in range(len(df)):
                cell_value = df.iloc[row, col]
                if pd.notna(cell_value) and isinstance(cell_value, str):
                    # Try to convert string numbers to float with full precision
                    try:
                        if cell_value.replace(',', '').replace('.', '').replace('-', '').isdigit():
                            df.iloc[row, col] = float(cell_value.replace(',', ''))
                    except:
                        pass
        
        # Initialize results
        mapped_data = {}
        metadata = {
            'file_processed': os.path.basename(file_path),
            'processing_date': datetime.now().isoformat(),
            'total_tlid_codes': len(TLID_MAPPING),
            'successfully_mapped': 0,
            'mapping_details': {}
        }
        
        # Process each TLID code
        print("\n--- APPLYING TLID MAPPING ---")
        for tlid_code, mapping_info in TLID_MAPPING.items():
            print(f"\nProcessing {tlid_code}...")
            print(f"  Looking for: {mapping_info['excel_pattern']}")
            
            # Find the row containing this investment type
            row_index = find_row_by_pattern(df, mapping_info['excel_pattern'])
            
            if row_index is not None:
                print(f"  ✓ Found at row {row_index + 1}")
                
                # Extract data from this row with full precision
                row_data = extract_data_columns(df, row_index)
                
                if row_data:
                    mapped_data[tlid_code] = {
                        'mapping_info': mapping_info,
                        'data': row_data,
                        'excel_row': row_index + 1
                    }
                    metadata['successfully_mapped'] += 1
                    metadata['mapping_details'][tlid_code] = {
                        'status': 'success',
                        'excel_row': row_index + 1,
                        'data_points': len(row_data)
                    }
                    print(f"  ✓ Extracted {len(row_data)} data points")
                else:
                    metadata['mapping_details'][tlid_code] = {
                        'status': 'found_but_no_data',
                        'excel_row': row_index + 1
                    }
                    print(f"  ⚠ Found row but no valid data extracted")
            else:
                metadata['mapping_details'][tlid_code] = {
                    'status': 'not_found'
                }
                print(f"  ✗ Not found in Excel file")
        
        return mapped_data, metadata
        
    except Exception as e:
        print(f"ERROR processing Excel file: {e}")
        return None, None

def process_excel_file_xlrd(file_path):
    """Alternative method to process Excel file using xlrd for older formats"""
    print(f"\n--- PROCESSING EXCEL FILE (XLRD): {file_path} ---")
    
    try:
        # Try reading with xlrd engine (for older .xls files)
        df = pd.read_excel(file_path, header=None, engine='xlrd')
        print(f"SUCCESS: Loaded Excel file with {len(df)} rows and {len(df.columns)} columns")
        
        # Initialize results
        mapped_data = {}
        metadata = {
            'file_processed': os.path.basename(file_path),
            'processing_date': datetime.now().isoformat(),
            'total_tlid_codes': len(TLID_MAPPING),
            'successfully_mapped': 0,
            'mapping_details': {}
        }
        
        # Process each TLID code
        print("\n--- APPLYING TLID MAPPING (XLRD) ---")
        for tlid_code, mapping_info in TLID_MAPPING.items():
            print(f"\nProcessing {tlid_code}...")
            print(f"  Looking for: {mapping_info['excel_pattern']}")
            
            # Find the row containing this investment type
            row_index = find_row_by_pattern(df, mapping_info['excel_pattern'])
            
            if row_index is not None:
                print(f"  ✓ Found at row {row_index + 1}")
                
                # Extract data from this row
                row_data = extract_data_columns(df, row_index)
                
                if row_data:
                    mapped_data[tlid_code] = {
                        'mapping_info': mapping_info,
                        'data': row_data,
                        'excel_row': row_index + 1
                    }
                    metadata['successfully_mapped'] += 1
                    metadata['mapping_details'][tlid_code] = {
                        'status': 'success',
                        'excel_row': row_index + 1,
                        'data_points': len(row_data)
                    }
                    print(f"  ✓ Extracted {len(row_data)} data points")
                else:
                    metadata['mapping_details'][tlid_code] = {
                        'status': 'found_but_no_data',
                        'excel_row': row_index + 1
                    }
                    print(f"  ⚠ Found row but no valid data extracted")
            else:
                metadata['mapping_details'][tlid_code] = {
                    'status': 'not_found'
                }
                print(f"  ✗ Not found in Excel file")
        
        return mapped_data, metadata
        
    except Exception as e:
        print(f"ERROR processing Excel file with xlrd: {e}")
        return None, None

def create_tlid_format_data(mapped_data):
    """Create data in the exact TLID format for the most recent period only"""
    
    # Find the most recent period from all mapped data
    latest_period = None
    for tlid_code, data in mapped_data.items():
        if data.get('data'):
            periods = [key.split('_')[0] for key in data['data'].keys() if '_amount' in key]
            for period in periods:
                if latest_period is None or period > latest_period:
                    latest_period = period
    
    if not latest_period:
        print("No period data found")
        return None
    
    print(f"Creating TLID format for period: {latest_period}")
    
    # Create two separate rows: header and data
    header_data = {}
    data_row = {}
    
    # Build header row (English titles)
    for tlid_code in tlid_order:
        if tlid_code in mapped_data:
            header_data[tlid_code] = mapped_data[tlid_code]['mapping_info']['english']
        else:
            header_data[tlid_code] = TLID_MAPPING.get(tlid_code, {}).get('english', '')
    
    # Build data row (amounts) - preserve full precision
    data_row['Period'] = latest_period
    for tlid_code in tlid_order:
        if tlid_code in mapped_data and mapped_data[tlid_code].get('data'):
            amount_key = f"{latest_period}_amount"
            if amount_key in mapped_data[tlid_code]['data']:
                value = mapped_data[tlid_code]['data'][amount_key]
                # Keep full precision as shown in formula bar
                if isinstance(value, (int, float)):
                    # Don't round - keep the full precision from the formula bar
                    data_row[tlid_code] = value
                else:
                    data_row[tlid_code] = str(value)
            else:
                data_row[tlid_code] = ""
        else:
            data_row[tlid_code] = ""
    
    # Create final DataFrame with two rows
    final_data = []
    
    # Row 1: Headers (no Period column)
    header_row_dict = {}
    header_row_dict['Period'] = ""  # Empty for header
    for tlid_code in tlid_order:
        header_row_dict[tlid_code] = header_data.get(tlid_code, "")
    final_data.append(header_row_dict)
    
    # Row 2: Data (with Period)
    final_data.append(data_row)
    
    # Convert to DataFrame
    df = pd.DataFrame(final_data)
    
    # Reorder columns: Period first, then TLID codes in order
    column_order = ['Period'] + tlid_order
    df = df[column_order]
    
    return df

def save_processed_data(mapped_data, metadata, original_filename):
    """Save the processed and mapped data to files"""
    if not mapped_data:
        print("No data to save")
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = os.path.splitext(original_filename)[0]
    
    # Save mapped data as JSON
    json_filename = f"{base_name}_mapped_{timestamp}.json"
    json_path = os.path.join(output_dir, json_filename)
    
    output_data = {
        'metadata': metadata,
        'mapped_data': mapped_data
    }
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Saved mapped data to: {json_path}")
    
    # Create TLID format CSV (horizontal layout)
    try:
        tlid_format_data = create_tlid_format_data(mapped_data)
        
        if tlid_format_data is not None and not tlid_format_data.empty:
            csv_filename = f"{base_name}_TLID_format_{timestamp}.csv"
            csv_path = os.path.join(output_dir, csv_filename)
            
            tlid_format_data.to_csv(csv_path, index=False)
            print(f"✓ Saved TLID format CSV to: {csv_path}")
            
            # Also save as Excel for better formatting with full precision
            try:
                excel_filename = f"{base_name}_TLID_format_{timestamp}.xlsx"
                excel_path = os.path.join(output_dir, excel_filename)
                
                # Use xlsxwriter engine for better number formatting control
                with pd.ExcelWriter(excel_path, engine='xlsxwriter', options={'remove_timezone': True}) as writer:
                    tlid_format_data.to_excel(writer, index=False, sheet_name='TLID_Data')
                    
                    # Get the xlsxwriter workbook and worksheet objects
                    workbook = writer.book
                    worksheet = writer.sheets['TLID_Data']
                    
                    # Define a number format that shows full precision
                    number_format = workbook.add_format({'num_format': '0.000000'})
                    
                    # Apply number format to data rows (skip header rows)
                    for col_num, tlid_code in enumerate(tlid_order):
                        worksheet.set_column(col_num + 1, col_num + 1, 15, number_format)
                
                print(f"✓ Saved TLID format Excel to: {excel_path}")
            except Exception as e:
                print(f"⚠ Could not save Excel with precision formatting: {e}")
                # Fallback to standard Excel save
                excel_filename = f"{base_name}_TLID_format_{timestamp}_simple.xlsx"
                excel_path = os.path.join(output_dir, excel_filename)
                tlid_format_data.to_excel(excel_path, index=False)
                print(f"✓ Saved TLID format Excel (simple) to: {excel_path}")
        else:
            print("⚠ No TLID format data created - check data extraction")
    except Exception as e:
        print(f"✗ Error creating TLID format: {e}")
        import traceback
        traceback.print_exc()
    
    # Print summary
    print(f"\n--- PROCESSING SUMMARY ---")
    print(f"Total TLID codes: {metadata['total_tlid_codes']}")
    print(f"Successfully mapped: {metadata['successfully_mapped']}")
    print(f"Success rate: {(metadata['successfully_mapped']/metadata['total_tlid_codes']*100):.1f}%")

# ---------------------------------------------------
# MAIN SCRIPT WITH INTEGRATED MAPPING
# ---------------------------------------------------

driver = None
print("\n--- Enhanced Scraper with TLID Mapping Started ---")
print(f"Files will be saved to: {download_dir}")
print(f"Processed data will be saved to: {output_dir}")

try:
    # 1. SETUP THE WEBDRIVER
    print("\nSTEP 1: Setting up the Chrome WebDriver...")
    chrome_options = Options()
    prefs = {"download.default_directory": download_dir}
    chrome_options.add_experimental_option("prefs", prefs)
    service = ChromeService()
    driver = webdriver.Chrome(service=service, options=chrome_options)
    print("SUCCESS: WebDriver configured.")

    # 2. ACCESS THE SITE
    print(f"\nSTEP 2: Accessing the site -> {TARGET_URL}")
    driver.get(TARGET_URL)
    wait = WebDriverWait(driver, 20)
    print("SUCCESS: Site access complete.")

    # 3. LOCATE AND EXPAND THE CORRECT SECTION
    print(f"\nSTEP 3: Finding and expanding the '{SECTION_HEADER_TEXT}' section...")
    header_xpath = f"//div[contains(@class, 'card-header') and contains(text(), '{SECTION_HEADER_TEXT}')]"
    section_header = wait.until(EC.element_to_be_clickable((By.XPATH, header_xpath)))
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", section_header)
    time.sleep(1)
    section_header.click()
    print("SUCCESS: Clicked the section header.")

    # 4. WAIT FOR SECTION TO EXPAND
    print("\nSTEP 4: Waiting for section to expand...")
    time.sleep(3)

    # 5. DIRECT SEARCH FOR THE 17-1 XLS LINK
    print("\nSTEP 5: Looking for 17-1 XLS download link...")
    
    strategies = [
        "//a[contains(@href, '17-1_') and contains(@class, 'icon-file-xls')]",
        "//a[contains(@href, '17-1_') and contains(@title, '.xls')]",
        "//a[contains(@href, '17-1_')]",
        "//a[contains(@class, 'icon-file-xls') and contains(@title, 'Life insurance industry fund utilization')]"
    ]
    
    download_link = None
    used_strategy = None
    
    for i, xpath in enumerate(strategies, 1):
        try:
            print(f"  Trying strategy {i}: {xpath}")
            potential_links = driver.find_elements(By.XPATH, xpath)
            
            if potential_links:
                for link in potential_links:
                    href = link.get_attribute('href')
                    title = link.get_attribute('title') or ""
                    class_name = link.get_attribute('class') or ""
                    
                    print(f"    Found link: {href}")
                    
                    if '.xls' in href.lower() or 'xls' in class_name.lower():
                        download_link = link
                        used_strategy = i
                        break
                
                if download_link:
                    break
                    
        except Exception as e:
            print(f"    Strategy {i} failed: {e}")
            continue
    
    if not download_link:
        raise Exception("Could not find the 17-1 XLS download link using any strategy")
    
    # 6. EXTRACT FILE INFO AND DOWNLOAD
    href = download_link.get_attribute('href')
    title = download_link.get_attribute('title')
    filename = href.split('/')[-1] if href else "unknown"
    
    print(f"\nSTEP 6: Found target link using strategy {used_strategy}:")
    print(f"  File: {filename}")
    print(f"  Full URL: {href}")
    
    # Scroll to the link and click it
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", download_link)
    time.sleep(1)
    download_link.click()
    
    print(f"\nSTEP 7: Clicked download link. Waiting for download to complete...")
    
    # Enhanced download waiting with verification
    max_wait_time = 60  # Maximum wait time in seconds
    check_interval = 2  # Check every 2 seconds
    waited_time = 0
    
    while waited_time < max_wait_time:
        time.sleep(check_interval)
        waited_time += check_interval
        
        # Check for new files in download directory
        current_files = [f for f in os.listdir(download_dir) if f.endswith('.xls')]
        
        if current_files:
            # Check if file is still being downloaded (has .crdownload extension or is very small)
            latest_file = max(current_files, key=lambda f: os.path.getctime(os.path.join(download_dir, f)))
            file_path = os.path.join(download_dir, latest_file)
            file_size = os.path.getsize(file_path)
            
            # Check if there are any .crdownload files (Chrome partial downloads)
            temp_files = [f for f in os.listdir(download_dir) if f.endswith('.crdownload')]
            
            if not temp_files and file_size > 10000:  # File exists, no temp files, and size > 10KB
                print(f"SUCCESS: Download completed. File size: {file_size} bytes")
                break
            else:
                print(f"  Still downloading... File size: {file_size} bytes, Temp files: {len(temp_files)}")
        else:
            print(f"  Waiting for download to start... ({waited_time}s)")
    
    if waited_time >= max_wait_time:
        print("WARNING: Download may not have completed within the expected time")
    
    # 8. VERIFY DOWNLOAD AND GET FILE PATH
    print(f"\nSTEP 8: Verifying downloaded files...")
    downloaded_files = [f for f in os.listdir(download_dir) if f.endswith('.xls')]
    
    if downloaded_files:
        print(f"SUCCESS: Found downloaded file(s): {downloaded_files}")
        
        # Process the most recent file
        latest_file = max(downloaded_files, key=lambda f: os.path.getctime(os.path.join(download_dir, f)))
        file_path = os.path.join(download_dir, latest_file)
        file_size = os.path.getsize(file_path)
        
        print(f"Processing: {latest_file}")
        print(f"File size: {file_size} bytes")
        
        # Verify file is not corrupted by checking minimum size and trying to read first few bytes
        if file_size < 1000:
            raise Exception(f"Downloaded file is too small ({file_size} bytes) - likely corrupted")
        
        # Try to read first few bytes to verify it's a valid Excel file
        try:
            with open(file_path, 'rb') as f:
                first_bytes = f.read(8)
                # Check for Excel file signatures
                if not (first_bytes.startswith(b'PK') or first_bytes.startswith(b'\xd0\xcf\x11\xe0')):
                    print(f"WARNING: File may not be a valid Excel file. First bytes: {first_bytes}")
                else:
                    print("✓ File appears to be a valid Excel file")
        except Exception as e:
            print(f"WARNING: Could not verify file format: {e}")
        
        # 9. APPLY TLID MAPPING
        print(f"\nSTEP 9: Applying TLID mapping to downloaded file...")
        
        # Try different methods to read the Excel file
        mapped_data, metadata = None, None
        
        # Method 1: Try with openpyxl
        try:
            print("  Trying to read with openpyxl engine...")
            mapped_data, metadata = process_excel_file(file_path)
        except Exception as e:
            print(f"  openpyxl failed: {e}")
        
        # Method 2: Try with xlrd (for older Excel files)
        if not mapped_data:
            try:
                print("  Trying to read with xlrd engine...")
                mapped_data, metadata = process_excel_file_xlrd(file_path)
            except Exception as e:
                print(f"  xlrd failed: {e}")
        
        # Method 3: Try downloading again if file seems corrupted
        if not mapped_data and file_size < 50000:  # If file is suspiciously small
            print("  File seems corrupted, attempting re-download...")
            
            # Delete the corrupted file
            os.remove(file_path)
            
            # Click download link again
            try:
                download_link.click()
                time.sleep(15)  # Wait longer for re-download
                
                # Check for new file
                new_files = [f for f in os.listdir(download_dir) if f.endswith('.xls')]
                if new_files:
                    latest_file = max(new_files, key=lambda f: os.path.getctime(os.path.join(download_dir, f)))
                    file_path = os.path.join(download_dir, latest_file)
                    print(f"  Re-downloaded file: {latest_file}, Size: {os.path.getsize(file_path)} bytes")
                    
                    # Try processing again
                    mapped_data, metadata = process_excel_file(file_path)
            except Exception as e:
                print(f"  Re-download failed: {e}")
        
        if mapped_data and metadata:
            # 10. SAVE PROCESSED DATA
            print(f"\nSTEP 10: Saving processed data...")
            save_processed_data(mapped_data, metadata, latest_file)
            print("SUCCESS: TLID mapping completed successfully!")
        else:
            print("ERROR: Failed to process Excel file or apply mapping")
            print("This could be due to:")
            print("- Corrupted download")
            print("- Changed file format on the website")
            print("- Network issues during download")
            print("- File access permissions")
    else:
        print("WARNING: No .xls files found in download directory")

except Exception as e:
    print(f"\nAN ERROR OCCURRED: {e}")
    print("Current URL:", driver.current_url if driver else "N/A")
    
    if driver:
        try:
            all_17_links = driver.find_elements(By.XPATH, "//a[contains(@href, '17-1')]")
            print(f"\nDEBUG: Found {len(all_17_links)} total links containing '17-1':")
            for link in all_17_links:
                href = link.get_attribute('href')
                print(f"  {href}")
        except:
            print("Could not perform additional debugging")

finally:
    # 11. CLOSE THE BROWSER
    if driver:
        print("\nSTEP 11: Closing the WebDriver.")
        driver.quit()
    
    print("\n--- Enhanced Scraper Finished ---")
    print(f"Check {output_dir} for processed files with TLID mapping!")