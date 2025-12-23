import pandas as pd
import re
import uuid
from datetime import datetime
import os
from airflow.utils.log.logging_mixin import LoggingMixin
import warnings
import calendar

try:
    import openpyxl
except ImportError:
    openpyxl = None

FINAL_COLUMNS = [
    'uuid_report',
    'report_date',
    'doc_number',
    'product_name',
    'pharmacy_name',
    'pharmacy_id',
    'pharmacy_address',
    'pharmacy_city',
    'pharmacy_territory',
    'pharmacy_jur_person',
    'supplier_name',
    'retail',
    'quantity',
    'name_pharm_chain',
    'name_report',
    'start_date',
    'end_date',
    'processed_dttm'
]

def extract_custom_farmlend(path, name_report, name_pharm_chain) -> dict:
    logger = LoggingMixin().log
    logger.info(f"Начат парсинг отчета Фармленд (Закупки): {path}")

    if "(1)" not in os.path.basename(path):
        logger.warning(f"Файл {path} пропущен: в названии нет '(1)'.")
        return {}

    try:
        df_raw = pd.read_excel(path, header=None, engine='calamine')
    except Exception as e:
        logger.error(f"Ошибка чтения через calamine: {e}")
        return {}

    filename = os.path.basename(path)
    match = re.search(r'(\d{2})_(\d{4})\s*\(1\)', filename)
    
    if match:
        month = int(match.group(1))
        year = int(match.group(2))
        start_date = datetime(year, month, 1)
        _, last_day = calendar.monthrange(year, month)
        end_date = datetime(year, month, last_day, 23, 59, 59)
    else:
        logger.warning("Не удалось извлечь дату из имени файла (ожидался формат 'MM_YYYY (1)').")
        start_date = datetime.now().replace(day=1, hour=0, minute=0, second=0)
        end_date = datetime.now()

    logger.info(f"Определен период: {start_date} - {end_date}")
    
    scan_limit = min(30, len(df_raw))

    header_row_idx = None
    col_indices = {}
    qty_col_idx = None

    for i in range(scan_limit):
        row_vals = df_raw.iloc[i].astype(str).values
        for idx, val in enumerate(row_vals):
            if val and "Количество" in val:
                qty_col_idx = idx
                break
        if qty_col_idx is not None:
            break

    for i, row in df_raw.iterrows():
        row_list = [str(x).strip() for x in row.values]
        if "СНП" in row_list and "Поставщик" in row_list:
            header_row_idx = i
            for idx, val in enumerate(row_list):
                if val:
                    col_indices[val] = idx
            break
    
    if header_row_idx is None:
        logger.error("Не найдена строка заголовка таблицы (ожидались колонки 'СНП', 'Поставщик')")
        return {}

    if qty_col_idx is None:
        if "Розница" in col_indices:
            qty_col_idx = col_indices["Розница"] + 1
        else:
            qty_col_idx = len(df_raw.columns) - 1

    idx_date = col_indices.get('Дата') 
    idx_doc_num = col_indices.get('Номер')
    idx_product = col_indices.get('СНП') 
    idx_supplier = col_indices.get('Поставщик')
    idx_retail = col_indices.get('Розница')
    if idx_product is None or idx_date is None:
        logger.error("Критические колонки 'СНП' или 'Дата' не найдены.")
        return {}

    data_rows = []
    current_pharmacy = None
    
    for i in range(header_row_idx + 1, len(df_raw)):
        row = df_raw.iloc[i]
        
        val_0 = str(row[0]).strip() if pd.notna(row[0]) else ""
        val_date_cell = str(row[idx_date]).strip() if (idx_date < len(row) and pd.notna(row[idx_date])) else ""
        
        is_transaction_row = False
        doc_date_dt = None
        
        if re.match(r'\d{2}\.\d{2}\.\d{4}', val_date_cell):
            try:
                doc_date_dt = datetime.strptime(val_date_cell, "%d.%m.%Y %H:%M:%S")
                is_transaction_row = True
            except ValueError:
                try:
                    doc_date_dt = datetime.strptime(val_date_cell, "%d.%m.%Y")
                    is_transaction_row = True
                except ValueError:
                    pass

        if is_transaction_row:
            val_product = str(row[idx_product]).strip() if (idx_product < len(row) and pd.notna(row[idx_product])) else ""
            val_doc_num = str(row[idx_doc_num]).strip() if (idx_doc_num and idx_doc_num < len(row) and pd.notna(row[idx_doc_num])) else ""
            val_supplier = str(row[idx_supplier]).strip() if (idx_supplier and idx_supplier < len(row) and pd.notna(row[idx_supplier])) else ""
            val_retail = str(row[idx_retail]).strip() if (idx_retail is not None and idx_retail < len(row) and pd.notna(row[idx_retail])) else ""
            val_qty = str(row[qty_col_idx]).strip() if (qty_col_idx < len(row) and pd.notna(row[qty_col_idx])) else "0"

            row_dict = {
                'uuid_report': str(uuid.uuid4()),
                'report_date': doc_date_dt, 
                'pharmacy_name': current_pharmacy, 
                'product_name': val_product,
                'doc_number': val_doc_num,
                'supplier_name': val_supplier,
                'retail': val_retail,
                'quantity': val_qty,
                'name_pharm_chain': name_pharm_chain,
                'name_report': name_report
            }
            data_rows.append(row_dict)
            
        elif val_0:
            clean_val = val_0.lower()
            if "аптека" in clean_val and "итого" not in clean_val and "организация" not in clean_val:
                current_pharmacy = val_0
            elif val_0 not in ["Дата", "Итого", "Организация"] and not re.match(r'^\d', val_0):
                 if len(val_0) > 5:
                    current_pharmacy = val_0

    df_result = pd.DataFrame(data_rows)
    
    if df_result.empty:
        logger.warning("Внимание: отчет пуст или структура не распознана.")
    else:
        df_result['report_date'] = pd.to_datetime(df_result['report_date']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df_result['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df_result['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df_result['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for col in FINAL_COLUMNS:
            if col not in df_result.columns:
                df_result[col] = None
        df_result = df_result[FINAL_COLUMNS]
        logger.info(f"Успешно обработано {len(df_result)} строк.")

    return {
        'table_report': df_result
    }

def extract_remain_sales_farmlend(path, name_report, name_pharm_chain) -> dict:
    logger = LoggingMixin().log
    logger.info(f"Начат парсинг отчета Фармленд ({name_report}): {path}")

    if "(1)" in os.path.basename(path):
        logger.warning(f"Файл {path} пропущен: в названии есть '(1)' (ожидается файл без суффикса для {name_report}).")
        return {}

    try:
        df_raw = pd.read_excel(path, header=None, engine='calamine')
    except Exception as e:
        logger.error(f"Ошибка чтения: {e}")
        return {}

    # Дата
    filename = os.path.basename(path)
    match = re.search(r'(\d{2})_(\d{4})', filename)
    if match:
        month = int(match.group(1))
        year = int(match.group(2))
        start_date = datetime(year, month, 1)
        _, last_day = calendar.monthrange(year, month)
        end_date = datetime(year, month, last_day, 23, 59, 59)
    else:
        logger.warning("Не удалось извлечь дату из имени файла.")
        start_date = datetime.now().replace(day=1, hour=0, minute=0, second=0)
        end_date = datetime.now()

    product_col_idx = None
    data_start_row = None
    anchor_row_idx = None 

    scan_limit = min(30, len(df_raw))
    for i in range(scan_limit):
        row_vals = [str(val) for val in df_raw.iloc[i].values]
        
        if "DM - NAME" in row_vals:
            anchor_row_idx = i
            data_start_row = i + 1
            for idx, val in enumerate(row_vals):
                if val == "DM - NAME":
                    product_col_idx = idx
            break
    
    if product_col_idx is None or anchor_row_idx is None:
        logger.error("Не удалось найти структуру таблицы (якорь 'DM - NAME' не найден).")
        return {}

    logger.info(f"Структура найдена. Строка с ID аптек: {anchor_row_idx}, Колонка товара: {product_col_idx}")

    pharmacy_map = {} 

    row_ids  = df_raw.iloc[anchor_row_idx]
    row_addr = df_raw.iloc[anchor_row_idx - 1] if anchor_row_idx >= 1 else None
    row_city = df_raw.iloc[anchor_row_idx - 2] if anchor_row_idx >= 2 else None
    row_terr = df_raw.iloc[anchor_row_idx - 3] if anchor_row_idx >= 3 else None
    row_jur  = df_raw.iloc[anchor_row_idx - 4] if anchor_row_idx >= 4 else None

    for col in range(product_col_idx + 1, len(df_raw.columns)):
        val_id = row_ids[col]
        
        raw_id = str(val_id).strip()
        
        if raw_id.lower() == 'nan' or raw_id.lower() == 'none' or raw_id == '':
            continue
            
        if raw_id.endswith('.0'): 
            raw_id = raw_id[:-2]

        if not raw_id or "итог" in raw_id.lower() or "dm - name" in raw_id.lower():
            continue

        def safe_get(row_obj, col_idx):
            if row_obj is None: return ""
            val = row_obj[col_idx]
            s = str(val).strip()
            if s.lower() == 'nan' or s.lower() == 'none': return ""
            return s

        addr_val = safe_get(row_addr, col)
        city_val = safe_get(row_city, col)
        terr_val = safe_get(row_terr, col)
        jur_val  = safe_get(row_jur, col)

        pharmacy_map[col] = {
            'id': raw_id,
            'address': addr_val,
            'city': city_val,
            'territory': terr_val,
            'jur_person': jur_val
        }

    data_rows = []
    
    for i in range(data_start_row, len(df_raw)):
        row = df_raw.iloc[i]
        
        prod_name = str(row[product_col_idx]).strip()
        if prod_name.lower() == 'nan': prod_name = ""

        if not prod_name or "общий итог" in prod_name.lower():
            continue

        for col_idx, pharm_info in pharmacy_map.items():
            val = row[col_idx]
            val_str = str(val).strip()

            if not val_str or val_str.lower() == 'nan':
                continue

            val_clean = val_str.replace(',', '.').replace('\xa0', '').replace(' ', '')
            
            if val_clean.endswith('.0'): 
                val_clean = val_clean[:-2]
            
            if val_clean == '0' or val_clean == '0.0' or val_clean == '':
                continue
            
            row_dict = {
                'uuid_report': str(uuid.uuid4()),                
                'product_name': prod_name,
                'quantity': val_clean,
                
                'pharmacy_id': pharm_info['id'],
                'pharmacy_address': pharm_info['address'],
                'pharmacy_city': pharm_info['city'],
                'pharmacy_territory': pharm_info['territory'],
                'pharmacy_jur_person': pharm_info['jur_person'],
                'name_pharm_chain': name_pharm_chain,
                'name_report': name_report
            }
            data_rows.append(row_dict)

    df_result = pd.DataFrame(data_rows)

    if not df_result.empty:
        df_result['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df_result['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df_result['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for col in FINAL_COLUMNS:
            if col not in df_result.columns:
                df_result[col] = None
        df_result = df_result[FINAL_COLUMNS]
        logger.info(f"Успешно обработано {len(df_result)} строк ({name_report}).")
    else:
        logger.warning("Результат пуст.")

    return {'table_report': df_result}

# --- Диспетчер ---
def extract_xls(path, name_report, name_pharm_chain) -> dict:
    if "Фармленд" in name_pharm_chain or "Farmlend" in name_pharm_chain:
        if name_report in ['Закупки', 'Закуп']:
            return extract_custom_farmlend(path, name_report, name_pharm_chain)
        elif name_report in ['Остатки', 'Продажи']:
            return extract_remain_sales_farmlend(path, name_report, name_pharm_chain)
    return {}

if __name__ == "__main__":
    main_logger = LoggingMixin().log
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты_аптек\Фармленд\Закуп\2025\07_2025 (1).xlsx' 
    
    if os.path.exists(test_file_path):
        main_logger.info("Запуск теста...")
        result = extract_xls(test_file_path, 'Закупки', 'Фармленд')
        df = result.get('table_report')
        if df is not None and not df.empty:
            output_path = os.path.join(os.path.dirname(test_file_path), f"{os.path.splitext(os.path.basename(test_file_path))[0]}_result.csv")
            df.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
            print(f"Результат сохранен в: {output_path}")
            print("Первые 5 строк результата:")
            print(df.head())
            print("Список колонок:", df.columns.tolist())
            print(f"Всего строк: {len(df)}")
        else:
            print("Результат пуст.")
    else:
        print(f"Файл не найден: {test_file_path}")