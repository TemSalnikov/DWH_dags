import pandas as pd
import re
import uuid
from datetime import datetime
import os
from airflow.utils.log.logging_mixin import LoggingMixin
import calendar

FINAL_COLUMNS = [
    'uuid_report',  
    'product_name', 
    'pharmacy_name', 
    'pharmacy_id', 
    'supplier_name',     
    'quantity', 
    'name_pharm_chain', 
    'name_report',         
    'start_date', 
    'end_date', 
    'processed_dttm'
]

RUSSIAN_MONTHS = {
    'январь': 1, 'января': 1, 'февраль': 2, 'февраля': 2,
    'март': 3, 'марта': 3, 'апрель': 4, 'апреля': 4,
    'май': 5, 'мая': 5, 'июнь': 6, 'июня': 6,
    'июль': 7, 'июля': 7, 'август': 8, 'августа': 8,
    'сентябрь': 9, 'сентября': 9, 'октябрь': 10, 'октября': 10,
    'ноябрь': 11, 'ноября': 11, 'декабрь': 12, 'декабря': 12
}

def get_report_period(filename, df_raw=None):
    start_date = datetime.now().replace(day=1, hour=0, minute=0, second=0)
    end_date = datetime.now()
    
    match = re.search(r'(\d{2})_(\d{4})', filename)
    if match:
        month = int(match.group(1))
        year = int(match.group(2))
        start_date = datetime(year, month, 1)
        _, last_day = calendar.monthrange(year, month)
        end_date = datetime(year, month, last_day, 23, 59, 59)
        return start_date, end_date

    if df_raw is not None and not df_raw.empty:
        found_date_str = ""
        for i in range(min(15, len(df_raw))):
            val = str(df_raw.iloc[i, 0]).lower()
            if "дата" in val or any(m in val for m in RUSSIAN_MONTHS):
                found_date_str = val
                break
        
        if found_date_str:
            year_match = re.search(r'20\d{2}', found_date_str)
            year = int(year_match.group(0)) if year_match else datetime.now().year
            
            month = None
            for m_name, m_num in RUSSIAN_MONTHS.items():
                if m_name in found_date_str:
                    month = m_num
                    break
            
            if month:
                start_date = datetime(year, month, 1)
                _, last_day = calendar.monthrange(year, month)
                end_date = datetime(year, month, last_day, 23, 59, 59)

    return start_date, end_date

def find_sheet_by_name_part(excel_file, part_name):
    for sheet in excel_file.sheet_names:
        if part_name.lower() in sheet.lower():
            return sheet
    return None

def process_sheet_gubernskie(df_raw, report_type, filename, name_pharm_chain):
    logger = LoggingMixin().log
    start_date, end_date = get_report_period(filename, df_raw)

    product_col_keywords = ["НАИМЕНОВАНИЕ ЛС", "НАЗВАНИЯ СТРОК"]
    
    header_row_idx = None
    
    scan_limit = min(30, len(df_raw))
    for i in range(scan_limit):
        row_vals = [str(x).strip().upper() for x in df_raw.iloc[i].values]
        if any(kw in v for v in row_vals for kw in product_col_keywords):
            header_row_idx = i
            break
            
    if header_row_idx is None:
        logger.warning(f"Шапка таблицы не найдена на листе для {report_type}.")
        return []

    cols = {'product': None, 'supplier': None}
    pharmacy_map = {}
    
    row_headers = df_raw.iloc[header_row_idx]
    row_pharm_names = df_raw.iloc[header_row_idx - 1] if header_row_idx > 0 else None
    
    for idx in range(len(row_headers)):
        val = str(row_headers[idx]).strip()
        val_upper = val.upper()
        
        is_product_col = any(kw in val_upper for kw in product_col_keywords)
        
        if is_product_col:
            cols['product'] = idx
        elif "ПОСТАВЩИК" in val_upper:
            cols['supplier'] = idx
        else:
            
            p_name = ""
            if row_pharm_names is not None:
                p_name_raw = str(row_pharm_names[idx]).strip()
                if p_name_raw.lower() != 'nan':
                    p_name = p_name_raw.replace('\n', ' ')

            if "ИТОГ" in val_upper or (p_name and "ИТОГ" in p_name.upper()):
                continue
            
            p_id = val if val and val.replace('.','').isdigit() else None
            
            if p_id:
                if not p_name: 
                    p_name = f"Аптека {p_id}"
                
                pharmacy_map[idx] = {
                    'id': p_id,
                    'name': p_name,
                    'address': p_name
                }

    logger.info(f"Тип '{report_type}': Найдено {len(pharmacy_map)} аптек/колонок.")

    data_rows = []
    
    for i in range(header_row_idx + 1, len(df_raw)):
        row = df_raw.iloc[i]
        
        prod = str(row[cols['product']]).strip() if cols['product'] is not None else ""
        
        if not prod or prod.lower() == 'nan' or "итог" in prod.lower(): 
            continue

        supplier = None
        if cols['supplier'] is not None:
            raw_supp = str(row[cols['supplier']]).strip()
            if raw_supp and raw_supp.lower() != 'nan':
                supplier = raw_supp
        
        for col_idx, pharm_info in pharmacy_map.items():
            qty_raw = row[col_idx]
            qty_str = str(qty_raw).strip().replace(',', '.').replace('\xa0', '').replace(' ', '')
            
            if qty_str.lower() in ['nan', '', 'none', '0', '0.0']: 
                continue
            
            try:
                qty_float = float(qty_str)
                if qty_float == 0: continue
            except ValueError:
                continue

            row_dict = {
                'uuid_report': str(uuid.uuid4()),
                'product_name': prod,
                'pharmacy_name': pharm_info['name'],
                'pharmacy_id': pharm_info['id'],
                'supplier_name': supplier, 
                'quantity': f"{qty_float:g}",
                'name_pharm_chain': name_pharm_chain,
                'name_report': report_type, 
                'start_date': start_date.strftime('%Y-%m-%d %H:%M:%S'),
                'end_date': end_date.strftime('%Y-%m-%d %H:%M:%S'),
                'processed_dttm': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            data_rows.append(row_dict)
            
    return data_rows

def extract_full_report_gubernskie(path, name_pharm_chain) -> dict:
    logger = LoggingMixin().log
    logger.info(f"Начат парсинг файла Губернские аптеки: {path}")
    
    if not os.path.exists(path):
        logger.error(f"Файл не найден: {path}")
        return {}

    try:
        xl = pd.ExcelFile(path, engine='openpyxl')
    except Exception as e:
        logger.error(f"Ошибка открытия Excel: {e}")
        return {}
    
    all_data = []

    tasks = [
        ('Закупки', 'Закуп'), 
        ('Продажи', 'Продажи'),
        ('Остатки', 'Остатки')
    ]

    for report_type, sheet_keyword in tasks:
        sheet_name = find_sheet_by_name_part(xl, sheet_keyword)
        if sheet_name:
            logger.info(f"Обработка листа '{sheet_name}' как '{report_type}'...")
            try:
                df_raw = pd.read_excel(xl, sheet_name=sheet_name, header=None)
                
                rows = process_sheet_gubernskie(df_raw, report_type, os.path.basename(path), name_pharm_chain)
                all_data.extend(rows)
                logger.info(f"Добавлено {len(rows)} строк из '{report_type}'.")
                
            except Exception as e:
                logger.error(f"Ошибка при обработке листа {sheet_name}: {e}")
                import traceback
                traceback.print_exc()
        else:
            logger.warning(f"Лист для '{report_type}' (ключ: {sheet_keyword}) не найден в файле.")

    df_result = pd.DataFrame(all_data)
    
    if not df_result.empty:
        for col in FINAL_COLUMNS:
            if col not in df_result.columns:
                df_result[col] = None
        df_result = df_result[FINAL_COLUMNS]
        logger.info(f"Итого собрано {len(df_result)} строк.")
    else:
        logger.warning("Итоговый результат пуст.")

    return {'table_report': df_result}

def extract_xls(path, name_report, name_pharm_chain) -> dict:
    if "Губернские" in name_pharm_chain:
        return extract_full_report_gubernskie(path, name_pharm_chain)
            
    return {}

if __name__ == "__main__":
    main_logger = LoggingMixin().log
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты_аптек\Губернские аптеки\03_2025.xlsx'
    
    if os.path.exists(test_file_path):
        main_logger.info("Запуск теста для Губернские аптеки...")
        result = extract_xls(test_file_path, 'ALL', 'Губернские аптеки')
        df = result.get('table_report')
        
        if df is not None and not df.empty:
            output_csv = os.path.join(os.path.dirname(test_file_path), f"result_gubernskie_{datetime.now().strftime('%H%M%S')}.csv")
            df.to_csv(output_csv, sep=';', index=False, encoding='utf-8-sig')
            
            print(f"Результат сохранен в: {output_csv}")
            print(f"Всего строк: {len(df)}")
            print("\nРаспределение по типам отчетов:")
            print(df['name_report'].value_counts())
            
            print("\nПример данных (первые 3 строки):")
            print(df[['name_report', 'product_name', 'pharmacy_id', 'quantity']].head(3))
        else:
            print("Результат пуст.")
    else:
        print(f"Файл не найден: {test_file_path}")