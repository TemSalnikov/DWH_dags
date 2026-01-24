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
    'pharmacy_inn',         
    'pharmacy_address', 
    'pharmacy_city', 
    'pharmacy_jur_person', 
    'supplier_name',     
    'product_code_ap',      
    'product_code_2005',
    'purchase_price',      
    'row_sum_up',           
    'row_sum_cond_price',   
    'retail',              
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
        val_a1 = str(df_raw.iloc[0, 0]).lower()
        year_match = re.search(r'20\d{2}', val_a1)
        year = int(year_match.group(0)) if year_match else datetime.now().year
        
        month = None
        for m_name, m_num in RUSSIAN_MONTHS.items():
            if m_name in val_a1:
                month = m_num
                break
        
        if month:
            start_date = datetime(year, month, 1)
            _, last_day = calendar.monthrange(year, month)
            end_date = datetime(year, month, last_day, 23, 59, 59)
            return start_date, end_date

    return start_date, end_date

def find_sheet_by_name_part(excel_file, part_name):
    for sheet in excel_file.sheet_names:
        if part_name.lower() in sheet.lower():
            return sheet
    return None

def find_pharmacy_metadata_rows(df_raw, header_row_idx):
    meta = {'code': None, 'jur': None, 'inn': None, 'addr': None, 'terr': None}
    
    for i in range(header_row_idx):
        row_str = " ".join([str(x).strip() for x in df_raw.iloc[i].values[:30]])
        if "Код ТТ" in row_str: meta['code'] = i
        if "Юр.лицо" in row_str: meta['jur'] = i
        if "ИНН" in row_str: meta['inn'] = i
        if "Субъект РФ" in row_str: meta['terr'] = i
        if "Торговая точка" in row_str or "аптека" in row_str.lower(): meta['addr'] = i

    if meta['code'] is None:
        for i in range(header_row_idx):
            match_count = 0
            for val in df_raw.iloc[i].values:
                if isinstance(val, str) and val.startswith('A0') and len(val) >= 5:
                    match_count += 1
            if match_count > 2:
                meta['code'] = i
                break
    return meta

def process_sheet_generic(df_raw, report_type, filename, name_pharm_chain):
    logger = LoggingMixin().log
    start_date, end_date = get_report_period(filename, df_raw)

    header_keywords = ["Товар"]
    data_col_keywords = []
    
    if report_type == 'Закупки':
        header_keywords.append("Поставщик")
        data_col_keywords = ["Закупки"] # Ищем "Закупки УП"
    elif report_type == 'Продажи':
        data_col_keywords = ["Продажи"] 
    elif report_type == 'Остатки':
        data_col_keywords = ["Сток", "Остатки"] 
    
    header_row_idx = None
    cols = {}
    
    scan_limit = min(50, len(df_raw))
    for i in range(scan_limit):
        row_vals = [str(x).strip() for x in df_raw.iloc[i].values]
        
        if all(k in row_vals for k in header_keywords):
            if report_type == 'Закупки' and "Поставщик" not in row_vals: continue
            
            if report_type == 'Продажи' and not any(("Продажи" in v or "Ср зак цена" in v) for v in row_vals): continue
            if report_type == 'Остатки' and not any(("Сток" in v or "Остатки" in v) for v in row_vals): continue

            header_row_idx = i
            
            for idx, val in enumerate(row_vals):
                val_clean = val.replace('\n', ' ')
                if val_clean == "Товар": cols['product'] = idx
                elif val_clean == "Поставщик": cols['supplier'] = idx
                elif "Усл цена" in val_clean and "Сумма" not in val_clean: cols['price'] = idx
                elif "Код АП" in val_clean: cols['code_ap'] = idx
                elif "Код Аптека2005" in val_clean: cols['code_2005'] = idx
                elif "Сумм УП" in val_clean: cols['sum_up'] = idx
                elif "Сумма Усл" in val_clean: cols['sum_usl'] = idx
                elif "Ср зак цена" in val_clean: cols['avg_price'] = idx
            break
            
    if header_row_idx is None:
        logger.warning(f"Шапка для отчета '{report_type}' не найдена на листе.")
        return []

    meta_rows = find_pharmacy_metadata_rows(df_raw, header_row_idx)
    pharmacy_map = {} 
    header_row_values = df_raw.iloc[header_row_idx]

    for col in range(len(header_row_values)):
        val_header = str(header_row_values[col]).strip()
        is_data_col = False
        if "Сумма" not in val_header:
            for kw in data_col_keywords:
                if kw in val_header and "УП" in val_header: # Обычно формат "XXX УП"
                    is_data_col = True
                    break
            if not is_data_col and report_type == 'Остатки' and "Остатки" in val_header:
                 is_data_col = True

        if is_data_col:
            p_id = None
            if meta_rows['code'] is not None:
                raw_code = str(df_raw.iloc[meta_rows['code'], col]).strip()
                if len(raw_code) > 2 and raw_code.lower() != 'nan': 
                    p_id = raw_code
            
            if p_id:
                p_jur = str(df_raw.iloc[meta_rows['jur'], col]).strip() if meta_rows['jur'] else ""
                p_inn = str(df_raw.iloc[meta_rows['inn'], col]).strip() if meta_rows['inn'] else ""
                p_addr = str(df_raw.iloc[meta_rows['addr'], col]).strip() if meta_rows['addr'] else ""
                p_terr = str(df_raw.iloc[meta_rows['terr'], col]).strip() if meta_rows['terr'] else ""
                
                if p_jur.lower() == 'nan': p_jur = ""
                if p_inn.lower() == 'nan': p_inn = ""
                
                pharmacy_map[col] = {
                    'id': p_id, 'jur': p_jur, 'inn': p_inn, 
                    'addr': p_addr.replace('\n', ' '), 'terr': p_terr
                }

    logger.info(f"Тип '{report_type}': Найдено {len(pharmacy_map)} аптек.")

    data_rows = []
    for i in range(header_row_idx + 1, len(df_raw)):
        row = df_raw.iloc[i]
        
        prod = str(row[cols.get('product')]).strip() if cols.get('product') is not None else ""
        if not prod or prod.lower() == 'nan' or "итог" in prod.lower(): continue

        supplier = str(row[cols.get('supplier')]).strip() if cols.get('supplier') is not None else ""
        retail = str(row[cols.get('price')]).strip() if cols.get('price') is not None else ""
        avg_price = str(row[cols.get('avg_price')]).strip() if cols.get('avg_price') is not None else ""
        
        code_ap = str(row[cols.get('code_ap')]).strip() if cols.get('code_ap') else ""
        code_2005 = str(row[cols.get('code_2005')]).strip() if cols.get('code_2005') else ""
        sum_up = str(row[cols.get('sum_up')]).strip() if cols.get('sum_up') else ""
        sum_usl = str(row[cols.get('sum_usl')]).strip() if cols.get('sum_usl') else ""

        for col_idx, pharm in pharmacy_map.items():
            qty_raw = row[col_idx]
            qty_str = str(qty_raw).strip().replace(',', '.').replace('\xa0', '').replace(' ', '')
            
            if qty_str.lower() in ['nan', '', 'none', '0', '0.0']: continue
            
            try:
                qty_float = float(qty_str)
                if qty_float == 0: continue
            except ValueError:
                continue

            row_dict = {
                'uuid_report': str(uuid.uuid4()),
                'report_date': start_date.strftime('%Y-%m-%d %H:%M:%S'),
                'product_name': prod,
                'pharmacy_name': pharm['addr'],
                'pharmacy_id': pharm['id'],
                'pharmacy_inn': pharm['inn'],
                'pharmacy_address': pharm['addr'],
                'pharmacy_city': pharm['terr'],
                'pharmacy_jur_person': pharm['jur'],
                'supplier_name': supplier if supplier else None,
                'retail': retail if retail else None,
                'quantity': f"{qty_float:g}",
                'product_code_ap': code_ap if code_ap else None,
                'product_code_2005': code_2005 if code_2005 else None,
                'purchase_price': avg_price if avg_price else None,
                'row_sum_up': sum_up if sum_up else None,
                'row_sum_cond_price': sum_usl if sum_usl else None,
                'name_pharm_chain': name_pharm_chain,
                'name_report': report_type, # 'Закупки', 'Продажи', 'Остатки'
                'start_date': start_date.strftime('%Y-%m-%d %H:%M:%S'),
                'end_date': end_date.strftime('%Y-%m-%d %H:%M:%S'),
                'processed_dttm': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            data_rows.append(row_dict)
            
    return data_rows

def extract_full_report_erkafarm(path, name_pharm_chain) -> dict:
    logger = LoggingMixin().log
    logger.info(f"Начат полный парсинг файла Эркафарм: {path}")
    
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
        ('Закупки', 'Закупки по аптекам'),
        ('Продажи', 'Продажи по аптекам'),
        ('Остатки', 'Остатки по')
    ]

    for report_type, sheet_keyword in tasks:
        sheet_name = find_sheet_by_name_part(xl, sheet_keyword)
        if sheet_name:
            logger.info(f"Обработка листа '{sheet_name}' как '{report_type}'...")
            try:
                df_raw = pd.read_excel(xl, sheet_name=sheet_name, header=None)
                rows = process_sheet_generic(df_raw, report_type, os.path.basename(path), name_pharm_chain)
                all_data.extend(rows)
                logger.info(f"Добавлено {len(rows)} строк из '{report_type}'.")
            except Exception as e:
                logger.error(f"Ошибка при обработке листа {sheet_name}: {e}")
        else:
            logger.warning(f"Лист для '{report_type}' не найден в файле.")

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
    if "Эркафарм" in name_pharm_chain or "Erkafarm" in name_pharm_chain:
        return extract_full_report_erkafarm(path, name_pharm_chain)
            
    return {}

if __name__ == "__main__":
    main_logger = LoggingMixin().log
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты_аптек\Эркафарм\Закуп-остатки-продажи\2025\07_2025.xlsx'
    
    if os.path.exists(test_file_path):
        main_logger.info("Запуск полного теста для Эркафарм...")
        result = extract_xls(test_file_path, 'ALL', 'Эркафарм')
        df = result.get('table_report')
        
        if df is not None and not df.empty:
            output_path = os.path.join(os.path.dirname(test_file_path), f"{os.path.splitext(os.path.basename(test_file_path))[0]}_FULL_result.csv")
            df.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
            
            print(f"Результат сохранен в: {output_path}")
            print(f"Всего строк: {len(df)}")
            print("\nРаспределение по типам отчетов:")
            print(df['name_report'].value_counts())
            
            print("\nПример данных (первые 3 строки):")
            print(df[['name_report', 'product_name', 'purchase_price', 'quantity']].head(3))
        else:
            print("Результат пуст.")
    else:
        print(f"Файл не найден: {test_file_path}")