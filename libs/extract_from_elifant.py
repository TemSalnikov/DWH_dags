import pandas as pd
import re
import uuid
from datetime import datetime
import os
from airflow.utils.log.logging_mixin import LoggingMixin
import calendar

FINAL_COLUMNS = [
    'uuid_report',
    'pharmacy_name',
    'product_name',
    'barcode',
    'supplier_name',
    'quantity_in',
    'quantity_out',
    'quantity_stock',
    'name_report',
    'name_pharm_chain',
    'start_date',
    'end_date',
    'processed_dttm'
]

def extract_elifant_report(path, name_report, name_pharm_chain) -> dict:
    loger = LoggingMixin().log
    loger.info(f"Начат парсинг иерархического отчета Элифант (Smart Search): {path}")
    
    try:
        df_raw = pd.read_excel(path, header=None)
    except Exception as e:
        loger.error(f"Ошибка чтения Excel файла: {e}")
        return {}

    start_date = None
    end_date = None
    
    filename = os.path.basename(path)
    match = re.search(r'(\d{2})[._-](\d{4})', filename)
    if match:
        month = int(match.group(1))
        year = int(match.group(2))
        start_date = datetime(year, month, 1)
        _, last_day = calendar.monthrange(year, month)
        end_date = datetime(year, month, last_day, 23, 59, 59)

    if not start_date:
        for i in range(min(15, len(df_raw))):
            row_str = " ".join([str(x) for x in df_raw.iloc[i].values if pd.notna(x)])
            
            if "Начало периода" in row_str or "Дата начала" in row_str:
                match = re.search(r'(\d{2}\.\d{2}\.\d{4}\s\d{2}:\d{2}:\d{2})', row_str)
                if match: start_date = datetime.strptime(match.group(1), "%d.%m.%Y %H:%M:%S")
                    
            if "Конец периода" in row_str or "Дата конец" in row_str:
                match = re.search(r'(\d{2}\.\d{2}\.\d{4}\s\d{2}:\d{2}:\d{2})', row_str)
                if match: end_date = datetime.strptime(match.group(1), "%d.%m.%Y %H:%M:%S")

    if not start_date:
        start_date = datetime.now().replace(day=1, hour=0, minute=0, second=0)
    if not end_date:
        end_date = datetime.now().replace(hour=23, minute=59, second=59)

    loger.info(f"Период: {start_date} - {end_date}")
    
    col_map = {
        'product': None, 
        'barcode': None, 
        'supplier': None,
        'income': None,
        'outcome': None,
        'stock': None
    }
    
    header_search_limit = min(20, len(df_raw))
    max_header_row_idx = 0
    
    for r in range(header_search_limit):
        row_vals = df_raw.iloc[r].astype(str).str.strip()
        for c, val in enumerate(row_vals):
            val_lower = val.lower()
            

            if 'товар' == val_lower:
                col_map['product'] = c
                max_header_row_idx = max(max_header_row_idx, r)
            
            elif 'шк' in val_lower.split() or val_lower == 'шк':
                col_map['barcode'] = c
                max_header_row_idx = max(max_header_row_idx, r)
            elif 'характеристик' in val_lower and col_map['barcode'] is None:
                col_map['barcode'] = c
                max_header_row_idx = max(max_header_row_idx, r)

            elif 'контрагент' in val_lower:
                col_map['supplier'] = c
                max_header_row_idx = max(max_header_row_idx, r)

            elif 'приход' in val_lower:
                col_map['income'] = c
                max_header_row_idx = max(max_header_row_idx, r)
                
            elif 'расход' in val_lower:
                col_map['outcome'] = c
                max_header_row_idx = max(max_header_row_idx, r)
                
            elif 'конечный остаток' in val_lower or 'остаток' == val_lower:
                col_map['stock'] = c
                max_header_row_idx = max(max_header_row_idx, r)

    if col_map['stock'] is not None and col_map['supplier'] is not None:
        if col_map['stock'] == col_map['supplier']:
             loger.warning("Обнаружено наложение 'Конечный остаток' и 'Контрагент'. Ищем корректную колонку.")
             col_map['stock'] = None
             for r in range(header_search_limit):
                row_vals = df_raw.iloc[r].astype(str).str.strip()
                for c, val in enumerate(row_vals):
                    if ('конечный остаток' in val.lower() or 'остаток' == val.lower()) and c > col_map['supplier']:
                        col_map['stock'] = c
                        break
    supp_idx = col_map.get('supplier')
    if supp_idx is not None:
        if col_map['income'] is None: col_map['income'] = supp_idx + 1
        if col_map['outcome'] is None: col_map['outcome'] = supp_idx + 2
        if col_map['stock'] is None: col_map['stock'] = supp_idx + 3
    else:
        if col_map['product'] is None: col_map['product'] = 0
        supp_idx = 2
    
    loger.info(f"Найденные колонки: {col_map}")

    if col_map['product'] is None:
        loger.error("Не удалось найти колонку 'Товар'. Структура файла неизвестна.")
        return {}

    data_rows = []
    current_pharmacy = None
    
    start_data_row = max_header_row_idx + 1
    
    idx_prod = col_map['product']
    idx_bc = col_map.get('barcode', 1)
    idx_supp = col_map.get('supplier', 2)
    idx_in = col_map.get('income')
    idx_out = col_map.get('outcome')
    idx_st = col_map.get('stock')

    for i in range(start_data_row, len(df_raw)):
        row = df_raw.iloc[i]
        
        val_prod = str(row[idx_prod]).strip() if pd.notna(row[idx_prod]) else ""
        val_bc = str(row[idx_bc]).strip() if (idx_bc is not None and idx_bc < len(row) and pd.notna(row[idx_bc])) else ""
        val_supp = str(row[idx_supp]).strip() if (idx_supp is not None and idx_supp < len(row) and pd.notna(row[idx_supp])) else ""
        
        if not val_prod and not val_bc and not val_supp:
            continue
        
        is_pharmacy_row = False
        if val_prod and not val_supp:
            if not val_bc or len(val_bc) < 5 or not val_bc.isdigit():
                is_pharmacy_row = True
        
        if is_pharmacy_row:
            current_pharmacy = val_prod
            continue 

        if current_pharmacy:
            def get_float(idx):
                if idx is None or idx >= len(row): return None
                val = row[idx]
                try:
                    if pd.isna(val): return None
                    s = str(val).replace(',', '.').replace('\xa0', '').strip()
                    if not s or s.lower() == 'nan': return None
                    return float(s)
                except:
                    return None

            q_in = get_float(idx_in)
            q_out = get_float(idx_out)
            q_st = get_float(idx_st)

            has_movements = (q_in != 0 or q_out != 0 or q_st != 0)
            is_valid_product = (val_prod and (val_bc or val_supp))
            
            if "итого" in val_prod.lower():
                continue

            if has_movements or is_valid_product:
                row_dict = {
                    'uuid_report': str(uuid.uuid4()),
                    'pharmacy_name': current_pharmacy,
                    'product_name': val_prod,
                    'barcode': val_bc,
                    'supplier_name': val_supp,
                    'quantity_in': q_in,
                    'quantity_out': q_out,
                    'quantity_stock': q_st,
                    'name_pharm_chain': name_pharm_chain,
                    'name_report': name_report
                }
                data_rows.append(row_dict)

    df_result = pd.DataFrame(data_rows)
    
    if df_result.empty:
        loger.warning("Внимание: отчет Элифант не содержит строк с данными.")
    else:
        fmt = '%Y-%m-%d %H:%M:%S'
        df_result['start_date'] = start_date.strftime(fmt)
        df_result['end_date'] = end_date.strftime(fmt)
        df_result['processed_dttm'] = datetime.now().strftime(fmt)
        
        for col in FINAL_COLUMNS:
            if col not in df_result.columns:
                df_result[col] = None
        df_result = df_result[FINAL_COLUMNS]
        loger.info(f"Успешно обработано {len(df_result)} строк.")

    return {
        'table_report': df_result
    }




def extract_xls(path, name_report, name_pharm_chain) -> dict:
    if 'Элифант' in name_pharm_chain or 'Elifant' in name_pharm_chain:
        return extract_elifant_report(path, name_report, name_pharm_chain)
    return {}

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    test_file = r'c:\Users\nmankov\Desktop\отчеты_аптек\Элифант\Закупки, продажи, остатки\2025\06_2025.xls'
    if os.path.exists(test_file):
        res = extract_elifant_report(test_file, "Закуп_Продажи_Остатки", "Элифант")
        df = res.get('table_report')
        if df is not None and not df.empty:
            output_path = os.path.join(os.path.dirname(test_file), f"{os.path.splitext(os.path.basename(test_file))[0]}_result.csv")
            df.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
            print(f"Результат сохранен в: {output_path}")
            print(df[['pharmacy_name', 'product_name', 'supplier_name', 'quantity_stock']].head(10))
            print(f"Total rows: {len(df)}")