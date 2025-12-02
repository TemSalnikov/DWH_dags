import pandas as pd
import uuid
import os
import openpyxl
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

def get_dates_from_filename(path):
    try:
        filename_no_ext = os.path.basename(path).split('.')[0]
        if ' ' in filename_no_ext:
            filename_no_ext = filename_no_ext.split(' ')[0]
            
        start_date = datetime.strptime(f"01.{filename_no_ext.replace('_', '.')}", "%d.%m.%Y")
        end_date = start_date + pd.offsets.MonthEnd(1)
        return start_date, end_date
    except Exception as e:
        raise ValueError(f"Имя файла должно быть в формате ММ_ГГГГ.xlsx. Ошибка: {e}")

# --- парсер закупок с поставщиками ---
def parse_purchases_with_openpyxl(path):
    """
    Парсит закупки с учетом иерархии (Продукт -> Поставщик).
    Автоматически выбирает колонки:
    - Если есть филиалы (2025) -> игнорирует "Общий итог", берет филиалы.
    - Если нет филиалов (2024) -> берет "Общий итог".
    """
    wb = openpyxl.load_workbook(path, data_only=True)
    sheet = wb.active
    
    data_rows = []
    
    # Поиск заголовка
    header_row_idx = None
    headers = []
    rows = list(sheet.iter_rows(values_only=False))
    
    for i, row in enumerate(rows):
        row_values = [str(cell.value).strip() if cell.value else "" for cell in row]
        if any("Названия строк" in v for v in row_values):
            header_row_idx = i
            for col_idx, cell in enumerate(row):
                val = str(cell.value).strip() if cell.value else f"Col_{col_idx}"
                headers.append({'idx': col_idx, 'name': val})
            break
            
    if header_row_idx is None:
        raise ValueError("В файле Закупок не найдена строка 'Названия строк'")

    name_col_idx = headers[0]['idx']
    
    value_headers = [h for h in headers if h['idx'] != name_col_idx]
    
    has_branches = any("Общий итог" not in h['name'] for h in value_headers)
    
    current_product = None
    
    for row in rows[header_row_idx + 1:]:
        if not row: continue
        
        cell_name = row[name_col_idx]
        val_name = str(cell_name.value).strip() if cell_name.value else ""
        
        if not val_name or "Названия строк" in val_name or val_name == "Общий итог":
            continue
            
        indent = int(cell_name.alignment.indent) if cell_name.alignment.indent else 0
        
        if indent == 0:
            current_product = val_name
        else:
            if current_product:
                supplier_name = val_name
                
                for h in value_headers:
                    col_name = h['name']
                    
                    if has_branches and "Общий итог" in col_name:
                        continue
                        
                    # Получаем значение
                    cell_val = row[h['idx']].value
                    qty = 0
                    
                    if isinstance(cell_val, (int, float)):
                        qty = cell_val
                    elif cell_val:
                        try:
                            clean_val = str(cell_val).replace(',', '.').replace('\xa0', '').strip()
                            qty = float(clean_val)
                        except:
                            qty = 0
                            
                    if qty != 0:
                        data_rows.append({
                            'product': current_product,
                            'supplier': supplier_name,
                            'branch_raw': col_name,
                            'sale_quantity': qty
                        })

    return pd.DataFrame(data_rows)

# --- парсер для продаж и остатков ---
def parse_standard_pandas(path):
    df_raw = pd.read_excel(path, header=None)
    
    header_row_idx = None
    for idx, row in df_raw.iterrows():
        if row.astype(str).str.contains("Названия строк", na=False).any():
            header_row_idx = idx
            break
    if header_row_idx is None: header_row_idx = 0

    headers = df_raw.iloc[header_row_idx].values
    df = df_raw.iloc[header_row_idx + 1:].copy()
    df.columns = headers
    
    col_names = list(df.columns)
    prod_col_idx = -1
    for i, c in enumerate(col_names):
        if "Названия строк" in str(c):
            prod_col_idx = i
            break
    if prod_col_idx == -1: prod_col_idx = 0
    new_cols = col_names.copy()
    new_cols[prod_col_idx] = 'product'
    df.columns = new_cols
    
    df.dropna(subset=['product'], inplace=True)
    df = df[df['product'] != 'Общий итог']

    total_col_name = None
    for c in df.columns:
        if "Общий итог" in str(c):
            total_col_name = c
            break
            
    value_vars = [col for col in df.columns if col != 'product']
    if len(value_vars) > 1 and total_col_name in value_vars:
        value_vars.remove(total_col_name)
    
    df_melted = df.melt(
        id_vars=['product'], 
        value_vars=value_vars,
        var_name='branch_raw', 
        value_name='sale_quantity'
    )
    df_melted['supplier'] = None
    return df_melted

def parse_branch_info(full_name):
    if str(full_name).strip() == "Общий итог":
        return None, None, None
    try:
        s_name = str(full_name)
        if ',' in s_name:
            parts = s_name.split(',')
            branch_part = parts[0].strip()
            street_part = parts[1].strip() if len(parts) > 1 else None
            city_part = branch_part.split(' ')[0]
            return branch_part, city_part, street_part
        else:
            return s_name, None, None
    except:
        return str(full_name), None, None

def extract_all_xls(path='', name_report='Продажи', name_pharm_chain='Ваш доктор') -> dict:
    loger = LoggingMixin().log
    
    try:
        start_date, end_date = get_dates_from_filename(path)
        loger.info(f'Период отчета: {start_date.date()} - {end_date.date()}')
        
        # выбор логики парсера
        if 'Закупки' in name_report or 'Закупки' in path:
            loger.info("Режим: ЗАКУПКИ (поставщики с отступами)")
            df_report = parse_purchases_with_openpyxl(path)
        else:
            loger.info("Режим: СТАНДАРТНЫЙ (Продажи/Остатки)")
            df_report = parse_standard_pandas(path)

        if df_report.empty:
            loger.warning("Сформирован пустой DataFrame")

        # Обработка адресов
        if 'branch_raw' in df_report.columns:
            branch_data = df_report['branch_raw'].apply(parse_branch_info)
            df_report['branch_name'] = [x[0] for x in branch_data]
            df_report['city'] = [x[1] for x in branch_data]
            df_report['street'] = [x[2] for x in branch_data]
            df_report.drop(columns=['branch_raw'], inplace=True)
        else:
            df_report['branch_name'] = None
            df_report['city'] = None
            df_report['street'] = None

        # Чистка чисел
        df_report['sale_quantity'] = pd.to_numeric(df_report['sale_quantity'], errors='coerce').fillna(0)
        df_report = df_report[df_report['sale_quantity'] != 0].copy()
        df_report['sale_quantity'] = df_report['sale_quantity'].astype(str)
        
        # Технические поля
        count_rows = len(df_report)
        df_report['uuid_report'] = [str(uuid.uuid4()) for _ in range(count_rows)]
        df_report['name_report'] = name_report
        df_report['name_pharm_chain'] = name_pharm_chain
        df_report['start_date'] = str(start_date)
        df_report['end_date'] = str(end_date)
        df_report['processed_dttm'] = str(datetime.now())
        
        if 'supplier' not in df_report.columns:
            df_report['supplier'] = None
        
        # Финальный порядок колонок
        final_column_order = [
            'uuid_report',
            'branch_name',
            'city',
            'street',
            'product',
            'supplier',     
            'sale_quantity',
            'name_report',
            'name_pharm_chain',
            'start_date',
            'end_date',
            'processed_dttm'
        ]
        
        for col in final_column_order:
            if col not in df_report.columns:
                df_report[col] = None
                
        df_report = df_report[final_column_order]
            
        loger.info(f'Сформирован отчет: {len(df_report)} строк.')
        
        return {
            'table_report': df_report
        }

    except Exception as e:
        loger.info(f'ERROR parsing file {path}: {str(e)}', exc_info=True)
        raise

if __name__ == "__main__": 
    test_path = r'C:\Users\nmankov\Desktop\отчеты\Ваш доктор\Продажи\2025\01_2025.xlsx'
    
    if os.path.exists(test_path):
        result = extract_all_xls(path=test_path, name_report='Продажи')
        df = result['table_report']
        df.to_csv("test_result.csv", sep=';', encoding='utf-8-sig', index=False)
    else:
        print(f"Файл не найден: {test_path}")