import pandas as pd
import uuid
import os
import re
import calendar
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

MONTHS_RU = {
    'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4, 'май': 5, 'июнь': 6,
    'июль': 7, 'август': 8, 'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
}

FINAL_COLUMNS = [
    'uuid_report', 'product_name', 'product_code', 'region', 'legal_entity', 'inn',
    'pharmacy_name', 'pharmacy_code', 'supplier', 'quantity',
    'name_report', 'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm'
]

def _get_dates_from_filename(path: str) -> tuple[datetime, datetime]:
    try:
        filename = os.path.basename(path)
        match = re.search(r'(\d{2})_(\d{4})', filename)
        if match:
            month = int(match.group(1))
            year = int(match.group(2))
            start_date = datetime(year, month, 1)
            _, last_day = calendar.monthrange(year, month)
            end_date = datetime(year, month, last_day)
            return start_date, end_date
    except Exception:
        pass
    return None, None

def extract_purchases(path: str, name_report: str, name_pharm_chain: str) -> dict:
    """
    Парсер для отчета 'Закупки' от 'Планета Здоровья'.
    """
    loger = LoggingMixin().log
    loger.info(f"Запуск парсера закупок для '{name_pharm_chain}' из файла: {path}")
    
    try:
        xls = pd.ExcelFile(path)
        sheet_name = 0
        if len(xls.sheet_names) > 1:
            if 'Pivot' in xls.sheet_names:
                sheet_name = 'Pivot'
            elif 'Лист1' in xls.sheet_names:
                sheet_name = 'Лист1'

        df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=str)
        
        header_row_index = -1
        for i, row in df_raw.iterrows():
            row_values = [str(val).lower().strip() for val in row.values]
            if 'товар' in row_values and 'поставщик' in row_values:
                header_row_index = i
                break
        
        if header_row_index == -1:
            raise ValueError("Не удалось найти строку заголовков (ожидались 'Товар' и 'Поставщик').")
            
        headers = df_raw.iloc[header_row_index].fillna('').astype(str).str.strip()
        df = df_raw.iloc[header_row_index + 1:].copy()
        df.columns = headers
        
        cols_to_fill = [
            'Товар', 
            'Код ГЕС', 
            'Регион', 
            'Юр лицо', 
            'Юр.лицо',
            'ИНН', 
            'Аптека', 
            'Код аптеки'
        ]
        
        col_map = {c.lower(): c for c in df.columns}
        
        for col_name in cols_to_fill:
            col_lower = col_name.lower()
            if col_lower in col_map:
                actual_col = col_map[col_lower]
                df[actual_col] = df[actual_col].replace({'nan': None, 'None': None, '': None})
                df[actual_col] = df[actual_col].ffill()
        
        product_col = next((col for col in headers if 'товар' in col.lower()), None)
        if product_col:
             df = df[~df[product_col].astype(str).str.contains('итог', case=False, na=False)]
        
        supplier_col_idx = -1
        for idx, col in enumerate(headers):
            if col.lower() == 'поставщик':
                supplier_col_idx = idx
                break
        
        if supplier_col_idx == -1:
             raise ValueError("Колонка 'Поставщик' не найдена.")
             
        cols_after_supplier = [c for c in headers[supplier_col_idx+1:].tolist() if c]
        is_multi_month = False
        for col in cols_after_supplier:
            if any(m in col.lower() for m in MONTHS_RU):
                is_multi_month = True
                break
        
        if is_multi_month:
            fixed_cols = headers[:supplier_col_idx+1].tolist()
            month_cols = cols_after_supplier
            
            df_melted = df.melt(id_vars=fixed_cols, value_vars=month_cols, var_name='period_raw', value_name='quantity')
            
            df_melted['quantity'] = pd.to_numeric(df_melted['quantity'], errors='coerce').fillna(0)
            df_melted = df_melted[df_melted['quantity'] != 0]
            
            def get_dates(period_str):
                try:
                    s = str(period_str).strip().lower()
                    found_month = None
                    for m_name in MONTHS_RU:
                        if m_name in s:
                            found_month = m_name
                            break
                    
                    if found_month:
                        year_match = re.search(r'\d{4}', s)
                        if year_match:
                            year = int(year_match.group(0))
                            month = MONTHS_RU[found_month]
                            start_date = datetime(year, month, 1)
                            _, last_day = calendar.monthrange(year, month)
                            end_date = datetime(year, month, last_day)
                            return start_date, end_date
                except Exception:
                    pass
                return None

            dates_map = {p: get_dates(p) for p in month_cols}
            df_melted['dates'] = df_melted['period_raw'].map(dates_map)
            df_melted.dropna(subset=['dates'], inplace=True)
            
            df_melted['start_date'] = df_melted['dates'].apply(lambda x: x[0].strftime('%Y-%m-%d %H:%M:%S'))
            df_melted['end_date'] = df_melted['dates'].apply(lambda x: x[1].strftime('%Y-%m-%d %H:%M:%S'))
            
            rename_map = {
                'Товар': 'product_name', 'Код ГЕС': 'product_code', 'Регион': 'region',
                'Юр лицо': 'legal_entity', 'ИНН': 'inn', 'Аптека': 'pharmacy_name',
                'Код аптеки': 'pharmacy_code', 'Поставщик': 'supplier'
            }
            df_melted.rename(columns=rename_map, inplace=True)
        else:
            start_date, end_date = _get_dates_from_filename(path)
            if not start_date:
                loger.warning(f"Не удалось определить дату из имени файла: {path}")
                now = datetime.now()
                start_date = now.replace(day=1)
                _, last_day = calendar.monthrange(now.year, now.month)
                end_date = now.replace(day=last_day)
            
            df_melted = df.copy()
            rename_map = {
                'Товар': 'product_name', 'Код ГЕС': 'product_code', 'Регион': 'region',
                'Юр лицо': 'legal_entity', 'Юр.лицо': 'legal_entity', 'ИНН': 'inn', 
                'Аптека': 'pharmacy_name', 'Код аптеки': 'pharmacy_code', 'Поставщик': 'supplier',
                'Кол-во': 'quantity', 'Количество': 'quantity'
            }
            df_melted.rename(columns=rename_map, inplace=True)
            
            if 'quantity' not in df_melted.columns and cols_after_supplier:
                 df_melted.rename(columns={cols_after_supplier[0]: 'quantity'}, inplace=True)
            
            df_melted['quantity'] = pd.to_numeric(df_melted['quantity'], errors='coerce').fillna(0)
            df_melted = df_melted[df_melted['quantity'] != 0]
            
            df_melted['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
            df_melted['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        
        df_melted['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_melted))]
        df_melted['name_report'] = name_report
        df_melted['name_pharm_chain'] = name_pharm_chain
        df_melted['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for col in FINAL_COLUMNS:
            if col not in df_melted.columns:
                df_melted[col] = None
                
        return {'table_report': df_melted[FINAL_COLUMNS]}

    except Exception as e:
        loger.error(f"Ошибка при парсинге закупок: {e}", exc_info=True)
        raise

def extract_sales_remains(path: str, name_report: str, name_pharm_chain: str) -> dict:
    """
    Парсер для отчета 'Продажи + Остатки' от 'Планета Здоровья'.
    """
    loger = LoggingMixin().log
    loger.info(f"Запуск парсера продаж и остатков для '{name_pharm_chain}' из файла: {path}")
    
    try:
        xls = pd.ExcelFile(path)
        sheet_name = 0
        if len(xls.sheet_names) > 1:
            if 'Pivot' in xls.sheet_names:
                sheet_name = 'Pivot'
            elif 'Лист1' in xls.sheet_names:
                sheet_name = 'Лист1'

        df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=str)
        
        header_row_index = -1
        for i, row in df_raw.iterrows():
            row_values = [str(val).lower().strip() for val in row.values]
            if ('товар' in row_values or 'наименование товара' in row_values) and 'аптека' in row_values:
                header_row_index = i
                break
        
        if header_row_index == -1:
             for i, row in df_raw.iterrows():
                row_values = [str(val).lower().strip() for val in row.values]
                if 'наименование товара' in row_values:
                    header_row_index = i
                    break

        if header_row_index == -1:
            raise ValueError("Не удалось найти строку заголовков.")

        headers = df_raw.iloc[header_row_index].fillna('').astype(str).str.strip()
        df = df_raw.iloc[header_row_index + 1:].copy()
        df.columns = headers
        
        cols_to_fill = ['Товар', 'Наименование товара', 'Код ГЕС', 'Регион', 'Юр лицо', 'Юр.лицо', 'ИНН', 'Аптека', 'Код аптеки']
        col_map = {c.lower(): c for c in df.columns}
        for col_name in cols_to_fill:
            col_lower = col_name.lower()
            if col_lower in col_map:
                actual_col = col_map[col_lower]
                df[actual_col] = df[actual_col].replace({'nan': None, 'None': None, '': None})
                df[actual_col] = df[actual_col].ffill()

        product_col = next((col for col in headers if 'товар' in col.lower() or 'наименование товара' in col.lower()), None)
        if product_col:
             df = df[~df[product_col].astype(str).str.contains('итог', case=False, na=False)]

        month_cols = [c for c in headers if any(m in c.lower() for m in MONTHS_RU)]
        dfs_to_concat = []

        if month_cols:
            pharmacy_code_idx = -1
            for idx, col in enumerate(headers):
                if 'код аптеки' in col.lower():
                    pharmacy_code_idx = idx
                    break
            
            if pharmacy_code_idx == -1:
                 for idx, col in enumerate(headers):
                     if any(m in col.lower() for m in MONTHS_RU):
                         pharmacy_code_idx = idx - 1
                         break
            
            if pharmacy_code_idx == -1:
                 raise ValueError("Не удалось определить разделитель колонок (Код аптеки).")

            fixed_cols = headers[:pharmacy_code_idx+1].tolist()
            dynamic_cols = headers[pharmacy_code_idx+1:].tolist()
            
            month_cols_dynamic = [c for c in dynamic_cols if any(m in c.lower() for m in MONTHS_RU)]
            remains_col = next((c for c in dynamic_cols if 'остат' in c.lower()), None)
            
            def get_dates(period_str):
                try:
                    s = str(period_str).strip().lower()
                    found_month = None
                    for m_name in MONTHS_RU:
                        if m_name in s:
                            found_month = m_name
                            break
                    if found_month:
                        year_match = re.search(r'\d{4}', s)
                        if year_match:
                            year = int(year_match.group(0))
                            month = MONTHS_RU[found_month]
                            start_date = datetime(year, month, 1)
                            _, last_day = calendar.monthrange(year, month)
                            end_date = datetime(year, month, last_day)
                            return start_date, end_date
                except Exception:
                    pass
                return None, None

            if month_cols_dynamic:
                df_sales = df.melt(id_vars=fixed_cols, value_vars=month_cols_dynamic, var_name='period_raw', value_name='quantity')
                df_sales['quantity'] = pd.to_numeric(df_sales['quantity'], errors='coerce').fillna(0)
                df_sales = df_sales[df_sales['quantity'] != 0]
                
                dates_map = {p: get_dates(p) for p in month_cols_dynamic}
                df_sales['dates'] = df_sales['period_raw'].map(dates_map)
                df_sales = df_sales[df_sales['dates'].apply(lambda x: x[0] is not None)]
                
                df_sales['start_date'] = df_sales['dates'].apply(lambda x: x[0].strftime('%Y-%m-%d %H:%M:%S'))
                df_sales['end_date'] = df_sales['dates'].apply(lambda x: x[1].strftime('%Y-%m-%d %H:%M:%S'))
                df_sales['name_report'] = 'Продажи'
                df_sales.drop(columns=['dates', 'period_raw'], inplace=True)
                dfs_to_concat.append(df_sales)

            if remains_col:
                df_remains = df[fixed_cols + [remains_col]].copy()
                df_remains.rename(columns={remains_col: 'quantity'}, inplace=True)
                df_remains['quantity'] = pd.to_numeric(df_remains['quantity'], errors='coerce').fillna(0)
                df_remains = df_remains[df_remains['quantity'] != 0]
                
                r_start, r_end = None, None
                if month_cols_dynamic:
                    valid_dates = [get_dates(m) for m in month_cols_dynamic]
                    valid_starts = [d[0] for d in valid_dates if d[0]]
                    valid_ends = [d[1] for d in valid_dates if d[1]]
                    
                    if valid_starts and valid_ends:
                        r_start = min(valid_starts)
                        r_end = max(valid_ends)
                
                if not r_start:
                    r_start, r_end = _get_dates_from_filename(path)
                
                if not r_start:
                     now = datetime.now()
                     r_start = now.replace(day=1)
                     _, last_day = calendar.monthrange(now.year, now.month)
                     r_end = now.replace(day=last_day)

                df_remains['start_date'] = r_start.strftime('%Y-%m-%d %H:%M:%S')
                df_remains['end_date'] = r_end.strftime('%Y-%m-%d %H:%M:%S')
                df_remains['name_report'] = 'Остатки'
                dfs_to_concat.append(df_remains)
        else:
            sales_col = next((c for c in headers if 'продаж' in c.lower()), None)
            remains_col = next((c for c in headers if 'остат' in c.lower()), None)
            
            value_cols = [c for c in [sales_col, remains_col] if c]
            fixed_cols = [c for c in headers if c not in value_cols]
            
            start_date, end_date = _get_dates_from_filename(path)
            if not start_date:
                loger.warning(f"Не удалось определить дату из имени файла: {path}")
                now = datetime.now()
                start_date = now.replace(day=1)
                _, last_day = calendar.monthrange(now.year, now.month)
                end_date = now.replace(day=last_day)
            
            if sales_col:
                df_sales = df[fixed_cols + [sales_col]].copy()
                df_sales.rename(columns={sales_col: 'quantity'}, inplace=True)
                df_sales['quantity'] = pd.to_numeric(df_sales['quantity'], errors='coerce').fillna(0)
                df_sales = df_sales[df_sales['quantity'] != 0]
                df_sales['name_report'] = 'Продажи'
                df_sales['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
                df_sales['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
                dfs_to_concat.append(df_sales)
                
            if remains_col:
                df_remains = df[fixed_cols + [remains_col]].copy()
                df_remains.rename(columns={remains_col: 'quantity'}, inplace=True)
                df_remains['quantity'] = pd.to_numeric(df_remains['quantity'], errors='coerce').fillna(0)
                df_remains = df_remains[df_remains['quantity'] != 0]
                df_remains['name_report'] = 'Остатки'
                df_remains['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
                df_remains['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
                dfs_to_concat.append(df_remains)
            
        if not dfs_to_concat:
            loger.warning("Не найдено данных ни по продажам, ни по остаткам.")
            return {}
            
        df_final = pd.concat(dfs_to_concat, ignore_index=True)
        
        rename_map = {
            'Товар': 'product_name', 'Наименование товара': 'product_name',
            'Код ГЕС': 'product_code', 'Регион': 'region',
            'Юр лицо': 'legal_entity', 'Юр.лицо': 'legal_entity',
            'ИНН': 'inn', 'Аптека': 'pharmacy_name',
            'Код аптеки': 'pharmacy_code'
        }
        df_final.rename(columns=rename_map, inplace=True)
        
        df_final['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_final))]
        df_final['name_pharm_chain'] = name_pharm_chain
        df_final['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for col in FINAL_COLUMNS:
            if col not in df_final.columns:
                df_final[col] = None
                
        return {'table_report': df_final[FINAL_COLUMNS]}

    except Exception as e:
        loger.error(f"Ошибка при парсинге продаж и остатков: {e}", exc_info=True)
        raise

def extract_xls(path: str, name_report: str, name_pharm_chain: str) -> dict:
    """
    Диспетчер, вызывающий нужный парсер для 'Планета Здоровья' в зависимости от типа отчета.
    """
    loger = LoggingMixin().log
    loger.info(f"Диспетчер 'Планета Здоровья' получил задачу: '{name_report}' для '{name_pharm_chain}' из файла '{path}'")

    report_type_lower = name_report.lower()

    if 'закуп' in report_type_lower:
        return extract_purchases(path, name_report, name_pharm_chain)
    elif 'продаж' in report_type_lower or 'остат' in report_type_lower:
        return extract_sales_remains(path, name_report, name_pharm_chain)
    else:
        loger.warning(f"Неизвестный тип отчета для 'Планета Здоровья': '{name_report}'. Парсер не будет вызван.")
        return {}

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    main_loger.info("Запуск локального теста для парсера 'Планета Здоровья'.")
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты\Планета Здоровья\продажи+ остатки\2023\06_2023.xlsx'
    test_report_type = 'Продажи'

    if os.path.exists(test_file_path):
        main_loger.info(f"Тестовый файл найден: {test_file_path}")
        try:
            result = extract_xls(path=test_file_path, name_report=test_report_type, name_pharm_chain='Планета Здоровья')
            df = result.get('table_report')
            if df is not None and not df.empty:
                output_filename = f"{os.path.splitext(os.path.basename(test_file_path))[0]}_result.csv"
                output_path = os.path.join(os.path.dirname(test_file_path), output_filename)
                df.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
                main_loger.info(f"Результат успешно сохранен в: {output_path}")
        except Exception as e:
            main_loger.error(f"Во время локального теста произошла ошибка: {e}", exc_info=True)
    else:
        main_loger.warning(f"Тестовый файл не найден по пути: {test_file_path}")