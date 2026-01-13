import pandas as pd
import uuid
import os
import calendar
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
import re

FINAL_COLUMNS = [
    'uuid_report', 'product_name', 'product_code', 'supplier', 'pharmacy_name',
    'purchase_quantity', 'sale_quantity', 'remains_quantity',
    'name_report', 'name_pharm_chain',
    'start_date', 'end_date', 'processed_dttm'
]

def _get_dates_from_filename(path: str, loger) -> tuple[datetime, datetime]:
    try:
        filename = os.path.basename(path)
        date_part = os.path.splitext(filename)[0]
        report_date = datetime.strptime(date_part, "%m_%Y")
        start_date = report_date.replace(day=1)
        _, last_day = calendar.monthrange(report_date.year, report_date.month)
        end_date = report_date.replace(day=last_day)
        loger.info(f"Определен период отчета по имени файла: {start_date.date()} - {end_date.date()}")
        return start_date, end_date
    except Exception as e:
        loger.error(f"Не удалось определить дату из имени файла '{os.path.basename(path)}'. Ожидаемый формат: ММ_ГГГГ.xlsx. Ошибка: {e}")
        raise

def _extract_base(path: str, name_report: str, name_pharm_chain: str, rename_map: dict, header_validator=None) -> dict:
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")
        
        start_date, end_date = _get_dates_from_filename(path, loger)
        
        xls = pd.ExcelFile(path)
        df_raw = pd.DataFrame()
        header_row_index = -1

        if header_validator is None:
            header_validator = lambda row: 'товар' in [str(r).lower() for r in row]

        for sheet_name in xls.sheet_names:
            df_temp = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=str)
            for i, row in df_temp.head(20).iterrows():
                row_values = [str(v).strip() for v in row.values]
                if header_validator(row_values):
                    header_row_index = i
                    df_raw = df_temp
                    loger.info(f"Строка с заголовками найдена на листе '{sheet_name}' по индексу: {header_row_index}")
                    break
            if header_row_index != -1:
                break
        
        if header_row_index == -1:
            raise ValueError("Не удалось найти строку с заголовками ни на одном листе.")

        headers = df_raw.iloc[header_row_index]
        df = df_raw.iloc[header_row_index + 1:].copy()
        df.columns = [str(h).strip().lower().replace('\n', ' ').replace('  ', ' ') for h in headers]
        df.dropna(how='all', inplace=True)
        df.reset_index(drop=True, inplace=True)

        if not df.empty:
            last_row = df.iloc[-1]
            last_row_str = last_row.astype(str).str.lower()
            
            is_total = last_row_str.str.contains('итог').any()
            is_empty_product = 'товар' in df.columns and str(last_row['товар']).strip().lower() in ['', 'nan', 'none']

            if is_total or is_empty_product:
                df = df.iloc[:-1]
                loger.info("Удалена последняя строка (итог или пустой товар).")

        loger.info(f'Успешно получено {len(df)} строк!')

        df.rename(columns=rename_map, inplace=True)

        df['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df['name_report'] = name_report
        df['name_pharm_chain'] = name_pharm_chain
        df['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for col in FINAL_COLUMNS:
            if col not in df.columns:
                df[col] = None

        df_report = df[FINAL_COLUMNS]

        df_report = df_report.replace({pd.NA: None, pd.NaT: None, '': None, 'nan': None})

        loger.info(f"Парсинг отчета '{name_report}' успешно завершен.")
        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def _extract_hierarchical_purchases(df_raw, header_row_index, start_date, end_date, name_report, name_pharm_chain, loger):
    headers = df_raw.iloc[header_row_index]
    df = df_raw.iloc[header_row_index + 1:].copy()
    df.columns = [str(h).strip().lower().replace('\n', ' ').replace('  ', ' ') for h in headers]
    df.reset_index(drop=True, inplace=True)
    
    col_num = next((c for c in df.columns if '№' in c or c == 'n' or 'п/п' in c), None)
    col_prod = 'товар'
    col_qty = next((c for c in df.columns if c in ['кол-во', 'количество']), 'кол-во')
    col_code = next((c for c in df.columns if 'код' in c and 'товар' in c), None)
    
    data = []
    current_product = None
    
    for i, row in df.iterrows():
        val_num = str(row[col_num]).strip()
        val_prod = str(row[col_prod]).strip()
        val_qty = str(row[col_qty]).strip()
        val_code = str(row[col_code]).strip() if col_code else None
        
        if val_prod.lower() in ['nan', '', 'none']:
            continue
            
        is_product_row = False
        if val_num and val_num.lower() not in ['nan', 'none']:
             if any(c.isdigit() for c in val_num):
                 is_product_row = True
        
        if is_product_row:
            current_product = val_prod
        else:
            if current_product:
                supplier_name = val_prod
                if 'итог' in supplier_name.lower():
                    continue
                data.append({
                    'product_name': current_product,
                    'supplier': supplier_name,
                    'product_code': val_code,
                    'purchase_quantity': val_qty
                })

    df_report = pd.DataFrame(data)
    
    df_report['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_report))]
    df_report['name_report'] = name_report
    df_report['name_pharm_chain'] = name_pharm_chain
    df_report['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
    df_report['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
    df_report['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for col in FINAL_COLUMNS:
        if col not in df_report.columns:
            df_report[col] = None
            
    df_report = df_report[FINAL_COLUMNS].replace({pd.NA: None, pd.NaT: None, '': None, 'nan': None})
    
    loger.info(f"Иерархический парсинг завершен. Получено {len(df_report)} строк.")
    return {'table_report': df_report}

def _extract_hierarchical_remains(df_raw, header_row_index, start_date, end_date, name_report, name_pharm_chain, loger):
    headers = df_raw.iloc[header_row_index]
    df = df_raw.iloc[header_row_index + 1:].copy()
    df.columns = [str(h).strip().lower().replace('\n', ' ').replace('  ', ' ') for h in headers]
    df.reset_index(drop=True, inplace=True)
    
    col_num = next((c for c in df.columns if '№' in c or c == 'n' or 'п/п' in c), None)
    col_prod = next((c for c in df.columns if 'товар' in c and 'код' not in c), None)
    if not col_prod:
         col_prod = next((c for c in df.columns if 'товар' in c), None)

    col_qty = next((c for c in df.columns if 'остаток' in c or 'уп' in c), None)
    col_code = next((c for c in df.columns if 'код' in c and 'товар' in c), None)
    
    data = []
    current_pharmacy = None
    
    for i, row in df.iterrows():
        val_num = str(row[col_num]).strip() if col_num else ""
        val_prod = str(row[col_prod]).strip() if col_prod else ""
        val_qty = str(row[col_qty]).strip() if col_qty else ""
        val_code = str(row[col_code]).strip() if col_code else None
        
        row_str = " ".join([str(x).lower() for x in row.values])
        
        if 'аптека' in row_str and 'ассортимент' not in row_str and 'итог' not in row_str:
             for cell in row:
                 if 'аптека' in str(cell).lower():
                     current_pharmacy = str(cell).strip()
                     break
             continue

        if 'ассортимент' in row_str or 'итог' in row_str:
            continue
            
        if re.match(r'^\d+(\.0*)?$', val_num) and current_pharmacy:
             data.append({
                 'pharmacy_name': current_pharmacy,
                 'product_name': val_prod,
                 'product_code': val_code,
                 'remains_quantity': val_qty
             })

    df_report = pd.DataFrame(data)
    
    df_report['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_report))]
    df_report['name_report'] = name_report
    df_report['name_pharm_chain'] = name_pharm_chain
    df_report['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
    df_report['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
    df_report['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for col in FINAL_COLUMNS:
        if col not in df_report.columns:
            df_report[col] = None
            
    df_report = df_report[FINAL_COLUMNS].replace({pd.NA: None, pd.NaT: None, '': None, 'nan': None})
    
    loger.info(f"Иерархический парсинг остатков завершен. Получено {len(df_report)} строк.")
    return {'table_report': df_report}

def _extract_hierarchical_sales(df_raw, header_row_index, start_date, end_date, name_report, name_pharm_chain, loger):
    headers = df_raw.iloc[header_row_index]
    df = df_raw.iloc[header_row_index + 1:].copy()
    df.columns = [str(h).strip().lower().replace('\n', ' ').replace('  ', ' ') for h in headers]
    df.reset_index(drop=True, inplace=True)
    
    col_num = next((c for c in df.columns if '№' in c or c == 'n' or 'п/п' in c), None)
    col_prod = next((c for c in df.columns if 'товар' in c and 'код' not in c), None)
    if not col_prod:
         col_prod = next((c for c in df.columns if 'товар' in c), None)
         
    col_qty = next((c for c in df.columns if 'кол-во' in c or 'количество' in c), None)
    col_code = next((c for c in df.columns if 'код' in c and 'товар' in c), None)
    
    data = []
    current_pharmacy = None
    
    for i, row in df.iterrows():
        val_num = str(row[col_num]).strip() if col_num else ""
        val_prod = str(row[col_prod]).strip() if col_prod else ""
        val_qty = str(row[col_qty]).strip() if col_qty else ""
        val_code = str(row[col_code]).strip() if col_code else None
        
        row_str = " ".join([str(x).lower() for x in row.values])
        
        if 'аптека' in row_str and 'итог' not in row_str:
             for cell in row:
                 if 'аптека' in str(cell).lower():
                     current_pharmacy = str(cell).strip()
                     break
             continue

        if 'итог' in row_str:
            continue
            
        if re.match(r'^\d+(\.0*)?$', val_num) and current_pharmacy:
             data.append({
                 'pharmacy_name': current_pharmacy,
                 'product_name': val_prod,
                 'product_code': val_code,
                 'sale_quantity': val_qty
             })

    df_report = pd.DataFrame(data)
    df_report['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_report))]
    df_report['name_report'] = name_report
    df_report['name_pharm_chain'] = name_pharm_chain
    df_report['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
    df_report['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
    df_report['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for col in FINAL_COLUMNS:
        if col not in df_report.columns:
            df_report[col] = None
            
    df_report = df_report[FINAL_COLUMNS].replace({pd.NA: None, pd.NaT: None, '': None, 'nan': None})
    
    loger.info(f"Иерархический парсинг продаж завершен. Получено {len(df_report)} строк.")
    return {'table_report': df_report}

def extract_purchases(path: str, name_report: str, name_pharm_chain: str) -> dict:
    loger = LoggingMixin().log
    
    try:
        start_date, end_date = _get_dates_from_filename(path, loger)
        xls = pd.ExcelFile(path)
        
        def find(validator):
            for sheet_name in xls.sheet_names:
                df_temp = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=str)
                for i, row in df_temp.head(20).iterrows():
                    row_values = [str(v).strip() for v in row.values]
                    if validator(row_values):
                        return df_temp, i
            return pd.DataFrame(), -1

        def flat_validator(row):
            row_lower = [str(r).lower() for r in row]
            return 'товар' in row_lower and \
                   any(q in row_lower for q in ['кол-во', 'количество']) and \
                   any(s in row_lower for s in ['поставщик', 'дистрибьютор', 'контрагент'])

        df_raw, header_row_index = find(flat_validator)
        if header_row_index != -1:
            loger.info("Обнаружен плоский формат отчета закупок.")
            rename_map = {
                'товар': 'product_name',
                'поставщик': 'supplier',
                'дистрибьютор': 'supplier',
                'кол-во': 'purchase_quantity',
                'контрагент': 'supplier',
                'количество': 'purchase_quantity',
                'код товара': 'product_code'
            }
            return _extract_base(path, name_report, name_pharm_chain, rename_map, header_validator=flat_validator)

        def hierarchical_validator(row):
            row_lower = [str(r).lower() for r in row]
            has_num = any('№' in r or r == 'n' or 'п/п' in r for r in row_lower)
            has_prod = 'товар' in row_lower
            has_qty = any(q in row_lower for q in ['кол-во', 'количество'])
            return has_num and has_prod and has_qty

        df_raw, header_row_index = find(hierarchical_validator)
        if header_row_index != -1:
            loger.info("Обнаружен иерархический формат отчета закупок.")
            return _extract_hierarchical_purchases(df_raw, header_row_index, start_date, end_date, name_report, name_pharm_chain, loger)

        for sheet_name in xls.sheet_names:
            df_temp = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=str)
            if len(df_temp) >= 5:
                r3c0 = str(df_temp.iloc[3, 0]).lower().strip()
                r3c1 = str(df_temp.iloc[3, 1]).lower().strip()
                r4c2 = str(df_temp.iloc[4, 2]).lower().strip()
                
                cond1 = '№' in r3c0 or r3c0 == 'n' or 'п/п' in r3c0
                cond2 = 'товар' in r3c1
                cond3 = any(x in r4c2 for x in ['кол-во', 'количество'])
                
                if cond1 and cond2 and cond3:
                    loger.info(f"Обнаружен разделенный иерархический заголовок на листе '{sheet_name}'.")
                    df_temp.iloc[4, 0] = '№'
                    df_temp.iloc[4, 1] = 'Товар'
                    return _extract_hierarchical_purchases(df_temp, 4, start_date, end_date, name_report, name_pharm_chain, loger)

        raise ValueError("Не удалось определить формат отчета закупок (ни плоский, ни иерархический).")

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_sales(path: str, name_report: str, name_pharm_chain: str) -> dict:
    loger = LoggingMixin().log
    
    try:
        start_date, end_date = _get_dates_from_filename(path, loger)
        xls = pd.ExcelFile(path)
        
        def find(validator):
            for sheet_name in xls.sheet_names:
                df_temp = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=str)
                for i, row in df_temp.head(20).iterrows():
                    row_values = [str(v).strip() for v in row.values]
                    if validator(row_values):
                        return df_temp, i
            return pd.DataFrame(), -1

        def hierarchical_validator(row):
            row_lower = [str(r).lower().strip().replace('\n', ' ') for r in row]
            has_num = any(x in row_lower for x in ['№', 'n', 'п/п'])
            has_prod = 'товар' in row_lower
            has_qty = any(x in row_lower for x in ['кол-во', 'количество'])
            has_code = 'код товара' in row_lower
            has_pharm_col = 'аптека' in row_lower
            return has_num and has_prod and has_qty and has_code and not has_pharm_col

        df_raw, header_row_index = find(hierarchical_validator)
        if header_row_index != -1:
            loger.info("Обнаружен потенциально иерархический формат отчета продаж.")
            hierarchical_result = _extract_hierarchical_sales(df_raw, header_row_index, start_date, end_date, name_report, name_pharm_chain, loger)
            if not hierarchical_result['table_report'].empty:
                return hierarchical_result
            loger.info("Иерархический парсинг продаж не дал результатов (0 строк). Переход к плоскому парсингу.")

        # 2. Пробуем плоский формат (Аптека или №, Товар, Кол-во)
        def flat_validator(row):
            row_lower = [str(r).lower().strip().replace('\n', ' ') for r in row]
            has_prod = 'товар' in row_lower
            has_qty = any(x in row_lower for x in ['кол-во', 'количество', 'продажи шт'])
            has_pharm_or_num = any(x in row_lower for x in ['аптека', '№', 'n', 'п/п'])
            return has_prod and has_qty and has_pharm_or_num

        df_raw, header_row_index = find(flat_validator)
        if header_row_index != -1:
            loger.info("Обнаружен плоский формат отчета продаж.")
            rename_map = {
                'товар': 'product_name',
                'кол-во': 'sale_quantity',
                'количество': 'sale_quantity',
                'продажи шт': 'sale_quantity',
                'аптека': 'pharmacy_name',
                '№': 'pharmacy_name',
                'n': 'pharmacy_name',
                'п/п': 'pharmacy_name',
                'код товара': 'product_code'
            }
            return _extract_base(path, name_report, name_pharm_chain, rename_map, header_validator=flat_validator)

        raise ValueError("Не удалось определить формат отчета продаж.")

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_remains(path: str, name_report: str, name_pharm_chain: str) -> dict:
    loger = LoggingMixin().log
    
    try:
        start_date, end_date = _get_dates_from_filename(path, loger)
        xls = pd.ExcelFile(path)
        
        def find(validator):
            for sheet_name in xls.sheet_names:
                df_temp = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=str)
                for i, row in df_temp.head(20).iterrows():
                    row_values = [str(v).strip() for v in row.values]
                    if validator(row_values):
                        return df_temp, i
            return pd.DataFrame(), -1

        def hierarchical_validator(row):
            row_lower = [str(r).lower().strip().replace('\n', ' ') for r in row]
            has_num = any(x in row_lower for x in ['№', 'n', 'п/п'])
            has_prod = 'товар' in row_lower
            has_qty = any(x in row_lower for x in ['остаток', 'остаток (упаковки)', 'уп'])
            has_pharm_col = 'аптека' in row_lower
            return has_num and has_prod and has_qty and not has_pharm_col

        df_raw, header_row_index = find(hierarchical_validator)
        if header_row_index != -1:
            loger.info("Обнаружен потенциально иерархический формат отчета остатков.")
            hierarchical_result = _extract_hierarchical_remains(df_raw, header_row_index, start_date, end_date, name_report, name_pharm_chain, loger)
            if not hierarchical_result['table_report'].empty:
                return hierarchical_result
            loger.info("Иерархический парсинг не дал результатов (0 строк). Переход к плоскому парсингу.")

        # 2. Пробуем плоский формат (Аптека или №, Товар, Остаток/Уп)
        def flat_validator(row):
            row_lower = [str(r).lower().strip().replace('\n', ' ') for r in row]
            has_prod = 'товар' in row_lower
            has_qty = any(x in row_lower for x in ['остаток', 'остаток (упаковки)', 'уп'])
            has_pharm_or_num = any(x in row_lower for x in ['аптека', '№', 'n', 'п/п'])
            return has_prod and has_qty and has_pharm_or_num

        df_raw, header_row_index = find(flat_validator)
        if header_row_index != -1:
            loger.info("Обнаружен плоский формат отчета остатков.")
            rename_map = {
                'товар': 'product_name',
                'остаток (упаковки)': 'remains_quantity',
                'остаток': 'remains_quantity',
                'уп': 'remains_quantity',
                'аптека': 'pharmacy_name',
                '№': 'pharmacy_name',
                'n': 'pharmacy_name',
                'п/п': 'pharmacy_name',
                'код товара': 'product_code'
            }
            return _extract_base(path, name_report, name_pharm_chain, rename_map, header_validator=flat_validator)

        raise ValueError("Не удалось определить формат отчета остатков.")

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_xls(path, name_report, name_pharm_chain) -> dict:
    loger = LoggingMixin().log
    loger.info(f"Диспетчер 'Фармгарант' получил задачу: '{name_report}' для '{name_pharm_chain}' из файла '{path}'")

    report_type_lower = name_report.lower()

    if 'закуп' in report_type_lower:
        return extract_purchases(path, name_report, name_pharm_chain)
    elif 'продажи' in report_type_lower:
        return extract_sales(path, name_report, name_pharm_chain)
    elif 'остатки' in report_type_lower:
        return extract_remains(path, name_report, name_pharm_chain)
    else:
        loger.warning(f"Неизвестный тип отчета для 'Фармгарант': '{name_report}'. Парсер не будет вызван.")
        return {}

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    main_loger.info("Запуск локального теста для парсера 'Фармгарант'.")
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты_аптек\Фармгарант\Остатки\04_2025.xlsx'
    test_report_type = 'Остатки'

    if os.path.exists(test_file_path):
        main_loger.info(f"Тестовый файл найден: {test_file_path}")
        try:
            result = extract_xls(path=test_file_path, name_report=test_report_type, name_pharm_chain='Фармгарант')
            df = result.get('table_report')
            if df is not None and not df.empty:
                output_filename = f"{os.path.splitext(os.path.basename(test_file_path))[0]}_result.csv"
                output_path = os.path.join(os.path.dirname(test_file_path), output_filename)
                df.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
                main_loger.info(f"Результат успешно сохранен в: {output_path}")
            else:
                main_loger.info("Результат пустой (заглушка).")
        except Exception as e:
            main_loger.error(f"Во время локального теста произошла ошибка: {e}", exc_info=True)
    else:
        main_loger.warning(f"Тестовый файл не найден по пути: {test_file_path}")