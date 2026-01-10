import pandas as pd
import uuid
import os
import calendar
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin

FINAL_COLUMNS = [
    'uuid_report',
    'legal_entity',
    'pharmacy_name',
    'product_name',
    'invoice_number',
    'doc_date',
    'supplier',
    'quantity',
    'price',
    'amount',
    'start_stock', 'start_sum',
    'income_quantity', 'income_sum',
    'sale_quantity', 'sale_sum',
    'remains_quantity', 'remains_sum',
    'name_report',
    'name_pharm_chain',
    'start_date',
    'end_date',
    'processed_dttm'
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

def extract_purchases(path: str, name_report: str, name_pharm_chain: str) -> dict:
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")
        start_date, end_date = _get_dates_from_filename(path, loger)

        df_raw = pd.read_excel(path, header=None, dtype=str)

        header_row_index = -1
        for i, row in df_raw.head(20).iterrows():
            row_values = [str(v).lower().strip() for v in row.values]
            if 'юр.лицо' in row_values and 'аптека' in row_values and 'наименование' in row_values:
                header_row_index = i
                break
        
        if header_row_index == -1:
             raise ValueError("Не удалось найти строку с заголовками (ожидались 'Юр.лицо', 'Аптека', 'Наименование').")

        headers = df_raw.iloc[header_row_index]
        df = df_raw.iloc[header_row_index + 1:].copy()
        df.columns = [str(h).strip() for h in headers]
        
        rename_map = {
            'Юр.лицо': 'legal_entity',
            'Аптека': 'pharmacy_name',
            '№ счёта-фактуры': 'invoice_number',
            'Дата поставки': 'doc_date',
            'Наименование': 'product_name',
            'Поставщик': 'supplier',
            'Кол-во': 'quantity',
            'Цена': 'price',
            'Сумма': 'amount'
        }
        
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
        df_report = df_report.replace({pd.NA: None, pd.NaT: None, float('nan'): None, 'nan': None})
        return {'table_report': df_report}
    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_sales_and_remains(path: str, name_report: str, name_pharm_chain: str) -> dict:
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")
        start_date, end_date = _get_dates_from_filename(path, loger)

        df_raw = pd.read_excel(path, header=None, dtype=str)

        header_row_index = -1
        for i, row in df_raw.head(20).iterrows():
            row_values = [str(v).lower().strip() for v in row.values]
            if 'юр.лицо' in row_values and 'аптека' in row_values and 'продажи' in row_values:
                header_row_index = i
                break
        
        if header_row_index == -1:
             raise ValueError("Не удалось найти строку с заголовками (ожидались 'Юр.лицо', 'Аптека', 'Продажи').")

        headers = df_raw.iloc[header_row_index]
        data_start_index = header_row_index + 1
        if data_start_index < len(df_raw):
            next_row_vals = [str(v).lower().strip() for v in df_raw.iloc[data_start_index].values]
            if any(x in next_row_vals for x in ['кол-во', 'сумма', 'цена', 'количество']):
                loger.info("Обнаружена строка подзаголовков, пропускаем её.")
                data_start_index += 1

        df_data = df_raw.iloc[data_start_index:].copy()
        df_data.reset_index(drop=True, inplace=True)
        
        col_indices = {}
        for idx, val in enumerate(headers):
            val_str = str(val).strip()
            if val_str in ['Юр.лицо', 'Аптека', 'Наименование', 'Остаток на начало', 'Приход', 'Продажи', 'Остаток на конец']:
                col_indices[val_str] = idx
        
        df_report = pd.DataFrame()
        
        if 'Юр.лицо' in col_indices: df_report['legal_entity'] = df_data.iloc[:, col_indices['Юр.лицо']]
        if 'Аптека' in col_indices: df_report['pharmacy_name'] = df_data.iloc[:, col_indices['Аптека']]
        if 'Наименование' in col_indices: df_report['product_name'] = df_data.iloc[:, col_indices['Наименование']]
        
        def extract_pair(header_name, col_qty, col_sum):
            if header_name in col_indices:
                idx = col_indices[header_name]
                df_report[col_qty] = df_data.iloc[:, idx]
                if idx + 1 < df_data.shape[1]:
                    df_report[col_sum] = df_data.iloc[:, idx + 1]
        
        extract_pair('Остаток на начало', 'start_stock', 'start_sum')
        extract_pair('Приход', 'income_quantity', 'income_sum')
        extract_pair('Продажи', 'sale_quantity', 'sale_sum')
        extract_pair('Остаток на конец', 'remains_quantity', 'remains_sum')

        df_report['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_report))]
        df_report['name_report'] = 'Продажи+Остатки'
        df_report['name_pharm_chain'] = name_pharm_chain
        df_report['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df_report['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df_report['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for col in FINAL_COLUMNS:
            if col not in df_report.columns:
                df_report[col] = None
                
        df_report = df_report[FINAL_COLUMNS]
        df_report = df_report.replace({pd.NA: None, pd.NaT: None, float('nan'): None, 'nan': None})
        return {'table_report': df_report}
    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_xls(path, name_report, name_pharm_chain) -> dict:
    loger = LoggingMixin().log
    loger.info(f"Диспетчер 'Семейная аптека' получил задачу: '{name_report}' для '{name_pharm_chain}' из файла '{path}'")

    report_type_lower = name_report.lower()

    if 'закуп' in report_type_lower:
        return extract_purchases(path, name_report, name_pharm_chain)
    elif 'продажи' in report_type_lower or 'остатки' in report_type_lower:
        return extract_sales_and_remains(path, name_report, name_pharm_chain)
    else:
        loger.warning(f"Неизвестный тип отчета для 'Семейная аптека': '{name_report}'. Парсер не будет вызван.")
        return {}