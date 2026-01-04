import pandas as pd
import uuid
import os
import calendar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.utils.log.logging_mixin import LoggingMixin

FINAL_COLUMNS = [
    'uuid_report', 'legal_entity', 'inn', 'pharmacy_name', 'pharmacy_code',
    'supplier', 'doc_number', 'doc_date', 'product_group', 'product_name',
    'product_code', 'quantity', 'purchase_sum', 'sale_sum',
    'name_report', 'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm'
]

def _get_dates_from_filename(path: str, loger, offset_months=0) -> tuple[datetime, datetime]:
    try:
        filename = os.path.basename(path)
        date_part = os.path.splitext(filename)[0]
        report_date = datetime.strptime(date_part, "%m_%Y")
        start_date = (report_date - relativedelta(months=offset_months)).replace(day=1)
        _, last_day = calendar.monthrange(report_date.year, report_date.month)
        end_date = report_date.replace(day=last_day)
        loger.info(f"Определен период отчета по имени файла: {start_date.date()} - {end_date.date()}")
        return start_date, end_date
    except Exception as e:
        loger.error(f"Не удалось определить дату из имени файла '{os.path.basename(path)}'. Ожидаемый формат: ММ_ГГГГ.xlsx. Ошибка: {e}")
        raise

def extract_custom(path: str, name_report: str, name_pharm_chain: str) -> dict:

    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")
        
        df_raw = pd.read_excel(path, header=None, dtype=str)

        header_row_index = -1
        keywords = ['ЮрЛицо', 'ИНН', 'Торговая точка', 'Код аптеки', 'Контрагент']
        
        for i, row in df_raw.head(20).iterrows():
            row_values = [str(v).strip() for v in row.values]
            if any(k in row_values for k in keywords):
                header_row_index = i
                break
        
        if header_row_index == -1:
            raise ValueError("Не удалось найти строку с заголовками в файле.")

        headers = df_raw.iloc[header_row_index]
        df = df_raw.iloc[header_row_index + 1:].copy()
        df.columns = [str(h).strip() for h in headers]
        
        rename_map = {
            'ЮрЛицо': 'legal_entity',
            'ИНН': 'inn',
            'Торговая точка': 'pharmacy_name',
            'Код аптеки': 'pharmacy_code',
            'Контрагент': 'supplier',
            'Номер документа': 'doc_number',
            'Дата документа прихода': 'doc_date',
            'Группа товаров': 'product_group',
            'Товар': 'product_name',
            'Код ГЕС товара': 'product_code',
            'Количество штук': 'quantity',
            'Сумма покупки СИП': 'purchase_sum'
        }
        
        df.rename(columns=rename_map, inplace=True)
        df.dropna(how='all', inplace=True)
        
        if 'legal_entity' in df.columns:
            df = df[df['legal_entity'].astype(str).str.strip() != 'Все']
        
        try:
            if 'doc_date' in df.columns:
                dates = pd.to_datetime(df['doc_date'], format='%d.%m.%Y', errors='coerce')
                if dates.notna().any():
                    min_date = dates.min()
                    max_date = dates.max()
                    start_date = min_date.replace(day=1)
                    _, last_day = calendar.monthrange(max_date.year, max_date.month)
                    end_date = max_date.replace(day=last_day)
                    loger.info(f"Определен период отчета по данным: {start_date.date()} - {end_date.date()}")
                else:
                    raise ValueError("Не найдены корректные даты в столбце 'doc_date'")
            else:
                raise ValueError("Столбец 'doc_date' отсутствует")
        except Exception as e:
            loger.warning(f"Не удалось определить даты из содержимого ({e}). Попытка определить из имени файла.")
            start_date, end_date = _get_dates_from_filename(path, loger)

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
        df_report = df_report.where(pd.notna(df_report), None)
        
        loger.info(f"Парсинг отчета '{name_report}' успешно завершен. Строк: {len(df_report)}")
        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_sales(path: str, name_report: str, name_pharm_chain: str) -> dict:
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")
        
        filename = os.path.basename(path)
        date_part = os.path.splitext(filename)[0]
        try:
            file_dt = datetime.strptime(date_part, "%m_%Y")
            year = file_dt.year
        except ValueError:
            raise ValueError(f"Неверный формат имени файла '{filename}'. Ожидается MM_YYYY.xlsx")

        df_raw = pd.read_excel(path, header=None, dtype=str)

        idx_month = 11    
        idx_prod_name = 12
        idx_headers = 13  
        idx_data_start = 14

        months_ru = {
            'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4, 'май': 5, 'июнь': 6,
            'июль': 7, 'август': 8, 'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
        }

        valid_col_indices = []
        col_metadata = {}

        for col_idx in range(3, df_raw.shape[1]):
            month_val = str(df_raw.iloc[idx_month, col_idx]).strip().lower()
            
            if 'все' in month_val:
                continue
            
            month_num = None
            for m_name, m_num in months_ru.items():
                if m_name in month_val:
                    month_num = m_num
                    break
            
            if month_num:
                start_date = datetime(year, month_num, 1)
                _, last_day = calendar.monthrange(year, month_num)
                end_date = datetime(year, month_num, last_day)
                
                prod_name = str(df_raw.iloc[idx_prod_name, col_idx]).strip()
                prod_code = str(df_raw.iloc[idx_headers, col_idx]).strip()
                
                valid_col_indices.append(col_idx)
                col_metadata[col_idx] = {
                    'product_name': prod_name,
                    'product_code': prod_code,
                    'start_date': start_date,
                    'end_date': end_date
                }

        if not valid_col_indices:
            loger.warning("Не найдено колонок с месяцами для обработки.")
            return {'table_report': pd.DataFrame()}

        df_data = df_raw.iloc[idx_data_start:, [0, 1, 2] + valid_col_indices].copy()
        
        df_data.rename(columns={0: 'legal_entity', 1: 'pharmacy_name', 2: 'pharmacy_code'}, inplace=True)
        
        if 'legal_entity' in df_data.columns:
            df_data = df_data[df_data['legal_entity'].astype(str).str.strip() != 'Все']
        
        # Unpivot
        df_melted = df_data.melt(
            id_vars=['legal_entity', 'pharmacy_name', 'pharmacy_code'],
            value_vars=valid_col_indices,
            var_name='col_idx',
            value_name='quantity'
        )
        
        df_melted['product_name'] = df_melted['col_idx'].map(lambda x: col_metadata[x]['product_name'])
        df_melted['product_code'] = df_melted['col_idx'].map(lambda x: col_metadata[x]['product_code'])
        df_melted['start_date'] = df_melted['col_idx'].map(lambda x: col_metadata[x]['start_date'].strftime('%Y-%m-%d %H:%M:%S'))
        df_melted['end_date'] = df_melted['col_idx'].map(lambda x: col_metadata[x]['end_date'].strftime('%Y-%m-%d %H:%M:%S'))
        
        df_melted.drop(columns=['col_idx'], inplace=True)
        
        df_melted['quantity'] = pd.to_numeric(df_melted['quantity'], errors='coerce').fillna(0)
        df_melted = df_melted[df_melted['quantity'] != 0]
        
        df_melted['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_melted))]
        df_melted['name_report'] = name_report
        df_melted['name_pharm_chain'] = name_pharm_chain
        df_melted['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for col in FINAL_COLUMNS:
            if col not in df_melted.columns:
                df_melted[col] = None
                
        df_report = df_melted[FINAL_COLUMNS]
        df_report = df_report.where(pd.notna(df_report), None)
        
        loger.info(f"Парсинг отчета '{name_report}' успешно завершен. Строк: {len(df_report)}")
        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_remains(path: str, name_report: str, name_pharm_chain: str) -> dict:
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")
        
        start_date, end_date = _get_dates_from_filename(path, loger)

        df_raw = pd.read_excel(path, header=None, dtype=str)

        product_names = df_raw.iloc[2, 2:].values
        product_codes = df_raw.iloc[3, 2:].values
        
        df_data = df_raw.iloc[4:].copy()
        
        df_data.rename(columns={0: 'pharmacy_name', 1: 'pharmacy_code'}, inplace=True)
        
        if 'pharmacy_name' in df_data.columns:
            df_data = df_data[df_data['pharmacy_name'].astype(str).str.strip() != 'Все']
        
        cols_count = 2 + len(product_names)
        df_data = df_data.iloc[:, :cols_count]
        
        value_vars = list(df_data.columns[2:])
        
        df_melted = df_data.melt(
            id_vars=['pharmacy_name', 'pharmacy_code'],
            value_vars=value_vars,
            var_name='col_index',
            value_name='quantity'
        )
        
        col_to_name = {col: str(product_names[i]).strip() for i, col in enumerate(value_vars)}
        col_to_code = {col: str(product_codes[i]).strip() for i, col in enumerate(value_vars)}
        
        df_melted['product_name'] = df_melted['col_index'].map(col_to_name)
        df_melted['product_code'] = df_melted['col_index'].map(col_to_code)
        
        df_melted.drop(columns=['col_index'], inplace=True)
        
        df_melted['quantity'] = pd.to_numeric(df_melted['quantity'], errors='coerce').fillna(0)
        df_melted = df_melted[df_melted['quantity'] != 0]
        
        df_melted['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_melted))]
        df_melted['name_report'] = name_report
        df_melted['name_pharm_chain'] = name_pharm_chain
        df_melted['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df_melted['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df_melted['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for col in FINAL_COLUMNS:
            if col not in df_melted.columns:
                df_melted[col] = None
                
        df_report = df_melted[FINAL_COLUMNS]
        df_report = df_report.where(pd.notna(df_report), None)
        
        loger.info(f"Парсинг отчета '{name_report}' успешно завершен. Строк: {len(df_report)}")
        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_xls(path, name_report, name_pharm_chain) -> dict:
    loger = LoggingMixin().log
    loger.info(f"Диспетчер 'Живика' получил задачу: '{name_report}' для '{name_pharm_chain}' из файла '{path}'")

    report_type_lower = name_report.lower()

    if 'закуп' in report_type_lower:
        return extract_custom(path, name_report, name_pharm_chain)
    elif 'продажи' in report_type_lower:
        return extract_sales(path, name_report, name_pharm_chain)
    elif 'остатки' in report_type_lower:
        return extract_remains(path, name_report, name_pharm_chain)
    else:
        loger.warning(f"Неизвестный тип отчета для 'Живика': '{name_report}'. Парсер не будет вызван.")
        return {}

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    main_loger.info("Запуск локального теста для парсера 'Живика'.")
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты_аптек\Живика\продажи\2025\03_2025.xlsx'
    test_report_type = 'Продажи'

    if os.path.exists(test_file_path):
        main_loger.info(f"Тестовый файл найден: {test_file_path}")
        try:
            result = extract_xls(path=test_file_path, name_report=test_report_type, name_pharm_chain='Живика')
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