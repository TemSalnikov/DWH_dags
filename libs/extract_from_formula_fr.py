import pandas as pd
import uuid
import os
import calendar
import re
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin

FINAL_COLUMNS = [
    'uuid_report', 'product_name', 'quantity', 'pharmacy_count',
    'name_report', 'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm'
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

def _extract_report(path: str, name_report: str, name_pharm_chain: str, col_keywords: dict) -> dict:
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")
        start_date, end_date = _get_dates_from_filename(path, loger)

        df_raw = pd.read_excel(path, header=None, dtype=str)
        
        header_row_index = -1
        col_indices = {}
        
        for i, row in df_raw.head(20).iterrows():
            row_vals = [str(v).strip() for v in row.values]
            row_vals_lower = [v.lower() for v in row_vals]
            
            if 'птовар' in row_vals_lower:
                header_row_index = i
                
                for idx, val in enumerate(row_vals):
                    val_lower = val.lower()
                    if val_lower == 'птовар':
                        col_indices['product_name'] = idx
                    elif val_lower == col_keywords['quantity'].lower():
                        col_indices['quantity'] = idx
                    elif 'количество аптек' in val_lower:
                        col_indices['pharmacy_count'] = idx
                break
        
        if header_row_index == -1:
            raise ValueError("Не удалось найти строку с заголовками (ожидалось 'ПТовар').")

        if 'product_name' not in col_indices:
             raise ValueError("Не найдена колонка 'ПТовар'.")
        if 'quantity' not in col_indices:
             raise ValueError(f"Не найдена колонка количества ('{col_keywords['quantity']}').")

        # Извлечение данных
        df = df_raw.iloc[header_row_index + 1:].copy()
        
        # Переименование колонок по индексам
        new_col_names = {}
        for col_name, idx in col_indices.items():
            new_col_names[df.columns[idx]] = col_name
            
        df.rename(columns=new_col_names, inplace=True)
        
        # Оставляем только найденные колонки
        df = df[list(new_col_names.values())]

        # Очистка данных
        df.dropna(subset=['product_name'], inplace=True)
        df = df[df['product_name'].astype(str).str.strip() != '']
        
        # Технические поля
        df['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df['name_report'] = name_report
        df['name_pharm_chain'] = name_pharm_chain
        df['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Заполнение недостающих колонок
        for col in FINAL_COLUMNS:
            if col not in df.columns:
                df[col] = None
                
        df_report = df[FINAL_COLUMNS]
        df_report = df_report.replace({pd.NA: None, float('nan'): None, 'nan': None})
        
        loger.info(f"Парсинг завершен. Строк: {len(df_report)}")
        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}': {e}", exc_info=True)
        raise


def extract_sales(path: str, name_report: str, name_pharm_chain: str) -> dict:
    return _extract_report(path, name_report, name_pharm_chain, {'quantity': 'Количество'})

def extract_remains(path: str, name_report: str, name_pharm_chain: str) -> dict:
    return _extract_report(path, name_report, name_pharm_chain, {'quantity': 'ОстатокРозницы'})

def extract_xls(path, name_report, name_pharm_chain) -> dict:
    """
    Диспетчер, вызывающий нужный парсер для 'Формула ФР' в зависимости от типа отчета.
    """
    loger = LoggingMixin().log
    loger.info(f"Диспетчер 'Формула ФР' получил задачу: '{name_report}' для '{name_pharm_chain}' из файла '{path}'")

    report_type_lower = name_report.lower()

    if 'продажи' in report_type_lower:
        return extract_sales(path, name_report, name_pharm_chain)
    elif 'остатки' in report_type_lower:
        return extract_remains(path, name_report, name_pharm_chain)
    else:
        loger.warning(f"Неизвестный тип отчета для 'Формула ФР': '{name_report}'. Парсер не будет вызван.")
        return {}

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    main_loger.info("Запуск локального теста для парсера 'Формула ФР'.")
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты\Формула ФР\Продажи\2025\02_2025.xlsx'
    test_report_type = 'Продажи'

    if os.path.exists(test_file_path):
        main_loger.info(f"Тестовый файл найден: {test_file_path}")
        try:
            result = extract_xls(path=test_file_path, name_report=test_report_type, name_pharm_chain='Формула ФР')
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