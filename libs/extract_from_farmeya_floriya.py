import pandas as pd
import uuid
import os
import calendar
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

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

def _extract_base(path: str, name_report: str, name_pharm_chain: str, rename_map: dict) -> dict:
    """
    Базовый парсер для отчетов 'Фармея Флория'.
    """
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")
        start_date, end_date = _get_dates_from_filename(path, loger)
        
        xls = pd.ExcelFile(path)
        target_sheet = None
        
        if 'TDSheet' in xls.sheet_names:
            target_sheet = 'TDSheet'
        elif len(xls.sheet_names) == 1:
            target_sheet = xls.sheet_names[0]
        else:
            loger.warning(f"Лист 'TDSheet' не найден, и в файле несколько листов: {xls.sheet_names}. Используем первый лист.")
            target_sheet = xls.sheet_names[0]
            
        loger.info(f"Используем лист: {target_sheet}")
        df_raw = pd.read_excel(xls, sheet_name=target_sheet, header=None, dtype=str)

        header_row_index = -1
        required_cols = ['Товар', 'Количество']
        
        for i, row in df_raw.head(20).iterrows():
            row_values = [str(v).strip() for v in row.values]
            if all(col in row_values for col in required_cols):
                header_row_index = i
                loger.info(f"Строка с заголовками найдена по индексу: {header_row_index}")
                break
        
        if header_row_index == -1:
            raise ValueError("Не удалось найти строку с заголовками в файле (ожидалось 'Товар', 'Количество').")

        headers = df_raw.iloc[header_row_index]
        df = df_raw.iloc[header_row_index + 1:].copy()
        df.columns = [str(h).strip() for h in headers]
        
        df.dropna(how='all', inplace=True)
        df.reset_index(drop=True, inplace=True)

        loger.info(f'Успешно получено {len(df)} строк!')

        df.rename(columns=rename_map, inplace=True)

        df['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df['name_report'] = name_report
        df['name_pharm_chain'] = name_pharm_chain
        df['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        final_columns = [
            'uuid_report', 'product_name', 'supplier', 'warehouse_address',
            'quantity', 'name_report', 'name_pharm_chain',
            'start_date', 'end_date', 'processed_dttm'
        ]

        for col in final_columns:
            if col not in df.columns:
                df[col] = None

        df_report = df[final_columns]
        df_report = df_report.replace({pd.NA: None, pd.NaT: None, '': None, 'nan': None})

        loger.info(f"Парсинг отчета '{name_report}' успешно завершен.")
        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_custom(path: str, name_report: str, name_pharm_chain: str) -> dict:
    """
    Парсер для отчета 'Закупки' от 'Фармея Флория'.
    """
    rename_map = {
        'Поставщик': 'supplier',
        'Товар': 'product_name',
        'Склад.Адрес': 'warehouse_address',
        'Количество': 'quantity'
    }
    return _extract_base(path, name_report, name_pharm_chain, rename_map)

def extract_sales(path: str, name_report: str, name_pharm_chain: str) -> dict:
    """
    Парсер для отчета 'Продажи' от 'Фармея Флория'.
    """
    rename_map = {
        'Товар': 'product_name',
        'Склад.Адрес': 'warehouse_address',
        'Количество': 'quantity'
    }
    return _extract_base(path, name_report, name_pharm_chain, rename_map)

def extract_remains(path: str, name_report: str, name_pharm_chain: str) -> dict:
    """
    Парсер для отчета 'Остатки' от 'Фармея Флория'.
    """
    rename_map = {
        'Товар': 'product_name',
        'Склад.Адрес': 'warehouse_address',
        'Количество': 'quantity'
    }
    return _extract_base(path, name_report, name_pharm_chain, rename_map)

def extract_xls(path, name_report, name_pharm_chain) -> dict:
    """
    Диспетчер, вызывающий нужный парсер для 'Фармея Флория' в зависимости от типа отчета.
    """
    loger = LoggingMixin().log
    loger.info(f"Диспетчер 'Фармея Флория' получил задачу: '{name_report}' для '{name_pharm_chain}' из файла '{path}'")

    report_type_lower = name_report.lower()

    if 'закуп' in report_type_lower:
        return extract_custom(path, name_report, name_pharm_chain)
    elif 'продажи' in report_type_lower:
        return extract_sales(path, name_report, name_pharm_chain)
    elif 'остатки' in report_type_lower:
        return extract_remains(path, name_report, name_pharm_chain)
    else:
        loger.warning(f"Неизвестный тип отчета для 'Фармея Флория': '{name_report}'. Парсер не будет вызван.")
        return {}

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    main_loger.info("Запуск локального теста для парсера-заглушки 'Фармея Флория'.")
    
    test_file_path = r'C:\Users\nmankov\Desktop\отчеты\Фармея Флория\Закуп\2024\01_2024.xlsx'
    test_report_type = 'Закупки'
    pharm_chain_name = 'Фармея Флория'

    main_loger.info(f"Тестовый файл: {test_file_path}")
    try:
        result = extract_xls(path=test_file_path, name_report=test_report_type, name_pharm_chain=pharm_chain_name)
        df = result.get('table_report')
        
        if df is not None and not df.empty:
            output_filename = f"{os.path.splitext(os.path.basename(test_file_path))[0]}_result.csv"
            output_path = os.path.join(os.path.dirname(test_file_path), output_filename)
            df.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
            main_loger.info(f"Результат успешно сохранен в: {output_path}")
        elif df is not None:
            main_loger.warning("Результирующий датафрейм пуст.")
            
    except Exception as e:
        main_loger.error(f"Во время локального теста произошла ошибка: {e}", exc_info=True)