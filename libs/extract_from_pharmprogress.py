import pandas as pd
import uuid
import os
import calendar
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
from dateutil.relativedelta import relativedelta

def _get_dates_from_filename(path: str, loger) -> tuple[datetime, datetime]:
    try:
        filename = os.path.basename(path)
        date_part = os.path.splitext(filename)[0]
        try:
            report_date = datetime.strptime(date_part, "%m_%Y")
        except ValueError:
            report_date = datetime.strptime(date_part, "%m.%Y")
        start_date = (report_date - relativedelta(months=2)).replace(day=1)
        _, last_day = calendar.monthrange(report_date.year, report_date.month)
        end_date = report_date.replace(day=last_day)
        loger.info(f"Определен период отчета по имени файла: {start_date.date()} - {end_date.date()}")
        return start_date, end_date
    except Exception as e:
        loger.error(f"Не удалось определить дату из имени файла '{os.path.basename(path)}'. Ожидаемый формат: ММ_ГГГГ.xlsx или ММ.ГГГГ.xlsx. Ошибка: {e}")
        raise

def _extract_base(path: str, name_report: str, name_pharm_chain: str, rename_map: dict) -> dict:
    """
    Базовый парсер для отчетов 'Фармпрогресс'.
    """
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")
        start_date, end_date = _get_dates_from_filename(path, loger)

        df_raw = pd.read_excel(path, header=None, dtype=str)

        header_row_index = -1
        search_marker = 'Наименование товара'
        for i, row in df_raw.iterrows():
            if search_marker in row.astype(str).values:
                header_row_index = i
                loger.info(f"Строка с заголовками найдена по индексу: {header_row_index}")
                break
        
        if header_row_index == -1:
            raise ValueError(f"Не удалось найти строку с заголовками в файле (ожидалось слово '{search_marker}').")

        headers = df_raw.iloc[header_row_index]
        df = df_raw.iloc[header_row_index + 1:].copy()
        df.columns = headers
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

        # Унификация колонок
        final_columns = [
            'uuid_report', 'period', 'product_name', 'manufacturer', 'portfolio',
            'purchase_quantity', 'purchase_sum', 'price', 'supplier',
            'remains_quantity', 'remains_sum', 'pharmacy_name', 'pharmacy_category',
            'legal_entity', 'pharmacy_address',
            'sale_quantity', 'sale_sum', 'name_report', 'name_pharm_chain',
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

def extract_purchases(path: str, name_report: str, name_pharm_chain: str) -> dict:
    """
    Парсер для отчета 'Закупки' от 'Фармпрогресс'.
    """
    rename_map = {
        'Месяц, год': 'period',
        'Наименование товара': 'product_name',
        'Производитель': 'manufacturer',
        'Кол-во': 'purchase_quantity',
        'Контрактная цена': 'price',
        'Сумма (контрактные цены)': 'purchase_sum',
        'Поставщик': 'supplier',
        'Портфель': 'portfolio'
    }
    return _extract_base(path, name_report, name_pharm_chain, rename_map)

def extract_sales(path: str, name_report: str, name_pharm_chain: str) -> dict:
    """
    Парсер для отчета 'Продажи' от 'Фармпрогресс'.
    """
    rename_map = {
        'Месяц, год': 'period',
        'Наименование товара': 'product_name',
        'Производитель': 'manufacturer',
        'Кол-во': 'sale_quantity',
        'Контрактная цена': 'price',
        'Сумма (контрактные цены)': 'sale_sum',
        'Наименование аптеки': 'pharmacy_name',
        'Юридическое лицо': 'legal_entity',
        'Адрес аптеки': 'pharmacy_address',
        'Портфель': 'portfolio'
    }
    return _extract_base(path, name_report, name_pharm_chain, rename_map)

def extract_remains(path: str, name_report: str, name_pharm_chain: str) -> dict:
    """
    Парсер для отчета 'Остатки' от 'Фармпрогресс'.
    """
    rename_map = {
        'Наименование товара': 'product_name',
        'Производитель': 'manufacturer',
        'Кол-во': 'remains_quantity',
        'Сумма (контрактные цены)': 'remains_sum',
        'Контрактная цена': 'price',
        'Наименование аптеки': 'pharmacy_name',
        'Категория аптеки': 'pharmacy_category',
        'Юридическое лицо': 'legal_entity',
        'Адрес аптеки': 'pharmacy_address',
        'Портфель': 'portfolio'
    }
    return _extract_base(path, name_report, name_pharm_chain, rename_map)

def extract_xls(path, name_report, name_pharm_chain) -> dict:
    """
    Диспетчер, вызывающий нужный парсер для 'Фармпрогресс' в зависимости от типа отчета.
    """
    loger = LoggingMixin().log
    loger.info(f"Диспетчер 'Фармпрогресс' получил задачу: '{name_report}' для '{name_pharm_chain}' из файла '{path}'")

    report_type_lower = name_report.lower()

    if 'закуп' in report_type_lower:
        return extract_purchases(path, name_report, name_pharm_chain)
    elif 'продажи' in report_type_lower:
        return extract_sales(path, name_report, name_pharm_chain)
    elif 'остатки' in report_type_lower:
        return extract_remains(path, name_report, name_pharm_chain)
    else:
        loger.warning(f"Неизвестный тип отчета для 'Фармпрогресс': '{name_report}'. Парсер не будет вызван.")
        return {}

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    main_loger.info("Запуск локального теста для парсера 'Фармпрогресс'.")
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты\ФармПрогресс\Продажи\2024\09_2024.xlsx'
    test_report_type = 'Продажи'

    if os.path.exists(test_file_path):
        main_loger.info(f"Тестовый файл найден: {test_file_path}")
        try:
            result = extract_xls(path=test_file_path, name_report=test_report_type, name_pharm_chain='Фармпрогресс')
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