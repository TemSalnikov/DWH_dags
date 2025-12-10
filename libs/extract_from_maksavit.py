import pandas as pd
import uuid
import os
import calendar
import re
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin

def _get_dates(path: str, df_raw: pd.DataFrame, loger) -> tuple[datetime, datetime]:
    """
    Определяет начальную и конечную дату отчета.
    Сначала пытается извлечь из имени файла (ММ_ГГГГ.xlsx).
    Если не удается, ищет дату в содержимом файла в строке 'Период:'.
    """
    try:
        filename = os.path.basename(path)
        date_part = os.path.splitext(filename)[0]
        report_date = datetime.strptime(date_part, "%m_%Y")
        loger.info(f"Дата отчета определена из имени файла: {report_date.strftime('%m-%Y')}")
    except ValueError:
        loger.info("Не удалось определить дату из имени файла. Поиск в содержимом...")
        report_date = None
        for i in range(min(10, len(df_raw))):
            for cell in df_raw.iloc[i].astype(str):
                cell_lower = cell.lower()
                if 'период:' in cell_lower:
                    match_dmy = re.search(r'(\d{2}\.\d{2}\.\d{4})', cell)
                    if match_dmy:
                        report_date = datetime.strptime(match_dmy.group(1), "%d.%m.%Y")
                        loger.info(f"Дата отчета найдена в ячейке: {report_date.strftime('%d.%m.%Y')}")
                        break
                    
                    match_month_year = re.search(r'период:\s*([а-я]+)\s*(\d{4})', cell_lower)
                    if match_month_year:
                        month_name = match_month_year.group(1)
                        year = int(match_month_year.group(2))
                        months_map = {
                            'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4, 'май': 5, 'июнь': 6,
                            'июль': 7, 'август': 8, 'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
                        }
                        if month_name in months_map:
                            report_date = datetime(year, months_map[month_name], 1)
                            loger.info(f"Дата отчета найдена в ячейке в формате 'Месяц Год': {report_date.strftime('%m-%Y')}")
                            break
            if report_date:
                break
        if not report_date:
            loger.error(f"Не удалось определить дату ни из имени файла, ни из содержимого. Файл: '{os.path.basename(path)}'")
            raise ValueError("Дата отчета не найдена.")

    start_date = report_date.replace(day=1)
    _, last_day = calendar.monthrange(report_date.year, report_date.month)
    end_date = report_date.replace(day=last_day)
    loger.info(f"Определен период отчета: {start_date.date()} - {end_date.date()}")
    return start_date, end_date

def _extract_base(path: str, name_report: str, name_pharm_chain: str, rename_map: dict) -> dict:
    """
    Базовый парсер для отчетов 'Максавит'.
    """
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")
        
        df_raw = pd.read_excel(path, header=None, dtype=str)
        start_date, end_date = _get_dates(path, df_raw, loger)

        header_row_index = -1
        for i, row in df_raw.iterrows():
            if 'Номенклатура' in row.astype(str).values:
                header_row_index = i
                loger.info(f"Строка с заголовками найдена по индексу: {header_row_index}")
                break
        
        if header_row_index == -1:
            raise ValueError("Не удалось найти строку с заголовками в файле (ожидалось слово 'Номенклатура').")

        headers = df_raw.iloc[header_row_index]
        df = df_raw.iloc[header_row_index + 1:].copy()
        df.columns = headers
        df.columns = [str(col).lower().strip() for col in df.columns]
        df.dropna(how='all', inplace=True)
        df.reset_index(drop=True, inplace=True)

        initial_rows = len(df)
        df = df[~df.apply(lambda row: row.astype(str).str.contains('Итого', case=False, na=False).any(), axis=1)]
        loger.info(f"Удалено {initial_rows - len(df)} строк, содержащих 'Итого'.")

        loger.info(f'Успешно получено {len(df)} строк!')

        df.rename(columns=rename_map, inplace=True)

        df['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df['name_report'] = name_report
        df['name_pharm_chain'] = name_pharm_chain
        df['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        final_columns = [
            'uuid_report', 'legal_entity', 'supplier', 'warehouse_name', 'product_name',
            'purchase_quantity', 'sale_quantity', 'remains_quantity', 'expiration_date',
            'name_report', 'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm',
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
    """Парсер для отчета 'Закупки' от 'Максавит'."""
    rename_map = {
        'юридическое лицо компании': 'legal_entity',
        'контрагент': 'supplier',
        'подразделение компании': 'warehouse_name',
        'номенклатура': 'product_name',
        'количество': 'purchase_quantity'
    }
    return _extract_base(path, name_report, name_pharm_chain, rename_map)

def extract_sales(path: str, name_report: str, name_pharm_chain: str) -> dict:
    """Парсер для отчета 'Продажи' от 'Максавит'."""
    rename_map = {
        'юридическое лицо компании': 'legal_entity',
        'подразделение компании': 'warehouse_name',
        'номенклатура': 'product_name',
        'количество уп.': 'sale_quantity'
    }
    return _extract_base(path, name_report, name_pharm_chain, rename_map)

def extract_remains(path: str, name_report: str, name_pharm_chain: str) -> dict:
    """Парсер для отчета 'Остатки' от 'Максавит'."""
    rename_map = {
        'юридическое лицо компании': 'legal_entity',
        'контрагент': 'supplier',
        'подразделение компании': 'warehouse_name',
        'номенклатура': 'product_name',
        'количество': 'remains_quantity',
        'срок годности': 'expiration_date'
    }
    return _extract_base(path, name_report, name_pharm_chain, rename_map)

def extract_xls(path: str, name_report: str, name_pharm_chain: str) -> dict:
    """Диспетчер, вызывающий нужный парсер для 'Максавит' в зависимости от типа отчета."""
    loger = LoggingMixin().log
    loger.info(f"Диспетчер 'Максавит' получил задачу: '{name_report}' для '{name_pharm_chain}' из файла '{path}'")
    report_type_lower = name_report.lower()
    if 'закуп' in report_type_lower:
        return extract_purchases(path, name_report, name_pharm_chain)
    elif 'продажи' in report_type_lower:
        return extract_sales(path, name_report, name_pharm_chain)
    elif 'остатки' in report_type_lower:
        return extract_remains(path, name_report, name_pharm_chain)
    else:
        loger.warning(f"Неизвестный тип отчета для 'Максавит': '{name_report}'. Парсер не будет вызван.")
        return {}

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    main_loger.info("Запуск локального теста для парсера 'Максавит'.")
    test_file_path = r'C:\Users\nmankov\Desktop\отчеты\Максавит\Продажи\2025\01_2025.xls'
    test_report_type = 'Продажи'
    if os.path.exists(test_file_path):
        main_loger.info(f"Тестовый файл найден: {test_file_path}")
        try:
            result = extract_xls(path=test_file_path, name_report=test_report_type, name_pharm_chain='Максавит')
            df = result.get('table_report')

            if df is not None and not df.empty:
                output_filename = f"{os.path.splitext(os.path.basename(test_file_path))[0]}_result.csv"
                output_path = os.path.join(os.path.dirname(test_file_path), output_filename)
                df.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
                main_loger.info(f"Парсинг прошел успешно. Результат сохранен в: {output_path}")
            else:
                main_loger.error("Тест 'Максавит' провален: результат пустой.")
        except Exception as e:
            main_loger.error(f"Во время локального теста произошла ошибка: {e}", exc_info=True)
    else:
        main_loger.warning(f"Тестовый файл не найден по пути: {test_file_path}. Укажите корректный путь в `test_file_path`.")