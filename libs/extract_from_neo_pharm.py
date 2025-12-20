import pandas as pd
import uuid
import os
import calendar
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin

def _get_dates_from_filename(path: str, loger) -> tuple[datetime, datetime]:
    try:
        filename = os.path.basename(path)
        date_part = os.path.splitext(filename)[0]
        # Ожидаемый формат имени файла: ММ_ГГГГ (например, 01_2023)
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
    Базовый парсер для отчетов 'Нео фарм'.
    """
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")
        start_date, end_date = _get_dates_from_filename(path, loger)

        df_raw = pd.read_excel(path, header=None, dtype=str)

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
        
        # Удаляем пустые строки
        df.dropna(how='all', inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        if not df.empty:
            last_val = str(df.iloc[-1, 0]).lower()
            if 'итог' in last_val or 'total' in last_val:
                df = df.iloc[:-1]
                loger.info("Удалена последняя строка (Итог).")

        loger.info(f'Успешно получено {len(df)} строк!')

        df.rename(columns=rename_map, inplace=True)

        df['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df['name_report'] = name_report
        df['name_pharm_chain'] = name_pharm_chain
        df['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Унификация колонок для всех типов отчетов
        final_columns = [
            'uuid_report', 
            'product_code', 'product_name', 
            'legal_entity', 'inn', 
            'supplier', 'sender_subdivision',
            'pharmacy_name', 
            'vendor', 'manufacturer', 
            'doc_number', 'doc_date', 
            'quantity',
            'name_report', 'name_pharm_chain',
            'start_date', 'end_date', 'processed_dttm'
        ]

        for col in final_columns:
            if col not in df.columns:
                df[col] = None

        df_report = df[final_columns]

        # Замена пустых значений и NaN на None
        df_report = df_report.replace({pd.NA: None, pd.NaT: None, '': None, 'nan': None})

        loger.info(f"Парсинг отчета '{name_report}' успешно завершен.")
        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_purchases(path: str, name_report: str, name_pharm_chain: str) -> dict:
    """
    Парсер для отчета 'Закупки' от 'Нео фарм'.
    """
    rename_map = {
        'Код номенклатуры': 'product_code',
        'Номенклатура': 'product_name',
        'Организация': 'legal_entity',
        'ИНН организации': 'inn',
        'Поставщик': 'supplier',
        'Регистратор.Подразделение отправитель': 'sender_subdivision',
        'Подразделение': 'pharmacy_name',
        'Вендор': 'vendor',
        'Производитель': 'manufacturer',
        'Номер входящего документа': 'doc_number',
        'Дата прихода в аптеку': 'doc_date',
        'Количество': 'quantity'
    }
    return _extract_base(path, name_report, name_pharm_chain, rename_map)

def extract_sales(path: str, name_report: str, name_pharm_chain: str) -> dict:
    """
    Парсер для отчета 'Продажи' от 'Нео фарм'.
    """
    rename_map = {
        'Код номенклатуры': 'product_code',
        'Номенклатура': 'product_name',
        'Подразделение': 'pharmacy_name',
        'Организация': 'legal_entity',
        'Количество': 'quantity'
    }
    return _extract_base(path, name_report, name_pharm_chain, rename_map)

def extract_remains(path: str, name_report: str, name_pharm_chain: str) -> dict:
    """
    Парсер для отчета 'Остатки' от 'Нео фарм'.
    """
    rename_map = {
        'Код': 'product_code',
        'Номенклатура': 'product_name',
        'Подразделение': 'pharmacy_name',
        'Количество': 'quantity'
    }
    return _extract_base(path, name_report, name_pharm_chain, rename_map)

def extract_xls(path, name_report, name_pharm_chain) -> dict:
    """
    Диспетчер, вызывающий нужный парсер для 'Нео фарм' в зависимости от типа отчета.
    """
    loger = LoggingMixin().log
    loger.info(f"Диспетчер 'Нео фарм' получил задачу: '{name_report}' для '{name_pharm_chain}' из файла '{path}'")

    report_type_lower = name_report.lower()

    if 'закуп' in report_type_lower:
        return extract_purchases(path, name_report, name_pharm_chain)
    elif 'продажи' in report_type_lower:
        return extract_sales(path, name_report, name_pharm_chain)
    elif 'остатки' in report_type_lower:
        return extract_remains(path, name_report, name_pharm_chain)
    else:
        loger.warning(f"Неизвестный тип отчета для 'Нео фарм': '{name_report}'. Парсер не будет вызван.")
        return {}

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    main_loger.info("Запуск локального теста для парсера 'Нео фарм'.")
    test_file_path = r'C:\Users\nmankov\Desktop\отчеты\Нео-Фарм\Закуп\2025\02_2025.xlsx'
    test_report_type = 'Закупки'

    if os.path.exists(test_file_path):
        main_loger.info(f"Тестовый файл найден: {test_file_path}")
        try:
            result = extract_xls(path=test_file_path, name_report=test_report_type, name_pharm_chain='Нео фарм')
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