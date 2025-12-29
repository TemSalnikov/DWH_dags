import pandas as pd
import uuid
import os
import calendar
import re
from datetime import datetime, timedelta
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
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")
        loger.info(f"Парсинг отчета '{name_report}' (заглушка) завершен.")
        return {'table_report': pd.DataFrame()}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_purchases(path: str, name_report: str, name_pharm_chain: str) -> dict:
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")
        start_date, end_date = _get_dates_from_filename(path, loger)

        xls = pd.ExcelFile(path)
        sheet_names = xls.sheet_names
        
        target_sheet = None
        if "Лист 1" in sheet_names:
            target_sheet = "Лист 1"
        elif "исходник" in sheet_names:
            target_sheet = "исходник"
        else:
            loger.warning(f"Не найдены листы 'Лист 1' или 'исходник'. Доступные: {sheet_names}. Берем первый лист.")
            target_sheet = sheet_names[0]

        df_raw = pd.read_excel(xls, sheet_name=target_sheet, header=None, dtype=str)

        header_row_index = -1
        search_cols = ['Код товара', 'Наименование товара', 'Приходы|Кол-во']
        
        for i, row in df_raw.head(20).iterrows():
            row_values = [str(v).strip() for v in row.values]
            if sum(1 for c in search_cols if c in row_values) >= 2:
                header_row_index = i
                break
        
        if header_row_index == -1:
             header_row_index = 4 
             loger.warning("Заголовок не найден поиском, используется строка 5 (индекс 4).")

        headers = df_raw.iloc[header_row_index]
        df = df_raw.iloc[header_row_index + 1:].copy()
        df.columns = [str(h).strip() for h in headers]
        
        rename_map = {
            'Код товара': 'product_code',
            'Наименование товара': 'product_name',
            'Производитель': 'manufacturer',
            'Признак ЖВ': 'vital_sign',
            'Контрагент': 'contractor',
            'Филиал': 'pharmacy_name',
            'Товарная категория': 'product_category',
            'Признак маркировки': 'marking_sign',
            'Приходы|Кол-во': 'purchase_quantity',
            'Приходы|Сумма опт': 'purchase_sum_opt',
            'Приходы|Сумма розн': 'purchase_sum_retail',
            'Приходы|Наценка, %': 'markup_percent',
            'Приходы|Дата прихода': 'doc_date',
            'Приходы|Поставщик': 'supplier',
            'Приходы|Партия': 'batch',
            'Приходы|Номер документа поставщика': 'doc_number',
            'Приходы|Штрихкод производителя': 'manufacturer_barcode'
        }
        
        df.rename(columns=rename_map, inplace=True)
        df.dropna(how='all', inplace=True)
        
        df['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df['name_report'] = name_report
        df['name_pharm_chain'] = name_pharm_chain
        df['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        final_columns = [
            'uuid_report', 
            'product_code', 'product_name', 'manufacturer', 'vital_sign',
            'contractor', 'pharmacy_name', 'product_category', 'marking_sign',
            'purchase_quantity', 'purchase_sum_opt', 'purchase_sum_retail',
            'markup_percent', 'doc_date', 'supplier', 'batch', 'doc_number',
            'manufacturer_barcode',
            'name_report', 'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm'
        ]
        
        for col in final_columns:
            if col not in df.columns:
                df[col] = None
                
        df_report = df[final_columns]
        df_report = df_report.where(pd.notna(df_report), None)
        
        loger.info(f"Парсинг отчета '{name_report}' завершен. Строк: {len(df_report)}")
        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_sales(path: str, name_report: str, name_pharm_chain: str) -> dict:
    rename_map = {}
    return _extract_base(path, name_report, name_pharm_chain, rename_map)

def extract_remains(path: str, name_report: str, name_pharm_chain: str) -> dict:
    rename_map = {}
    return _extract_base(path, name_report, name_pharm_chain, rename_map)

def extract_xls(path, name_report, name_pharm_chain) -> dict:
    loger = LoggingMixin().log
    loger.info(f"Диспетчер 'Юнифарм' получил задачу: '{name_report}' для '{name_pharm_chain}' из файла '{path}'")

    report_type_lower = name_report.lower()

    if 'закуп' in report_type_lower:
        return extract_purchases(path, name_report, name_pharm_chain)
    elif 'продажи' in report_type_lower:
        return extract_sales(path, name_report, name_pharm_chain)
    elif 'остатки' in report_type_lower:
        return extract_remains(path, name_report, name_pharm_chain)
    else:
        loger.warning(f"Неизвестный тип отчета для 'Юнифарм': '{name_report}'. Парсер не будет вызван.")
        return {}

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    main_loger.info("Запуск локального теста для парсера 'Юнифарм'.")
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты_аптек\Юнифарм\Закуп\07_2025.xls'
    
    if os.path.exists(test_file_path):
        try:
            result = extract_xls(test_file_path, 'Закупки', 'Юнифарм')
            df = result.get('table_report')
            if df is not None and not df.empty:
                output_filename = f"{os.path.splitext(os.path.basename(test_file_path))[0]}_result.csv"
                output_path = os.path.join(os.path.dirname(test_file_path), output_filename)
                df.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
                main_loger.info(f"Результат успешно сохранен в: {output_path}")
        except Exception as e:
            main_loger.error(f"Ошибка при тестировании: {e}", exc_info=True)
    else:
        main_loger.warning(f"Тестовый файл не найден: {test_file_path}")