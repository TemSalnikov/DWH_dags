import pandas as pd
import uuid
import os
import calendar
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin
from dateutil.relativedelta import relativedelta

FINAL_COLUMNS = [
    'uuid_report',
    'product_code',
    'product_name',
    'supplier_code',
    'supplier',
    'quantity_warehouse',
    'quantity_pharmacy',
    'pharmacy_inn',
    'invoice_number',
    'invoice_date',
    'pharmacy_name',
    'quantity',
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

def _extract_base(path: str, name_report: str, name_pharm_chain: str, rename_map: dict, anchor='Наименование товара') -> dict:
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")
        start_date, end_date = _get_dates_from_filename(path, loger)

        if 'остатки' in name_report.lower():
            start_date = start_date - relativedelta(months=2)
            loger.info(f"Скорректирована дата начала для отчета 'Остатки': {start_date.date()}")
        
        xls = pd.ExcelFile(path)
        sheet_names = xls.sheet_names
        target_sheet = sheet_names[0]

        if len(sheet_names) > 1:
            data_sheet = next((s for s in sheet_names if s.lower() == 'данные'), None)
            if data_sheet:
                target_sheet = data_sheet
            else:
                loger.warning(f"В файле несколько листов {sheet_names}, но лист 'данные' не найден. Используем первый лист.")
        
        df_raw = pd.read_excel(xls, sheet_name=target_sheet, header=None, dtype=str)
        
        header_row_index = -1
        anchors = [anchor] if isinstance(anchor, str) else anchor
        for i, row in df_raw.head(20).iterrows():
            row_values = [str(v).strip() for v in row.values]
            if any(a in row_values for a in anchors):
                header_row_index = i
                loger.info(f"Строка с заголовками найдена по индексу: {header_row_index}")
                break
        
        if header_row_index == -1:
             raise ValueError(f"Не удалось найти строку с заголовками (ожидалось одно из: {anchors}).")

        headers = df_raw.iloc[header_row_index]
        df = df_raw.iloc[header_row_index + 1:].copy()
        df.columns = [str(h).strip() for h in headers]
        
        df.dropna(how='all', inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        for excel_col, db_col in rename_map.items():
            if excel_col in df.columns:
                df.rename(columns={excel_col: db_col}, inplace=True)
        
        df['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df['name_report'] = name_report
        df['name_pharm_chain'] = name_pharm_chain
        df['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for col in FINAL_COLUMNS:
            if col not in df.columns:
                df[col] = None

        df_report = df[FINAL_COLUMNS].copy()
        df_report = df_report.replace({pd.NA: None, pd.NaT: None, '': None, 'nan': None})

        loger.info(f"Парсинг отчета '{name_report}' успешно завершен. Строк: {len(df_report)}")
        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_purchases(path: str, name_report: str, name_pharm_chain: str) -> dict:
    rename_map = {
        'Код товара': 'product_code',
        'Наименование товара': 'product_name',
        'Код поставщика': 'supplier_code',
        'Наименование поставщика': 'supplier',
        'Кол-во прихода склад': 'quantity_warehouse',
        'Кол-во закупа на аптеки': 'quantity_pharmacy',
        'ИНН аптеки': 'pharmacy_inn',
        'Накладная': 'invoice_number',
        'Дата накладной': 'invoice_date'
    }
    return _extract_base(path, name_report, name_pharm_chain, rename_map, anchor='Наименование товара')

def extract_sales(path: str, name_report: str, name_pharm_chain: str) -> dict:
    rename_map = {
        'Код товара': 'product_code',
        'Наименование товара': 'product_name',
        'Наименование аптеки': 'pharmacy_name',
        'Кол-во продаж с аптеки': 'quantity',
        'Сумма по полю Кол-во расхода с аптеки': 'quantity',
        'Сумма по полю Кол-во продаж с аптеки': 'quantity'
    }
    return _extract_base(path, name_report, name_pharm_chain, rename_map, anchor=['Наименование товара', 'Код товара'])

def extract_remains(path: str, name_report: str, name_pharm_chain: str) -> dict:
    rename_map = {
        'Код товара': 'product_code',
        'Наименование товара': 'product_name',
        'Товар': 'product_name',
        'Наименование аптеки': 'pharmacy_name',
        'Аптека': 'pharmacy_name',
        'Наименование поставщика': 'supplier',
        'Остаток аптека (кол-во)': 'quantity_pharmacy',
        'Остаток аптек (кол-во)': 'quantity_pharmacy',
        'Остаток аптека': 'quantity_pharmacy',
        'Ост скл зар+скл ост на прод (кол-во)': 'quantity_warehouse',
        'Остаток склада': 'quantity_warehouse'
    }
    return _extract_base(path, name_report, name_pharm_chain, rename_map, anchor=['Наименование товара', 'Товар', 'Код товара'])

def extract_xls(path, name_report, name_pharm_chain) -> dict:
    loger = LoggingMixin().log
    loger.info(f"Диспетчер 'Апрель' получил задачу: '{name_report}' для '{name_pharm_chain}' из файла '{path}'")

    report_type_lower = name_report.lower()

    if 'закуп' in report_type_lower:
        return extract_purchases(path, name_report, name_pharm_chain)
    elif 'продажи' in report_type_lower:
        return extract_sales(path, name_report, name_pharm_chain)
    elif 'остатки' in report_type_lower:
        return extract_remains(path, name_report, name_pharm_chain)
    else:
        loger.warning(f"Неизвестный тип отчета для 'Апрель': '{name_report}'. Парсер не будет вызван.")
        return {}

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    main_loger.info("Запуск локального теста для парсера 'Апрель'.")
    
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты_аптек\Апрель\Закуп\2024\04_2024.xlsx'
    test_report_type = 'Закупки'
    pharm_chain_name = 'Апрель'

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
        else:
            main_loger.error("Парсер вернул None.")
            
    except Exception as e:
        main_loger.error(f"Во время локального теста произошла ошибка: {e}", exc_info=True)