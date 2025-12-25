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
        # Ожидаемый формат имени файла: ММ_ГГГГ (как в магните)
        report_date = datetime.strptime(date_part, "%m_%Y")
        start_date = report_date.replace(day=1)
        _, last_day = calendar.monthrange(report_date.year, report_date.month)
        end_date = report_date.replace(day=last_day)
        loger.info(f"Определен период отчета по имени файла: {start_date.date()} - {end_date.date()}")
        return start_date, end_date
    except Exception as e:
        loger.error(f"Не удалось определить дату из имени файла '{os.path.basename(path)}'. Ошибка: {e}")
        # Возвращаем текущий месяц как заглушку
        now = datetime.now()
        return now.replace(day=1), now

def _extract_base(path: str, name_report: str, name_pharm_chain: str, rename_map: dict) -> dict:
    loger = LoggingMixin().log
    loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")

    try:
        start_date, end_date = _get_dates_from_filename(path, loger)

        filename = os.path.basename(path)
        file_name_no_ext = os.path.splitext(filename)[0]
        month_part = file_name_no_ext.split('_')[0]

        xls = pd.ExcelFile(path)
        sheet_names = xls.sheet_names
        target_sheets = [s for s in sheet_names if month_part in s]
        
        if not target_sheets:
            loger.warning(f"Не найден лист с месяцем '{month_part}'. Доступные листы: {sheet_names}. Используем первый лист.")
            target_sheets = [sheet_names[0]]
        
        loger.info(f"Будут обработаны листы: {target_sheets}")
        
        all_dfs = []
        for sheet in target_sheets:
            df_raw = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str)

            header_row_index = -1
            search_cols = ['Товар', 'Аптека', 'Код товара']
            for i, row in df_raw.head(20).iterrows():
                row_values = [str(v).strip() for v in row.values]
                if any(col in row_values for col in search_cols):
                    header_row_index = i
                    break
            
            if header_row_index == -1:
                loger.warning(f"Не удалось найти строку с заголовками на листе '{sheet}'. Лист пропущен.")
                continue

            headers = df_raw.iloc[header_row_index]
            df = df_raw.iloc[header_row_index + 1:].copy()
            df.columns = [str(h).strip() for h in headers]

            df.dropna(how='all', inplace=True)
            
            if 'Товар' in df.columns:
                df = df[~df['Товар'].astype(str).str.lower().str.contains('итог', na=False)]
                df = df[df['Товар'].notna()]
            
            all_dfs.append(df)

        if not all_dfs:
            raise ValueError("Не удалось извлечь данные ни с одного листа.")
            
        df = pd.concat(all_dfs, ignore_index=True)

        df.rename(columns=rename_map, inplace=True)

        df['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df['name_report'] = name_report
        df['name_pharm_chain'] = name_pharm_chain
        df['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        final_columns = [
            'uuid_report', 'marketing_marker', 'pharmacy_name', 'product_name', 'product_code',
            'barcode', 'supplier', 'supplier_inn', 'manufacturer', 'price',
            'purchase_quantity', 'purchase_sum_opt', 'purchase_sum', 
            'sale_quantity', 'sale_sum_opt', 'sale_sum',
            'stock_quantity', 'stock_sum_opt', 'stock_sum',
            'expiration_date', 'mnn',
            'name_report', 'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm'
        ]

        for col in final_columns:
            if col not in df.columns:
                df[col] = None

        df_report = df[final_columns]
        df_report = df_report.replace({pd.NA: None, float('nan'): None, 'nan': None})

        loger.info(f"Парсинг завершен. Строк: {len(df_report)}")
        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при парсинге: {e}", exc_info=True)
        raise

def extract_purchases_and_sales(path: str, name_report: str, name_pharm_chain: str) -> dict:

    rename_map = {
        'Маркетинг': 'marketing_marker',
        'Аптека': 'pharmacy_name',
        'Товар': 'product_name',
        'Код товара': 'product_code',
        'Штрихкод': 'barcode',
        'Поставщик': 'supplier',
        'ИНН поставщика': 'supplier_inn',
        'Изготовитель': 'manufacturer',
        'Цена отч': 'price',
        'Кол-во приход': 'purchase_quantity',
        'Сумма опт.приход': 'purchase_sum_opt',
        'Сумма отч.приход': 'purchase_sum',
        'Кол-во продаж': 'sale_quantity',
        'Сумма опт.продаж': 'sale_sum_opt',
        'Сумма отч.продаж': 'sale_sum',
        'Кол-во кон': 'stock_quantity',
        'Сумм опт.кон': 'stock_sum_opt',
        'Сумма отч.кон': 'stock_sum',
        'Срок годности': 'expiration_date',
        'МНН': 'mnn'
    }
    return _extract_base(path, name_report, name_pharm_chain, rename_map)

def extract_xls(path, name_report, name_pharm_chain) -> dict:
    """
    Диспетчер для 'Аптеки Удмуртии (Госаптека)'.
    """
    loger = LoggingMixin().log
    loger.info(f"Диспетчер 'Аптеки Удмуртии' получил задачу: '{name_report}' для '{name_pharm_chain}' из файла '{path}'")

    report_type_lower = name_report.lower()

    if 'закуп' in report_type_lower or 'продажи' in report_type_lower:
        return extract_purchases_and_sales(path, name_report, name_pharm_chain)
    else:
        loger.warning(f"Неизвестный тип отчета для 'Аптеки Удмуртии': '{name_report}'. Парсер не будет вызван.")
        return {}

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    main_loger.info("Запуск локального теста для парсера 'Аптеки Удмуртии (Госаптека)'.")
    # Укажите путь к тестовому файлу
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты_аптек\Аптеки удмуртии(Госаптека)\Закуп+продажи\05_2025.xlsx'
    test_report_type = 'Закуп+Продажи'

    if os.path.exists(test_file_path):
        main_loger.info(f"Тестовый файл найден: {test_file_path}")
        try:
            result = extract_xls(path=test_file_path, name_report=test_report_type, name_pharm_chain='Аптеки Удмуртии (Госаптека)')
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