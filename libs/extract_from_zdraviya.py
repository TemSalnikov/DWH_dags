import pandas as pd
import uuid
import os
import calendar
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin

FINAL_COLUMNS = [
    'uuid_report', 'legal_entity', 'inn', 'pharmacy_address',
    'product_code', 'product_name', 'supplier',
    'purchase_quantity', 'sale_quantity', 'stock_quantity',
    'name_report', 'name_pharm_chain',
    'start_date', 'end_date', 'processed_dttm'
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

def extract_xls(path, name_report, name_pharm_chain) -> dict:
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")
        start_date, end_date = _get_dates_from_filename(path, loger)

        xls = pd.ExcelFile(path)
        sheet_name = None
        
        if len(xls.sheet_names) == 1:
            sheet_name = xls.sheet_names[0]
        else:
            sheet_name = next((s for s in xls.sheet_names if 'исходник' in s.lower()), None)
            if not sheet_name:
                loger.warning("Лист 'исходник' не найден, используется первый лист.")
                sheet_name = xls.sheet_names[0]

        loger.info(f"Парсинг листа: {sheet_name}")
        df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=str)

        header_row_idx = -1
        keywords = ['номенклатура', 'инн', 'код', 'адрес фактический']
        
        for i, row in df_raw.head(20).iterrows():
            row_vals = [str(v).lower().strip() for v in row.values]
            if any(k in row_vals for k in keywords):
                header_row_idx = i
                break
        
        if header_row_idx == -1:
            raise ValueError("Не удалось найти строку заголовков.")

        headers = df_raw.iloc[header_row_idx].fillna('').astype(str).str.strip()
        df = df_raw.iloc[header_row_idx + 1:].copy()
        df.columns = headers

        col_map = {}
        dynamic_cols = {'purchase': [], 'sale': [], 'stock': []}

        for col in df.columns:
            c_lower = col.lower()
            if 'адрес фактический' in c_lower: col_map['pharmacy_address'] = col
            elif 'контрагент основной' in c_lower: col_map['legal_entity'] = col
            elif 'инн' in c_lower: col_map['inn'] = col
            elif 'код' == c_lower: col_map['product_code'] = col
            elif 'номенклатура' in c_lower: col_map['product_name'] = col
            elif 'поставщик' in c_lower: col_map['supplier'] = col
            
            if 'закуп' in c_lower: dynamic_cols['purchase'].append(col)
            elif 'продаж' in c_lower: dynamic_cols['sale'].append(col)
            elif 'остат' in c_lower: dynamic_cols['stock'].append(col)

        df.rename(columns={v: k for k, v in col_map.items()}, inplace=True)

        for key, target_col in [('purchase', 'purchase_quantity'), ('sale', 'sale_quantity'), ('stock', 'stock_quantity')]:
            cols = dynamic_cols.get(key, [])
            if cols:
                numeric_df = df[cols].apply(pd.to_numeric, errors='coerce')
                df[target_col] = numeric_df.sum(axis=1, min_count=1)
            else:
                df[target_col] = pd.NA

        df = df[(df['purchase_quantity'].fillna(0) != 0) | (df['sale_quantity'].fillna(0) != 0) | (df['stock_quantity'].fillna(0) != 0)]

        if df.empty:
            loger.warning("Не найдено данных ни по одной из категорий (Закупка, Продажа, Остаток).")
            return {'table_report': pd.DataFrame()}

        df_final = df.copy()
        df_final['name_report'] = 'Закупки+Продажи+Остатки'
        
        df_final['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_final))]
        df_final['name_pharm_chain'] = name_pharm_chain
        df_final['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df_final['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df_final['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for col in FINAL_COLUMNS:
            if col not in df_final.columns:
                df_final[col] = None

        df_report = df_final[FINAL_COLUMNS]
        df_report = df_report.replace({pd.NA: None, pd.NaT: None, '': None, 'nan': None})
        df_report = df_report.where(pd.notna(df_report), None)

        loger.info(f"Парсинг отчета '{name_report}' завершен. Строк: {len(df_report)}")
        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    main_loger.info("Запуск локального теста для парсера 'Здравия'.")
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты_аптек\Здравия\Закуп+продажи\09_2025.xlsx'
    test_report_type = 'Продажи'

    if os.path.exists(test_file_path):
        main_loger.info(f"Тестовый файл найден: {test_file_path}")
        try:
            result = extract_xls(path=test_file_path, name_report=test_report_type, name_pharm_chain='Здравия')
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