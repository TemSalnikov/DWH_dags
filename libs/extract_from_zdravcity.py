import pandas as pd
import uuid
import os
import calendar
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin

def extract_sales(path: str, name_report: str, name_pharm_chain: str) -> dict:
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")
        
        xls = pd.ExcelFile(path)
        sheet_name = None
        for sheet in xls.sheet_names:
            if 'продажи' not in sheet.lower():
                sheet_name = sheet
                break
        
        if not sheet_name:
            sheet_name = xls.sheet_names[0]

        loger.info(f"Парсинг листа: {sheet_name}")
        df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=str)
        
        header_row_index = -1
        keywords = ['код позиции', 'наименование', 'кол-во', 'сумма, руб.']
        
        for i, row in df_raw.head(20).iterrows():
            row_values = [str(v).lower().strip() for v in row.values]
            if any(k in row_values for k in keywords):
                header_row_index = i
                break
        
        if header_row_index == -1:
            raise ValueError("Не удалось найти строку с заголовками.")
            
        headers = df_raw.iloc[header_row_index]
        df = df_raw.iloc[header_row_index + 1:].copy()
        df.columns = [str(h).strip() for h in headers]
        
        df.columns = [c.lower() for c in df.columns]
        
        rename_map = {
            'код поставщика': 'supplier_code',
            'код позиции': 'product_code',
            'наименование': 'product_name',
            'поставщик': 'supplier',
            'кол-во': 'sale_quantity',
            'сумма, руб.': 'sale_sum',
            'отдел': 'pharmacy_name'
        }
        
        df.rename(columns=rename_map, inplace=True)
        
        df['sale_quantity'] = pd.to_numeric(df['sale_quantity'], errors='coerce').fillna(0)
        df['sale_sum'] = pd.to_numeric(df['sale_sum'], errors='coerce').fillna(0)
        
        df = df[(df['sale_quantity'] != 0) | (df['sale_sum'] != 0)]
        
        def get_dates(row):
            try:
                m = int(float(row.get('месяц', 0)))
                y = int(float(row.get('год', 0)))
                if m < 1 or m > 12 or y < 2000:
                    return None, None
                
                start = datetime(y, m, 1)
                _, last = calendar.monthrange(y, m)
                end = datetime(y, m, last)
                return start, end
            except:
                return None, None

        dates = df.apply(get_dates, axis=1, result_type='expand')
        df['start_date'] = dates[0]
        df['end_date'] = dates[1]
        
        df.dropna(subset=['start_date', 'end_date'], inplace=True)
        
        df['start_date'] = df['start_date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        df['end_date'] = df['end_date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        
        df['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df['name_report'] = name_report
        df['name_pharm_chain'] = name_pharm_chain
        df['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        final_columns = [
            'uuid_report', 'product_code', 'product_name', 'pharmacy_name', 
            'supplier', 'supplier_code', 'sale_quantity', 'sale_sum', 
            'name_report', 'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm'
        ]
        
        for col in final_columns:
            if col not in df.columns:
                df[col] = None
                
        df_report = df[final_columns]
        df_report = df_report.replace({pd.NA: None, pd.NaT: None, float('nan'): None, '': None, 'nan': None})
        
        loger.info(f"Парсинг отчета '{name_report}' успешно завершен. Строк: {len(df_report)}")
        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_xls(path, name_report, name_pharm_chain) -> dict:
    loger = LoggingMixin().log
    loger.info(f"Диспетчер 'Здравсити' получил задачу: '{name_report}' для '{name_pharm_chain}' из файла '{path}'")

    report_type_lower = name_report.lower()

    if 'продажи' in report_type_lower:
        return extract_sales(path, name_report, name_pharm_chain)
    else:
        loger.warning(f"Неизвестный тип отчета для 'Здравсити': '{name_report}'. Парсер не будет вызван.")
        return {}

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    main_loger.info("Запуск локального теста для парсера 'Здравсити'.")
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты_аптек\Здравсити\Продажи\01_2023 - 03_2025.xlsx'
    test_report_type = 'Продажи'

    if os.path.exists(test_file_path):
        main_loger.info(f"Тестовый файл найден: {test_file_path}")
        try:
            result = extract_xls(path=test_file_path, name_report=test_report_type, name_pharm_chain='Здравсити')
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