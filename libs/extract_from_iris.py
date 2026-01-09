import pandas as pd
import uuid
import os
import calendar
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin

FINAL_COLUMNS = [
    'uuid_report',
    'contract', 'chain_name', 'brand', 'iris_code', 'iris_name',
    'product_name', 'portfolio_1', 'portfolio_2', 'portfolio_3',
    'historical_code', 'region', 'city', 'address',
    'legal_entity', 'inn', 'supplier_code', 'supplier',
    'agreed_distributor', 'so_sales_flag',
    'purchase_quantity', 'sale_quantity', 'remains_quantity',
    'cip_price', 'cip_sum',
    'name_report', 'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm'
]

def extract_purchases(path: str, name_report: str, name_pharm_chain: str) -> dict:
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")
        
        xls = pd.ExcelFile(path)
        sheet_names = xls.sheet_names
        
        dfs = []

        sales_sheet_exists = any('продажи' in sheet.lower() for sheet in sheet_names)
        if sales_sheet_exists:
            loger.info("В файле найден лист с продажами, вызываем парсер продаж.")
            sales_result = extract_sales(path, "Продажи", name_pharm_chain)
            if sales_result and 'table_report' in sales_result:
                dfs.append(sales_result['table_report'])

        target_sheet = None
        if len(sheet_names) > 1:
            for sheet in sheet_names:
                if 'закуп' in sheet.lower():
                    target_sheet = sheet
                    break
        else:
            if not sales_sheet_exists:
                target_sheet = sheet_names[0]
        
        if target_sheet:
            loger.info(f"Парсинг закупок с листа: {target_sheet}")
            df_raw = pd.read_excel(xls, sheet_name=target_sheet, header=None, dtype=str)
            
            header_row_index = -1
            for i, row in df_raw.head(20).iterrows():
                row_values = [str(v).lower().strip() for v in row.values]
                if 'годмесяц (ггггмм)' in row_values and 'приход в упак' in row_values:
                    header_row_index = i
                    break
            
            if header_row_index != -1:
                headers = df_raw.iloc[header_row_index]
                df = df_raw.iloc[header_row_index + 1:].copy()
                df.columns = [str(h).strip() for h in headers]
                
                rename_map = {
                    'Контракт': 'contract', 'Формат отчета': 'report_format', 'ГодМесяц (ГГГГММ)': 'period_yyyymm',
                    'Сеть': 'chain_name', 'Бренд': 'brand', 'код ИРИС': 'iris_code', 'Наименование ИРИС': 'iris_name',
                    'Наименование позиции из МП': 'product_name', 'Портфель объемник 1': 'portfolio_1',
                    'Портфель объемник 2': 'portfolio_2', 'Портфель объемник 3': 'portfolio_3',
                    'Код исторический': 'historical_code', 'Регион': 'region', 'Город': 'city', 'Адрес': 'address',
                    'Юр лицо': 'legal_entity', 'ИНН': 'inn', 'Код поставщика': 'supplier_code', 'Поставщик': 'supplier',
                    'Соглас.дистр': 'agreed_distributor', 'Приход в упак': 'purchase_quantity',
                    'СIP цена, руб': 'cip_price', 'Сумма в СИП': 'cip_sum'
                }
                
                df.rename(columns=rename_map, inplace=True)
                
                if 'period_yyyymm' in df.columns:
                    def get_dates(val):
                        try:
                            s = str(val).strip()
                            if len(s) == 6 and s.isdigit():
                                year = int(s[:4])
                                month = int(s[4:])
                                start = datetime(year, month, 1)
                                _, last_day = calendar.monthrange(year, month)
                                end = datetime(year, month, last_day)
                                return start, end
                        except: pass
                        return None, None

                    dates = df['period_yyyymm'].apply(get_dates)
                    df['start_date'] = dates.apply(lambda x: x[0].strftime('%Y-%m-%d %H:%M:%S') if x[0] else None)
                    df['end_date'] = dates.apply(lambda x: x[1].strftime('%Y-%m-%d %H:%M:%S') if x[1] else None)
                else:
                    df['start_date'] = None
                    df['end_date'] = None

                if 'report_format' in df.columns:
                    df.drop(columns=['report_format'], inplace=True)
                if 'period_yyyymm' in df.columns:
                    df.drop(columns=['period_yyyymm'], inplace=True)

                df['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df))]
                df['name_report'] = name_report
                df['name_pharm_chain'] = name_pharm_chain
                df['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                for col in FINAL_COLUMNS:
                    if col not in df.columns:
                        df[col] = None
                        
                df_report = df[FINAL_COLUMNS]
                df_report = df_report.replace({pd.NA: None, pd.NaT: None, float('nan'): None, 'nan': None})
                dfs.append(df_report)
            else:
                if not dfs:
                     raise ValueError("Не удалось найти строку с заголовками (ожидались 'ГодМесяц (ГГГГММ)' и 'Приход в упак').")

        if dfs:
            final_df = pd.concat(dfs, ignore_index=True)
            final_df = final_df.where(pd.notnull(final_df), None)
            return {'table_report': final_df}
        else:
            return {'table_report': pd.DataFrame()}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_sales(path: str, name_report: str, name_pharm_chain: str) -> dict:
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")
        
        xls = pd.ExcelFile(path)
        sheet_names = xls.sheet_names
        target_sheet = sheet_names[0]
        if len(sheet_names) > 1:
            for sheet in sheet_names:
                if 'продажи' in sheet.lower():
                    target_sheet = sheet
                    break
        
        df_raw = pd.read_excel(xls, sheet_name=target_sheet, header=None, dtype=str)
        
        header_row_index = -1
        for i, row in df_raw.head(20).iterrows():
            row_values = [str(v).lower().strip() for v in row.values]
            if 'годмесяц (ггггмм)' in row_values and 'количество в упак' in row_values:
                header_row_index = i
                break
        
        if header_row_index == -1:
             raise ValueError("Не удалось найти строку с заголовками (ожидались 'ГодМесяц (ГГГГММ)' и 'Количество в упак').")

        headers = df_raw.iloc[header_row_index]
        df = df_raw.iloc[header_row_index + 1:].copy()
        df.columns = [str(h).strip() for h in headers]
        
        rename_map = {
            'Контракт': 'contract', 'Формат отчета': 'report_format', 'ГодМесяц (ГГГГММ)': 'period_yyyymm',
            'Сеть': 'chain_name', 'Бренд': 'brand', 'код ИРИС': 'iris_code', 'Наименование ИРИС': 'iris_name',
            'Наименование позиции из МП': 'product_name', 'Портфель объемник 1': 'portfolio_1',
            'Портфель объемник 2': 'portfolio_2', 'Портфель объемник 3': 'portfolio_3',
            'Код исторический': 'historical_code', 'Регион': 'region', 'Город': 'city', 'Адрес': 'address',
            'Юр лицо': 'legal_entity', 'ИНН': 'inn', 'Признак продажи СО': 'so_sales_flag',
            'Количество в упак': 'sale_quantity', 'СIP цена, руб': 'cip_price', 'Сумма в СИП': 'cip_sum'
        }
        
        df.rename(columns=rename_map, inplace=True)
        
        if 'period_yyyymm' in df.columns:
            def get_dates(val):
                try:
                    s = str(val).strip()
                    if len(s) == 6 and s.isdigit():
                        year = int(s[:4])
                        month = int(s[4:])
                        start = datetime(year, month, 1)
                        _, last_day = calendar.monthrange(year, month)
                        end = datetime(year, month, last_day)
                        return start, end
                except: pass
                return None, None

            dates = df['period_yyyymm'].apply(get_dates)
            df['start_date'] = dates.apply(lambda x: x[0].strftime('%Y-%m-%d %H:%M:%S') if x[0] else None)
            df['end_date'] = dates.apply(lambda x: x[1].strftime('%Y-%m-%d %H:%M:%S') if x[1] else None)
        else:
            df['start_date'] = None
            df['end_date'] = None

        if 'report_format' in df.columns:
            df.drop(columns=['report_format'], inplace=True)
        if 'period_yyyymm' in df.columns:
            df.drop(columns=['period_yyyymm'], inplace=True)

        df['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df['name_report'] = name_report
        df['name_pharm_chain'] = name_pharm_chain
        df['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for col in FINAL_COLUMNS:
            if col not in df.columns:
                df[col] = None
                
        df_report = df[FINAL_COLUMNS]
        df_report = df_report.replace({pd.NA: None, pd.NaT: None, float('nan'): None, 'nan': None})
        return {'table_report': df_report}
    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_xls(path, name_report, name_pharm_chain) -> dict:
    loger = LoggingMixin().log
    loger.info(f"Диспетчер 'Ирис' получил задачу: '{name_report}' для '{name_pharm_chain}' из файла '{path}'")

    report_type_lower = name_report.lower()

    if 'закуп' in report_type_lower:
        return extract_purchases(path, name_report, name_pharm_chain)
    elif 'продажи' in report_type_lower:
        return extract_sales(path, name_report, name_pharm_chain)
    else:
        loger.warning(f"Неизвестный тип отчета для 'Ирис': '{name_report}'. Парсер не будет вызван.")
        return {}

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    main_loger.info("Запуск локального теста для парсера 'Ирис'.")
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты_аптек\ИРИС\Закуп\2023\12_2023.xlsx'
    test_report_type = 'Закуп'

    if os.path.exists(test_file_path):
        main_loger.info(f"Тестовый файл найден: {test_file_path}")
        try:
            result = extract_xls(path=test_file_path, name_report=test_report_type, name_pharm_chain='Ирис')
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