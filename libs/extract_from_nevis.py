import pandas as pd
import uuid
import re
import os
from datetime import datetime, timedelta
import calendar
from airflow.utils.log.logging_mixin import LoggingMixin

def extract_nevis(path='', name_report='Закупки', name_pharm_chain='Невис') -> dict:
    """
    Парсер для отчетов 'Невис'.
    """
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")

        xls = pd.ExcelFile(path)
        target_sheet = None
        df_raw = pd.DataFrame()

        if 'TDSheet' in xls.sheet_names:
            target_sheet = 'TDSheet'
            df_raw = pd.read_excel(xls, sheet_name=target_sheet, header=None, dtype=str)
        else:
            loger.warning("Лист 'TDSheet' не найден. Поиск первого непустого листа...")
            for sheet in xls.sheet_names:
                temp_df = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str)
                if not temp_df.dropna(how='all').empty:
                    target_sheet = sheet
                    df_raw = temp_df
                    loger.info(f"Выбран непустой лист: '{target_sheet}'")
                    break
            
            if target_sheet is None:
                target_sheet = xls.sheet_names[0]
                loger.warning(f"Непустой лист не найден. Используется первый лист: '{target_sheet}'")
                df_raw = pd.read_excel(xls, sheet_name=target_sheet, header=None, dtype=str)

        filename = os.path.basename(path)
        match = re.search(r'(\d{2})_(\d{4})', filename)
        
        if match:
            month_file = int(match.group(1))
            year_file = int(match.group(2))
            
            _, last_day = calendar.monthrange(year_file, month_file)
            end_date = datetime(year_file, month_file, last_day)
            
            start_date = (datetime(year_file, month_file, 1) - pd.DateOffset(months=2)).replace(day=1)
            
            loger.info(f"Период отчета определен из имени файла: {start_date.date()} - {end_date.date()}")
        else:
            loger.warning(f"Не удалось определить дату из имени файла '{filename}'. Используем текущий месяц.")
            now = datetime.now()
            start_date = now.replace(day=1)
            end_date = now

        df_report = pd.DataFrame()
        report_type_lower = name_report.lower()
        
        if 'закуп' in report_type_lower:
            header_idx = -1
            required_cols = ['Наименование', 'Адрес аптеки']
            
            for i, row in df_raw.head(20).iterrows():
                row_vals = [str(x).strip() for x in row.values]
                if all(col in row_vals for col in required_cols):
                    header_idx = i
                    break
            
            if header_idx != -1:
                headers = [str(x).strip() for x in df_raw.iloc[header_idx]]
                df_data = df_raw.iloc[header_idx + 1:].copy()
                df_data.columns = headers
                
                if 'Наименование' in df_data.columns:
                    df_data = df_data[~df_data['Наименование'].astype(str).str.contains(r'\bитого', case=False, na=False)]
                
                df_report['product_name'] = df_data.get('Наименование')
                df_report['address'] = df_data.get('Адрес аптеки')
                df_report['Pharmacy_name'] = df_data.get('Номер аптеки')
                df_report['legal_entity'] = df_data.get('Юр. лицо')
                df_report['branch_inn'] = df_data.get('ИНН')
                df_report['order_date'] = df_data.get('Дата заказа')
                df_report['doc_date'] = df_data.get('Дата входящего док.')
                df_report['doc_number'] = df_data.get('Номер входящего док.')
                df_report['supplier'] = df_data.get('Поставщик')
                df_report['document'] = df_data.get('Документ')
                df_report['quantity'] = pd.to_numeric(df_data.get('Количество'), errors='coerce').fillna(0)
                
                if 'Сумма с НДС' in df_data.columns:
                    df_report['product_total_sum'] = df_data['Сумма с НДС']
                elif 'Сумма' in df_data.columns:
                    df_report['product_total_sum'] = df_data['Сумма']
                
                df_report['sum_no_vat'] = df_data.get('Сумма без НДС')
                
                df_report.dropna(subset=['product_name'], inplace=True)

        elif 'остат' in report_type_lower:
            header_idx = -1
            required_cols = ['Наименование', 'Количество аптек']
            
            for i, row in df_raw.head(20).iterrows():
                row_vals = [str(x).strip() for x in row.values]
                if all(col in row_vals for col in required_cols):
                    header_idx = i
                    break
            
            if header_idx != -1:
                headers = [str(x).strip() for x in df_raw.iloc[header_idx]]
                df_data = df_raw.iloc[header_idx + 1:].copy()
                df_data.columns = headers
                
                if 'Наименование' in df_data.columns:
                    df_data = df_data[~df_data['Наименование'].astype(str).str.contains(r'\bитого', case=False, na=False)]
                
                df_report['product_name'] = df_data.get('Наименование')
                df_report['quantity'] = pd.to_numeric(df_data.get('Количество аптек'), errors='coerce').fillna(0)
                df_report.dropna(subset=['product_name'], inplace=True)

        elif 'продаж' in report_type_lower:            
            header_idx_long = -1
            required_cols_long = ['Номер аптеки', 'Адрес', 'Номенклатура', 'Количество']
            
            for i, row in df_raw.head(20).iterrows():
                row_vals = [str(x).strip() for x in row.values]
                if all(col in row_vals for col in required_cols_long):
                    header_idx_long = i
                    break
            
            if header_idx_long != -1:
                headers = [str(x).strip() for x in df_raw.iloc[header_idx_long]]
                df_data = df_raw.iloc[header_idx_long + 1:].copy()
                df_data.columns = headers
                
                for col in ['Номер аптеки', 'Номенклатура']:
                    if col in df_data.columns:
                        df_data = df_data[~df_data[col].astype(str).str.contains(r'\bитого', case=False, na=False)]
                
                df_report['Pharmacy_name'] = df_data.get('Номер аптеки')
                df_report['address'] = df_data.get('Адрес')
                df_report['product_name'] = df_data.get('Номенклатура')
                
                if 'продажи.Организация.ИНН' in df_data.columns:
                    df_report['branch_inn'] = df_data['продажи.Организация.ИНН']
                elif 'Магазин.Склад продажи.Организация.ИНН' in df_data.columns:
                    df_report['branch_inn'] = df_data['Магазин.Склад продажи.Организация.ИНН']
                
                df_report['quantity'] = pd.to_numeric(df_data.get('Количество'), errors='coerce').fillna(0)
                
                df_report.dropna(subset=['product_name'], inplace=True)
            
            else:
                header_idx = -1
                target_col = 'Аптека, Адрес'
                
                for i, row in df_raw.head(20).iterrows():
                    row_vals = [str(x).strip() for x in row.values]
                    if target_col in row_vals:
                        header_idx = i
                        break
                
                if header_idx != -1:
                    headers = [str(x).strip() for x in df_raw.iloc[header_idx]]
                    df_data = df_raw.iloc[header_idx + 1:].copy()
                    df_data.columns = headers
                    
                    df_data = df_data[~df_data[target_col].astype(str).str.contains(r'\bитого', case=False, na=False)]
                    
                    id_vars = [col for col in headers if col == target_col]
                    value_vars = [col for col in headers if col != target_col and col != 'nan' and col != '' and 'итого' not in col.lower()]
                    
                    df_melted = df_data.melt(id_vars=id_vars, value_vars=value_vars, 
                                             var_name='product_name', value_name='quantity')
                    
                    df_report['address'] = df_melted[target_col]
                    df_report['product_name'] = df_melted['product_name']
                    df_report['quantity'] = pd.to_numeric(df_melted['quantity'], errors='coerce').fillna(0)
                    

        final_columns = [
            'uuid_report', 'product_code', 'product_name',
            'branch_inn', 'legal_entity', 'Pharmacy_name', 'address',
            'legal_entity_inn','supplier', 'quantity', 'order_date',
            'doc_date', 'doc_number', 'document', 'sum_no_vat',
            'product_total_sum', 'name_report', 'name_pharm_chain', 
            'start_date', 'end_date', 'processed_dttm'
        ]
        
        # Добавляем недостающие колонки
        for col in final_columns:
            if col not in df_report.columns:
                df_report[col] = None

        if not df_report.empty:
            df_report['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_report))]
            df_report['name_report'] = name_report
            df_report['name_pharm_chain'] = name_pharm_chain
            df_report['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
            df_report['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
            df_report['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            if 'product_total_sum' in df_report.columns:
                 df_report['product_total_sum'] = df_report['product_total_sum'].astype(str).str.replace(r'[^\d.,]', '', regex=True).str.replace(',', '.')
            if 'sum_no_vat' in df_report.columns:
                 df_report['sum_no_vat'] = df_report['sum_no_vat'].astype(str).str.replace(r'[^\d.,]', '', regex=True).str.replace(',', '.')

        df_report = df_report[final_columns]
        
        df_report = df_report.astype(str)
        df_report = df_report.replace({'nan': None, 'None': None})

        loger.info(f"Парсинг Невис успешно завершен. Получено строк: {len(df_report)}")
        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    test_file_path = r'C:\Users\nmankov\Desktop\отчеты\Невис\Закуп\2024\09_2024.xlsx'
    test_report_type = 'Закупки'
    
    if os.path.exists(test_file_path):
        main_loger.info(f"Запуск локального теста для файла: {test_file_path}")
        try:
            result_data = extract_nevis(path=test_file_path, name_report=test_report_type)
            df_result = result_data.get('table_report')
            
            if df_result is not None and not df_result.empty:
                output_path = os.path.join(os.path.dirname(test_file_path), f"{os.path.splitext(os.path.basename(test_file_path))[0]}_result.csv")
                df_result.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
                main_loger.info(f"Результат успешно сохранен в: {output_path}")
                main_loger.info("Пример результата (первые 5 строк):")
                print(df_result.head().to_string())
            else:
                main_loger.warning("Результирующий датафрейм пуст.")
        except Exception as e:
            main_loger.error(f"Во время теста произошла ошибка: {e}", exc_info=True)
    else:
        main_loger.warning(f"Тестовый файл не найден: {test_file_path}")