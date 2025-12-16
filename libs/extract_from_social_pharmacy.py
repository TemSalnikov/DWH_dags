import pandas as pd
import uuid
import os
import calendar
import re
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

def _get_dates_from_filename(path: str, loger) -> tuple[datetime, datetime]:
    """
    Извлекает начальную и конечную дату из имени файла формата MM_YYYY.
    """
    try:
        filename = os.path.basename(path)
        match = re.search(r'(\d{2})_(\d{4})', filename)
        if match:
            month = int(match.group(1))
            year = int(match.group(2))
            start_date = datetime(year, month, 1)
            _, last_day = calendar.monthrange(year, month)
            end_date = datetime(year, month, last_day)
            loger.info(f"Определен период отчета из имени файла: {start_date.date()} - {end_date.date()}")
            return start_date, end_date
        else:
             raise ValueError("Не найден паттерн MM_YYYY в имени файла")
    except Exception as e:
        loger.error(f"Ошибка определения даты: {e}")
        raise

def extract_xls(path: str, name_report: str, name_pharm_chain: str = 'Социальная аптека') -> dict:
    """
    Парсер для отчетов 'Социальная аптека' (Закупки, Продажи, Остатки).
    """
    loger = LoggingMixin().log
    loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")

    try:
        start_date, end_date = _get_dates_from_filename(path, loger)
        
        xls = pd.ExcelFile(path)
        sheet_names = xls.sheet_names
        all_data = []

        # Маппинг колонок
        # Закупки
        purchases_map = {
            'субдоговор наименование': 'subcontract_name',
            'субдоговор наименование списка': 'subcontract_name',
            'наименование товара': 'product_name',
            'номенклатура маркетинговое наименование': 'marketing_name',
            'контрагент краткое наименование': 'contractor_short_name',
            'аптека краткое наименование': 'pharmacy_short_name',
            'аптека сводный адрес': 'pharmacy_address',
            'аптека инн': 'pharmacy_inn',
            'закупки упаковки': 'purchase_quantity',
            'закупки в сип-ценах': 'purchase_sip',
            'закупки в ценах поставщика без ндс': 'purchase_sum_no_vat',
            'закупки в ценах поставщика с ндс': 'purchase_sum_vat'
        }
        
        # Продажи
        sales_map = {
            'субдоговор наименование списка': 'subcontract_name',
            'наименование товара': 'product_name',
            'номенклатура маркетинговое наименование': 'marketing_name',
            'аптека краткое наименование': 'pharmacy_short_name',
            'аптека головная организация': 'head_organization',
            'аптека сводный адрес': 'pharmacy_address',
            'аптека инн': 'pharmacy_inn',
            'условие бонус % план №1': 'bonus_condition',
            'продажи упаковки': 'sale_quantity',
            'продажи в сип-ценах': 'sale_sip',
            'продажи в ценах поставщика без ндс': 'sale_sum_no_vat',
            'продажи в ценах поставщика с ндс': 'sale_sum_vat'
        }
        
        # Остатки
        stocks_map = {
            'субдоговор наименование списка': 'subcontract_name',
            'наименование товара': 'product_name',
            'номенклатура маркетинговое наименование': 'marketing_name',
            'аптека краткое наименование': 'pharmacy_short_name',
            'аптека сводный адрес': 'pharmacy_address',
            'аптека инн': 'pharmacy_inn',
            'партия серия': 'batch_series',
            'партия дата поступления накладной': 'batch_invoice_date',
            'партия срок годности': 'batch_expiration_date',
            'поставщик': 'supplier',
            'цена сип': 'price_sip',
            'состояние склада упаковки': 'stock_quantity',
            'состояние склада в сип-ценах': 'stock_sip',
            'состояние склада в ценах поставщика': 'stock_sum_supplier',
            'неликвиды упаковки': 'illiquid_quantity',
            'неликвиды в ценах поставщика': 'illiquid_sum_supplier'
        }

        for sheet in sheet_names:
            sheet_lower = sheet.lower().strip()
            current_map = None
            report_type = None
            
            if 'закуп' in sheet_lower or sheet_lower == 'лист1':
                current_map = purchases_map
                report_type = 'Закупки'
            elif 'продаж' in sheet_lower or sheet_lower == 'лист2':
                current_map = sales_map
                report_type = 'Продажи'
            elif 'остатк' in sheet_lower or sheet_lower == 'лист3':
                current_map = stocks_map
                report_type = 'Остатки'
            
            if current_map:
                loger.info(f"Обработка листа: '{sheet}' как '{report_type}'")
                
                df_raw = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str)
                
                header_row_index = -1
                keywords = ['наименование товара', 'аптека инн', 'аптека краткое наименование']
                
                for i, row in df_raw.head(20).iterrows():
                    row_values = [str(val).lower().strip() for val in row.values if pd.notna(val)]
                    if any(k in row_values for k in keywords):
                        header_row_index = i
                        break
                
                if header_row_index == -1:
                    loger.warning(f"Не удалось найти строку заголовков на листе '{sheet}'.")
                    continue
                
                headers = df_raw.iloc[header_row_index]
                df_sheet = df_raw.iloc[header_row_index + 1:].copy()
                df_sheet.columns = headers
                df_sheet.columns = [str(col).lower().strip().replace('\n', ' ').replace('  ', ' ') for col in df_sheet.columns]
                
                # Дедупликация имен колонок
                new_cols = []
                seen = set()
                for c in df_sheet.columns:
                    c_orig = c
                    i = 1
                    while c in seen:
                        c = f"{c_orig}_{i}"
                        i += 1
                    seen.add(c)
                    new_cols.append(c)
                df_sheet.columns = new_cols

                if not df_sheet.empty:
                    # Удаляем строки, содержащие "Общий итог" в первой колонке
                    df_sheet = df_sheet[~df_sheet.iloc[:, 0].astype(str).str.contains('общий итог', case=False, na=False)]
                
                df_sheet.rename(columns=current_map, inplace=True)
                df_sheet['name_report'] = report_type
                all_data.append(df_sheet)

        if not all_data:
            loger.warning("Не найдено подходящих листов (Закупки, Продажи, Остатки).")
            return {'table_report': pd.DataFrame()}

        df_final = pd.concat(all_data, ignore_index=True)
        
        df_final['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_final))]
        df_final['name_pharm_chain'] = name_pharm_chain
        df_final['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df_final['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df_final['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        final_columns = [
            'uuid_report', 
            'subcontract_name', 'product_name', 'marketing_name', 
            'contractor_short_name', 'pharmacy_short_name', 'pharmacy_address', 'pharmacy_inn',
            'head_organization', 'bonus_condition',
            'purchase_quantity', 'purchase_sip', 'purchase_sum_no_vat', 'purchase_sum_vat',
            'sale_quantity', 'sale_sip', 'sale_sum_no_vat', 'sale_sum_vat',
            'batch_series', 'batch_invoice_date', 'batch_expiration_date', 'supplier',
            'price_sip', 'stock_quantity', 'stock_sip', 'stock_sum_supplier',
            'illiquid_quantity', 'illiquid_sum_supplier',
            'name_report', 'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm'
        ]
        
        for col in final_columns:
            if col not in df_final.columns:
                df_final[col] = None
        
        df_report = df_final[final_columns]
        df_report = df_report.where(pd.notna(df_report), None)
        
        loger.info(f"Парсинг завершен. Итого строк: {len(df_report)}")
        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты\Социальная аптка\Остатки_Закуп_Продажи\2024\12_2024.xlsx'    
    if os.path.exists(test_file_path):
        main_loger.info(f"Запуск локального теста для файла: {test_file_path}")
        try:
            result = extract_xls(path=test_file_path, name_report='Тестовый отчет')
            df = result.get('table_report')
            if df is not None and not df.empty:
                output_filename = f"{os.path.splitext(os.path.basename(test_file_path))[0]}_result.csv"
                output_path = os.path.join(os.path.dirname(test_file_path), output_filename)
                df.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
                main_loger.info(f"Результат успешно сохранен в: {output_path}")
        except Exception as e:
            main_loger.error(f"Во время локального теста произошла ошибка: {e}", exc_info=True)
    else:
        main_loger.warning(f"Тестовый файл не найден по пути: {test_file_path}. Пожалуйста, создайте его для проверки.")