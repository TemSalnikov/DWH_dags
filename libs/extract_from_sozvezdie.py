import pandas as pd
import uuid
import os
import calendar
from datetime import datetime
from datetime import timedelta
from airflow.utils.log.logging_mixin import LoggingMixin

MONTHS_RU = {
    'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4, 'май': 5, 'июнь': 6,
    'июль': 7, 'август': 8, 'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
}

def _get_dates_from_column(df: pd.DataFrame, loger) -> tuple[datetime, datetime]:
    """
    Извлекает даты из столбца 'месяц' (формат '2023 Июнь').
    """
    if 'month' not in df.columns:
        loger.warning("Столбец 'месяц' (month) не найден в данных.")
        return datetime.now(), datetime.now()

    dates = []
    unique_vals = df['month'].dropna().unique()
    
    for val in unique_vals:
        try:
            parts = str(val).strip().split()
            if len(parts) >= 2:
                year = int(parts[0])
                month_name = parts[1].lower()
                month = MONTHS_RU.get(month_name)
                if month:
                    dates.append((year, month))
        except Exception:
            continue
            
    if not dates:
        loger.warning("Не удалось распарсить даты из столбца 'месяц'.")
        return datetime.now(), datetime.now()
        
    dates.sort()
    min_y, min_m = dates[0]
    max_y, max_m = dates[-1]
    
    start_date = datetime(min_y, min_m, 1)
    _, last_day = calendar.monthrange(max_y, max_m)
    end_date = datetime(max_y, max_m, last_day)
    
    if start_date == end_date:
        end_date = start_date + timedelta(days=1)
        
    return start_date, end_date

def extract_xls(path: str, name_report: str, name_pharm_chain: str = 'Созвездие') -> dict:
    """
    Парсер для отчетов 'Созвездие' (Закупки, Продажи, Остатки).
    """
    loger = LoggingMixin().log
    loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")

    try:
        xls = pd.ExcelFile(path)
        sheet_names = xls.sheet_names
        all_data = []

        common_map = {
            'инн': 'pharmacy_inn',
            'юл': 'legal_entity',
            'аптека(адрес)': 'pharmacy_name',
            'код промотовара': 'product_code',
            'промотовар': 'product_name',
            'месяц': 'month'
        }

        purchases_map = common_map.copy()
        purchases_map.update({
            'поставщик': 'supplier',
            'количество закуплено, уп.': 'purchase_quantity',
            'сумма закупки(с ндс), руб.': 'purchase_sum_vat',
            'сумма закупки в сип ценах': 'purchase_sum_sip'
        })

        sales_map = common_map.copy()
        sales_map.update({
            'количество реализовано, уп.': 'sale_quantity',
            'кол-во реализовано, уп.': 'sale_quantity'
        })

        stocks_map = common_map.copy()
        stocks_map.update({
            'кол-во уп.': 'stock_quantity',
            'кол-во реализовано, уп.': 'stock_quantity'
        })

        for sheet in sheet_names:
            sheet_lower = sheet.lower()
            current_map = None
            report_type = None
            
            if 'закуп' in sheet_lower:
                current_map = purchases_map
                report_type = 'Закупки'
            elif 'продаж' in sheet_lower:
                current_map = sales_map
                report_type = 'Продажи'
            elif 'остат' in sheet_lower:
                current_map = stocks_map
                report_type = 'Остатки'
            
            if current_map:
                loger.info(f"Обработка листа: '{sheet}'")
                df_raw = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str)
                
                header_row_index = -1
                search_keywords = ['инн', 'промотовар', 'аптека(адрес)']
                
                for i, row in df_raw.head(20).iterrows():
                    row_values = [str(val).lower().strip() for val in row.values if pd.notna(val)]
                    if sum(1 for k in search_keywords if k in row_values) >= 2:
                        header_row_index = i
                        break
                
                if header_row_index == -1:
                    loger.warning(f"Не удалось найти строку с заголовками на листе '{sheet}'.")
                    continue

                headers = df_raw.iloc[header_row_index]
                df_sheet = df_raw.iloc[header_row_index + 1:].copy()
                df_sheet.columns = headers
                df_sheet.columns = [str(col).lower().strip() for col in df_sheet.columns]
                df_sheet.rename(columns=current_map, inplace=True)
                df_sheet['name_report'] = report_type
                all_data.append(df_sheet)

        if not all_data:
            loger.warning("Не найдено листов с ключевыми словами 'закуп', 'продаж' или 'остат'.")
            return {'table_report': pd.DataFrame()}

        df_final = pd.concat(all_data, ignore_index=True)
        
        start_date, end_date = _get_dates_from_column(df_final, loger)
        
        df_final['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_final))]
        df_final['name_pharm_chain'] = name_pharm_chain
        df_final['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df_final['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df_final['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        final_columns = [
            'uuid_report', 'legal_entity', 'pharmacy_inn', 'pharmacy_name',
            'product_code', 'product_name', 'supplier', 
            'purchase_quantity', 'purchase_sum_vat', 'purchase_sum_sip',
            'sale_quantity', 'stock_quantity', 'month',
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
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты\Созвездие\Закуп Продажи Остатки\2024\03_2024.xlsx'    
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