import pandas as pd
import uuid
import os
import calendar
import re
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

MONTHS_RU_LIST = [
    'январь', 'февраль', 'март', 'апрель', 'май', 'июнь',
    'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь'
]

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

def extract_xls(path: str, name_report: str, name_pharm_chain: str = 'Фармаимпекс') -> dict:
    """
    Парсер для отчетов 'Фармаимпекс' (Закупки, Продажи, Остатки).
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
            'наименование': 'product_name',
            'кол уп': 'purchase_quantity',
            'сумма': 'purchase_sum',
            'датадистриб': 'doc_date',
            'документдистриб': 'doc_number',
            'поставщик': 'supplier',
            'филиал зак': 'pharmacy_name'
        }

        # Продажи
        sales_map = {
            'аптека': 'pharmacy_name',
            'код аптеки': 'pharmacy_code',
            'город': 'city',
            'регион': 'region',
            'наименование': 'product_name',
            'продажи, шт': 'sale_quantity',
            'суммазак': 'sale_sum_purchase_price',
            'формат аптеки': 'pharmacy_format'
        }

        # Остатки
        stocks_map = {
            'аптека': 'pharmacy_name',
            'код аптеки': 'pharmacy_code',
            'город': 'city',
            'регион': 'region',
            'наименование': 'product_name',
            'остатки, шт': 'stock_quantity',
            'срокгодности': 'expiration_date'
        }

        sheets_to_process = {}
        has_explicit_zakup = any('закуп' in s.lower() for s in sheet_names)
        
        if has_explicit_zakup:
            for sheet in sheet_names:
                if 'продаж' in sheet.lower():
                    sheets_to_process[sheet] = 'Продажи'
            
            for sheet in sheet_names:
                if 'закуп' in sheet.lower():
                    sheets_to_process[sheet] = 'Закупки'

            has_ostatki_main = any('остатки аптек' in s.lower() for s in sheet_names)
            for sheet in sheet_names:
                if sheet in sheets_to_process: continue
                s_lower = sheet.lower()
                if 'остатки аптек' in s_lower:
                    sheets_to_process[sheet] = 'Остатки'
                elif ('тихоликвид' in s_lower or 'тхл' in s_lower) and not has_ostatki_main:
                    sheets_to_process[sheet] = 'Остатки'
        else:
            month_sheet_found = False
            for sheet in sheet_names:
                s_lower = sheet.lower()
                for m in MONTHS_RU_LIST:
                    if m in s_lower:
                        sheets_to_process[sheet] = 'MonthSheet'
                        month_sheet_found = True
                        break
                if month_sheet_found: break

        sorted_sheets = []
        month_sheets = [s for s, t in sheets_to_process.items() if t == 'MonthSheet']
        other_sheets = [s for s in sheet_names if s in sheets_to_process and s not in month_sheets]
        sorted_sheets = month_sheets + other_sheets
        
        found_stocks_in_month_sheet = False

        for sheet in sorted_sheets:
            
            report_type = sheets_to_process[sheet]
            
            if report_type == 'Остатки' and found_stocks_in_month_sheet:
                if 'тихоликвид' in sheet.lower() or 'тхл' in sheet.lower():
                    loger.info(f"Пропуск листа '{sheet}' (ТХЛ), так как остатки найдены в комбинированном отчете.")
                    continue

            if report_type == 'MonthSheet':
                loger.info(f"Проверка содержимого листа '{sheet}' (MonthSheet)")
                df_raw = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str)
                
                section_starts = []
                
                for i, row in df_raw.iterrows():
                    if len(row) > 0:
                        val0 = str(row[0]).lower().strip() if pd.notna(row[0]) else ""
                        
                        if 'закуп' in val0 and len(val0) < 50:
                            section_starts.append((i, 'Закупки'))
                        elif 'продажи' in val0 and len(val0) < 50:
                            section_starts.append((i, 'Продажи'))
                        elif 'остатки аптек' in val0 and len(val0) < 100:
                            section_starts.append((i, 'Остатки'))
                        elif 'отгрузка' in val0 and len(val0) < 50:
                            section_starts.append((i, 'Ignore'))
                
                section_starts.sort(key=lambda x: x[0])
                
                if section_starts:
                    loger.info(f"Лист '{sheet}' определен как вертикальный комбинированный отчет. Секции: {section_starts}")
                    
                    for k, (start_idx, r_type) in enumerate(section_starts):
                        if r_type == 'Ignore':
                            continue

                        if r_type == 'Остатки':
                            found_stocks_in_month_sheet = True

                        # Определяем конец текущей секции (начало следующей или конец файла)
                        limit_idx = section_starts[k+1][0] if k + 1 < len(section_starts) else len(df_raw)
                        
                        # Заголовки - это строка start_idx + 1 (следующая после названия секции)
                        header_row_idx = start_idx + 1
                        if header_row_idx >= len(df_raw):
                            continue
                            
                        data_start_idx = header_row_idx + 1
                        end_idx = limit_idx
                        
                        # Ищем первую пустую строку (разделитель) в первом столбце
                        for i in range(data_start_idx, limit_idx):
                            val = df_raw.iloc[i, 0]
                            if pd.isna(val) or str(val).strip() == '' or str(val).lower() == 'nan':
                                end_idx = i
                                break
                            # Дополнительная проверка на начало следующей секции или "Отгрузка", если вдруг пропустили
                            val_str = str(val).lower().strip()
                            if 'отгрузка' in val_str and len(val_str) < 50:
                                end_idx = i
                                break

                        headers = df_raw.iloc[header_row_idx]
                        df_slice = df_raw.iloc[data_start_idx : end_idx].copy()
                        df_slice.columns = headers
                        df_slice.columns = [str(col).lower().strip().replace('\n', ' ').replace('  ', ' ') for col in df_slice.columns]
                        
                        # Дедупликация имен колонок
                        new_cols = []
                        seen = set()
                        for c in df_slice.columns:
                            c_orig = c
                            i = 1
                            while c in seen: c = f"{c_orig}_{i}"; i += 1
                            seen.add(c)
                            new_cols.append(c)
                        df_slice.columns = new_cols
                        
                        # Выбор маппинга
                        r_map = None
                        if r_type == 'Закупки': r_map = purchases_map
                        elif r_type == 'Продажи': r_map = sales_map
                        elif r_type == 'Остатки': r_map = stocks_map

                        if r_map:
                            df_slice.rename(columns=r_map, inplace=True)
                            df_slice['name_report'] = r_type
                            
                            # Удаляем пустые строки и строки с итогами
                            df_slice.dropna(how='all', inplace=True)
                            if not df_slice.empty:
                                df_slice = df_slice[~df_slice.iloc[:, 0].astype(str).str.contains('итог', case=False, na=False)]
                            
                            all_data.append(df_slice)
                            loger.info(f"Обработана секция '{r_type}', строк: {len(df_slice)}")
                        
                    continue # Лист обработан
                else:
                    # Если не комбинированный, считаем его обычным отчетом закупок
                    report_type = 'Закупки'

            current_map = None
            
            if report_type == 'Продажи':
                current_map = sales_map
            elif report_type == 'Остатки':
                current_map = stocks_map
            elif report_type == 'Закупки':
                current_map = purchases_map
            
            if current_map:
                loger.info(f"Обработка листа: '{sheet}' как '{report_type}'")
                
                df_raw = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str)
                
                header_row_index = -1
                # Ключевые слова для поиска заголовка
                if report_type == 'Закупки':
                    keywords = ['наименование', 'кол уп', 'поставщик']
                elif report_type == 'Продажи':
                    keywords = ['аптека', 'продажи, шт', 'суммазак']
                elif report_type == 'Остатки':
                    keywords = ['аптека', 'остатки, шт', 'срокгодности']
                
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

                df_sheet.rename(columns=current_map, inplace=True)
                df_sheet['name_report'] = report_type
                all_data.append(df_sheet)

        if not all_data:
            loger.warning("Не найдено подходящих листов.")
            return {'table_report': pd.DataFrame()}

        df_final = pd.concat(all_data, ignore_index=True)
        
        df_final['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_final))]
        df_final['name_pharm_chain'] = name_pharm_chain
        df_final['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df_final['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df_final['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        final_columns = [
            'uuid_report', 
            'pharmacy_name', 'pharmacy_code', 'city', 'region', 'pharmacy_format',
            'product_name', 'supplier', 'doc_date', 'doc_number',
            'purchase_quantity', 'purchase_sum',
            'sale_quantity', 'sale_sum_purchase_price',
            'stock_quantity', 'expiration_date',
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
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты\Фармаимпекс\Закуп_Продажи_Остатки\2023\07_2023.xlsx'    
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