import pandas as pd
import uuid
import re
import os
from datetime import datetime, timedelta
import calendar
from airflow.utils.log.logging_mixin import LoggingMixin

def extract_mz(path='', name_report='Закупки', name_pharm_chain='Мелодия Здоровья') -> dict:
    """
    Универсальный парсер для отчетов 'Мелодия Здоровья' (Закупки, Остатки, Продажи).
    """
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")

        df_raw = pd.read_excel(path, header=None, dtype=str)

        product_sums_row = None
        sum_keywords = ["Сумма в закупочных ценах", "Сумма, руб.(зак.)"]
        
        try:
            for i in range(len(df_raw) - 1, -1, -1):
                row_vals = [str(x) for x in df_raw.iloc[i, :10].values]
                if any(keyword in v for v in row_vals for keyword in sum_keywords):
                    product_sums_row = df_raw.iloc[i]
                    loger.info(f"Найдена строка сумм по товарам на индексе {i}")
                    break
        except Exception as e:
            loger.warning(f"Ошибка при поиске сумм: {e}")

        start_date = None
        end_date = None
        report_type_lower = name_report.lower()
        is_purchases = 'закуп' in report_type_lower
        is_remains = 'остатки' in report_type_lower
        is_sales = 'продажи' in report_type_lower
        
        if is_sales:
            dates_found = []
            date_found_flag = False
            for i, row in df_raw.head(7).iterrows():
                for cell_value in row:
                    if isinstance(cell_value, str):
                        match = re.fullmatch(r'(\d{2}\.\d{2}\.\d{4})', cell_value.strip())
                        if match:
                            try:
                                dates_found.append(datetime.strptime(match.group(1), "%d.%m.%Y"))
                            except ValueError:
                                continue
                if len(dates_found) >= 2:
                    start_date = dates_found[0]
                    end_date = dates_found[1]
                    loger.info(f"Определен период для отчета 'Продажи' из ячеек: {start_date.date()} - {end_date.date()}")
                    date_found_flag = True
                    break
            if not date_found_flag:
                loger.info("Для отчета 'Продажи' не найдены 2 даты в первых 7 строках. Поиск по 'За период'.")
        elif is_remains:
            date_found = False
            for i, row in df_raw.head(6).iterrows():
                for cell_value in row:
                    if isinstance(cell_value, str):
                        match = re.fullmatch(r'(\d{2}\.\d{2}\.\d{4})', cell_value.strip())
                        if match:
                            try:
                                report_date = datetime.strptime(match.group(1), "%d.%m.%Y")
                                start_date = report_date.replace(day=1)
                                _, last_day = calendar.monthrange(report_date.year, report_date.month)
                                end_date = report_date.replace(day=last_day)
                                loger.info(f"Определен период для отчета 'Остатки' из ячейки: {start_date.date()} - {end_date.date()}")
                                date_found = True
                                break
                            except ValueError:
                                continue
                if date_found:
                    break
            if not date_found:
                loger.info("Для отчета 'Остатки' не найдена дата в первых 6 строках. Поиск по 'За период'.")

        if start_date is None:
            for i, row in df_raw.iterrows():
                row_str = row.astype(str).str.cat(sep=' ')
                if 'За период' in row_str:
                    dates = []
                    for val in row:
                        if isinstance(val, str) and len(val) == 10 and val[2] == '.' and val[5] == '.':
                            dates.append(val)
                    
                    if len(dates) >= 2:
                        try:
                            start_date = datetime.strptime(dates[0].strip(), "%d.%m.%Y")
                            end_date = datetime.strptime(dates[1].strip(), "%d.%m.%Y")
                            loger.info(f"Определен период отчета: {start_date.date()} - {end_date.date()}")
                            break
                        except ValueError:
                            pass
        
        if start_date is None:
            loger.warning("Не удалось определить период отчета. Проставляем текущий месяц.")
            today = datetime.now()
            start_date = today.replace(day=1)
            end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)

        header_row_index = -1

        if is_purchases:
            for i, row in df_raw.iterrows():
                if 'ИНН филиала' in row.astype(str).values:
                    header_row_index = i
                    break
            if header_row_index == -1: raise ValueError("Не найден заголовок 'ИНН филиала' для отчета о закупках.")
        else:
            for i, row in df_raw.iterrows():
                row_vals = [str(v) for v in row.values]
                if 'Адрес аптеки' in row_vals and ('Филиал/Товар' in row_vals or 'ИНН филиала' in row_vals):
                    header_row_index = i
                    break
            if header_row_index == -1: raise ValueError("Не найдены заголовки 'Филиал/Товар' и 'Адрес аптеки' для отчета об остатках/продажах.")

        codes_row_index = header_row_index - 1
        headers = df_raw.iloc[header_row_index].tolist()
        codes_row = df_raw.iloc[codes_row_index].tolist()

        if is_purchases:
            fixed_cols_count = 0
            for idx, col_name in enumerate(headers):
                if pd.isna(col_name): continue
                if str(col_name).strip() in ['ИНН филиала', 'ЮЛ аптеки', 'Наименование аптеки/Товар', 'Адрес аптеки', 'ИНН поставщика', 'Поставщик']:
                    fixed_cols_count = idx + 1
                elif pd.notna(codes_row[idx]) and str(codes_row[idx]).strip() not in ['Код товара', '']: break
        else:
            fixed_cols_count = 0
            for idx, col_name in enumerate(headers):
                if pd.isna(col_name): continue
                if str(col_name).strip() in ['ИНН филиала', 'ЮЛ аптеки', 'Филиал/Товар', 'Адрес аптеки']:
                    fixed_cols_count = idx + 1
                elif pd.notna(codes_row[idx]) and str(codes_row[idx]).strip() not in ['Код товара', '']: break
        loger.info(f"Товары начинаются с колонки индекс: {fixed_cols_count}")

        product_codes_map = {}
        product_sums_map = {}
        product_cols_indices = []
        
        for idx in range(fixed_cols_count, len(headers)):
            col_header = headers[idx]
            code_val = codes_row[idx]
            
            if pd.isna(col_header) or "Сумма" in str(col_header):
                continue
                
            if pd.notna(code_val):
                col_name = str(col_header)
                product_codes_map[col_name] = str(code_val).split('.')[0]
                product_cols_indices.append(idx)
                
                if product_sums_row is not None:
                    sum_val = product_sums_row.iloc[idx] 
                    if pd.notna(sum_val):
                        clean_sum = str(sum_val).replace(' ', '').replace(',', '.').replace('\xa0', '')
                        try:
                            float(clean_sum) 
                            product_sums_map[col_name] = clean_sum
                        except ValueError:
                            pass

        df_data = df_raw.iloc[header_row_index + 1:].copy()
        df_data.columns = headers
        
        fixed_col_names = [h for i, h in enumerate(headers) if i < fixed_cols_count and pd.notna(h)]
        product_col_names = [headers[i] for i in product_cols_indices if i < len(headers)]
        
        df_data = df_data[fixed_col_names + product_col_names]
        
        df_data.dropna(subset=['Адрес аптеки'], inplace=True)
        
        if not df_data.empty:
             df_data = df_data[~df_data.iloc[:, 0].astype(str).str.contains("Итого|Сумма", case=False, na=False)]

        loger.info(f"Строк с аптеками до преобразования: {len(df_data)}")

        df_melted = df_data.melt(
            id_vars=fixed_col_names,
            value_vars=product_col_names,
            var_name='product_name',
            value_name='quantity'
        )

        df_melted.dropna(subset=['quantity'], inplace=True)
        df_melted = df_melted[df_melted['quantity'].astype(str).str.strip() != '']
        
        loger.info(f"Успешно получено {len(df_melted)} строк после преобразования!")

        df_report = pd.DataFrame()
        
        df_report['product_name'] = df_melted['product_name']
        df_report['product_code'] = df_melted['product_name'].map(product_codes_map)
        df_report['product_total_sum'] = df_melted['product_name'].map(product_sums_map)
        
        df_report['quantity'] = pd.to_numeric(df_melted['quantity'], errors='coerce')
        df_report['quantity'] = df_report['quantity'].fillna(0)
        
        if is_purchases:
            df_report['branch_inn'] = df_melted.get('ИНН филиала')
            df_report['legal_entity'] = df_melted.get('ЮЛ аптеки')
            df_report['Pharmacy_name'] = df_melted.get('Наименование аптеки/Товар')
            df_report['legal_entity_inn'] = df_melted.get('ИНН поставщика')
            df_report['supplier'] = df_melted.get('Поставщик')
        else:
            df_report['branch_inn'] = df_melted.get('ИНН филиала')
            df_report['legal_entity'] = df_melted.get('ЮЛ аптеки')
            df_report['Pharmacy_name'] = df_melted.get('Филиал/Товар')
        
        df_report['address'] = df_melted.get('Адрес аптеки')

        # Технические поля
        df_report['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_report))]
        df_report['name_report'] = name_report
        df_report['name_pharm_chain'] = name_pharm_chain
        df_report['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df_report['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df_report['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Итоговые колонки
        final_columns = [
            'uuid_report', 'product_code', 'product_name',
            'branch_inn', 'legal_entity', 'Pharmacy_name', 'address',
            'legal_entity_inn','supplier', 'quantity', 
            'product_total_sum','name_report', 'name_pharm_chain', 
            'start_date', 'end_date', 'processed_dttm'
        ]
        
        for col in final_columns:
            if col not in df_report.columns:
                df_report[col] = None
        
        df_report = df_report[final_columns]
        df_report = df_report.replace({pd.NA: None, pd.NaT: None, float('nan'): None})

        loger.info("Парсинг Мелодия Здоровья успешно завершен.")
        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    test_file_path = r'C:\Users\nmankov\Desktop\отчеты\МЗ\Продажи\2025\01_2025.xlsx'
    test_report_type = 'Продажи'
    
    if os.path.exists(test_file_path):
        main_loger.info(f"Запуск локального теста для файла: {test_file_path}")
        try:
            result_data = extract_mz(path=test_file_path, name_report=test_report_type, name_pharm_chain='Мелодия Здоровья')
            df_result = result_data.get('table_report')
            
            if df_result is not None and not df_result.empty:
                output_path = os.path.join(os.path.dirname(test_file_path), f"{os.path.splitext(os.path.basename(test_file_path))[0]}_result.csv")
                df_result.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
                main_loger.info(f"Результат успешно сохранен в: {output_path}")
                main_loger.info("Пример результата:")
                print(df_result.head().to_string())
            else:
                main_loger.warning("Результирующий датафрейм пуст.")
        except Exception as e:
            main_loger.error(f"Во время теста произошла ошибка: {e}", exc_info=True)
    else:
        main_loger.warning(f"Тестовый файл не найден: {test_file_path}")