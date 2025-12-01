import pandas as pd
import uuid
import os
from datetime import datetime
import hashlib
from airflow.utils.log.logging_mixin import LoggingMixin

def get_dates_from_filename(path):
    """
    Извлекает start_date и end_date из имени файла формата 'ММ_ГГГГ.xlsx'.
    """
    try:
        filename_no_ext = os.path.basename(path).split('.')[0]
        if ' ' in filename_no_ext:
            filename_no_ext = filename_no_ext.split(' ')[0]
            
        start_date = datetime.strptime(f"01.{filename_no_ext.replace('_', '.')}", "%d.%m.%Y")
        end_date = start_date + pd.offsets.MonthEnd(1)
        return start_date, end_date
    except Exception as e:
        raise ValueError(f"Имя файла должно быть в формате ММ_ГГГГ.xlsx. Ошибка: {e}")

def extract_all_xls(path='', name_report='Продажи', name_pharm_chain='Ваш доктор') -> dict:
    loger = LoggingMixin().log
    
    try:
        start_date, end_date = get_dates_from_filename(path)
        loger.info(f'Период отчета: {start_date.date()} - {end_date.date()}')
        
        df_raw = pd.read_excel(path, header=None)
        
        header_row_idx = None
        
        for idx, row in df_raw.iterrows():
            if row.astype(str).str.contains("Названия строк", na=False).any():
                header_row_idx = idx
                break
        
        if header_row_idx is None:
             raise ValueError("Не найдена строка заголовков (ожидалось 'Названия строк').")

        headers = df_raw.iloc[header_row_idx].values
        df = df_raw.iloc[header_row_idx + 1:].copy()
        df.columns = headers
        
        col_names = list(df.columns)
        prod_col_idx = -1
        for i, c in enumerate(col_names):
            if "Названия строк" in str(c):
                prod_col_idx = i
                break
        
        if prod_col_idx == -1:
             prod_col_idx = 0
             
        new_cols = col_names.copy()
        new_cols[prod_col_idx] = 'product'
        df.columns = new_cols
        
        df.dropna(subset=['product'], inplace=True)
        # Удаляем строки-итогов если они попали в данные (иногда "Общий итог" дублируется снизу)
        df = df[df['product'] != 'Общий итог']

        # --- ОПРЕДЕЛЕНИЕ ФОРМАТА файлов ---
        
        # Ищем колонку "Общий итог"
        total_col_name = None
        for c in df.columns:
            if "Общий итог" in str(c):
                total_col_name = c
                break
        
        # Если колонок <= 3 (Product, Total и мб индекс) -> 2024
        if len(df.columns) <= 3 and total_col_name:
            loger.info("--- ФОРМАТ 2024 (Упрощенный: Товар - Общий итог) ---")
            
            # Оставляем только Продукт и Итог
            df = df[['product', total_col_name]].copy()
            df.rename(columns={total_col_name: 'sale_quantity'}, inplace=True)
            
            # Заполняем пустотой поля филиалов
            df['branch_name'] = 'Null'
            df['city'] = 'Null'
            df['street'] = 'Null'
            
            df_report = df

        else:
            loger.info("--- ФОРМАТ 2025 (Детальный: По филиалам) ---")
            
            if total_col_name:
                df = df.drop(columns=[total_col_name])
            
            value_vars = [col for col in df.columns if col != 'product']
            
            df_melted = df.melt(
                id_vars=['product'], 
                value_vars=value_vars,
                var_name='branch_full_info', 
                value_name='sale_quantity'
            )
            
            # Парсинг адресов
            def parse_branch_info(full_name):
                try:
                    s_name = str(full_name)
                    if ',' in s_name:
                        parts = s_name.split(',')
                        branch_part = parts[0].strip()
                        street_part = parts[1].strip() if len(parts) > 1 else None
                        city_part = branch_part.split(' ')[0]
                        return branch_part, city_part, street_part
                    else:
                        return s_name, None, None
                except:
                    return str(full_name), None, None

            branch_data = df_melted['branch_full_info'].apply(parse_branch_info)
            df_melted['branch_name'] = [x[0] for x in branch_data]
            df_melted['city'] = [x[1] for x in branch_data]
            df_melted['street'] = [x[2] for x in branch_data]
            
            df_report = df_melted

        
        # Чистим числа
        df_report['sale_quantity'] = pd.to_numeric(df_report['sale_quantity'], errors='coerce').fillna(0)
        df_report = df_report[df_report['sale_quantity'] > 0].copy()
        
        # Технические поля
        count_rows = len(df_report)
        df_report['uuid_report'] = [str(uuid.uuid4()) for _ in range(count_rows)]
        df_report['name_report'] = name_report
        df_report['name_pharm_chain'] = name_pharm_chain
        df_report['start_date'] = str(start_date)
        df_report['end_date'] = str(end_date)
        df_report['processed_dttm'] = str(datetime.now())
        
        # Порядок колонок
        final_column_order = [
            'uuid_report',
            'branch_name',
            'city',
            'street',
            'product',
            'sale_quantity',
            'name_report',
            'name_pharm_chain',
            'start_date',
            'end_date',
            'processed_dttm'
        ]
        
        # Досоздаем поля, если их нет (для формата 2024 это не требуется, они уже созданы, но для надежности)
        for col in final_column_order:
            if col not in df_report.columns:
                df_report[col] = None
                
        df_report = df_report[final_column_order]
        
            
        loger.info(f'Сформирован отчет: {len(df_report)} строк.')
        
        return {
            'table_report': df_report
        }

    except Exception as e:
        loger.info(f'ERROR parsing sales file {path}: {str(e)}', exc_info=True)
        raise


if __name__ == "__main__": 
    test_path = r'C:\Users\nmankov\Desktop\отчеты\Ваш доктор\Закупки\2025\02_2025.xlsx'
    
    if not os.path.exists(test_path):
        test_path = os.path.join(os.getcwd(), test_path)

    if os.path.exists(test_path):
        print(f"Запуск парсинга файла: {test_path}")
        result = extract_all_xls(path=test_path, name_report='Закупки', name_pharm_chain='Ваш доктор')
        print("Первые 5 строк результата:")
        print(result['table_report'].head().to_string())
    else:
        print(f"Файл не найден: {test_path}")