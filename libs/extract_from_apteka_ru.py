import pandas as pd
import uuid
import os
from datetime import datetime
import hashlib
from airflow.utils.log.logging_mixin import LoggingMixin

def create_text_hash(row, columns):
    # Объединяем значения столбцов в строку
    combined = ''.join(str(row[col]) for col in columns)
    # Создаем хеш SHA256 и преобразуем в hex-строку
    return hashlib.sha256(combined.encode()).hexdigest()

def get_dates_from_filename(path):
    """
    Извлекает start_date и end_date из имени файла формата 'ММ_ГГГГ.xlsx'.
    """
    try:
        filename_no_ext = os.path.basename(path).split('.')[0]
        start_date = datetime.strptime(f"01.{filename_no_ext.replace('_', '.')}", "%d.%m.%Y")
        end_date = start_date + pd.offsets.MonthEnd(1)
        return start_date, end_date
    except Exception as e:
        raise ValueError(f"Имя файла должно быть в формате ММ_ГГГГ.xlsx. Ошибка: {e}")

def extract_sales_xls(path='', name_report='Продажи', name_pharm_chain='Аптека_Ру') -> dict:
    loger = LoggingMixin().log
    
    try:
        start_date, end_date = get_dates_from_filename(path)
        loger.info(f'Период отчета, извлеченный из файла: {start_date.date()} - {end_date.date()}')
        df = pd.read_excel(path, header=None)
        df = df.astype(str)
        loger.info(f'Успешно прочитано {len(df)} строк из файла.')
        
        try:
            start_row_index = df[df.iloc[:, 0] == 'Филиал'].index[0]
        except IndexError:
            raise ValueError("Не найдена строка заголовков (ожидалось слово 'Филиал' в первой колонке).")
        
        headers = df.iloc[start_row_index].values
        df.columns = headers
        df = df.drop(df.index[:start_row_index + 1]).reset_index(drop=True)
        df.dropna(how='all', inplace=True)
        
        rename_map = {
            'Филиал': 'branch_name',
            'Клиент': 'client_name',
            'ИНН клиента': 'client_inn',
            'Регион': 'region',
            'Город': 'city',
            'Улица': 'street',
            'Товар': 'product',
            'Продажи, шт.': 'sale_quantity'
        }

        df.rename(columns=rename_map, inplace=True)

        target_columns = list(rename_map.values())
        df_report = df[target_columns].copy()

        count_rows = len(df_report)
        
        df_report['uuid_report'] = [str(uuid.uuid4()) for _ in range(count_rows)]
        
        df_report['name_report'] = name_report
        df_report['name_pharm_chain'] = name_pharm_chain
        
        df_report['start_date'] = str(start_date)
        df_report['end_date'] = str(end_date)
        
        df_report['processed_dttm'] = str(datetime.now())

        df_report['sale_quantity'] = pd.to_numeric(df_report['sale_quantity'], errors='coerce').fillna(0)

        final_column_order = [
            'uuid_report',
            'branch_name',
            'client_name',
            'client_inn',
            'region',
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
        
        available_columns = [col for col in final_column_order if col in df_report.columns]
        df_report = df_report[available_columns]

        return {
            'table_report': df_report
        }

    except Exception as e:
        loger.info(f'ERROR parsing sales file {path}: {str(e)}', exc_info=True)
        raise

if __name__ == "__main__":
    
    test_path = 'C:/Users/nmankov/Desktop/отчеты/Аптека_Ру/Продажи/2023/01_2023.xlsx'
    
    if os.path.exists(test_path):
        print(f"локальный тест для файла: {test_path}")
        result = extract_sales_xls(path=test_path)
        print(result['table_report'].head())
    else:
        print(f"Файл не найден: {test_path}")