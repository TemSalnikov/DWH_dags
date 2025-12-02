import pandas as pd
import uuid
import os
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
from dateutil.relativedelta import relativedelta

def extract_xls(path='', name_report='Закуп_Продажи_Остатки', name_pharm_chain='Весна') -> dict:
    loger = LoggingMixin().log
    
    try:
        xls = pd.ExcelFile(path)
        sheet_names = xls.sheet_names
        
        all_sheets_data = []

        # Ожидаемые листы
        expected_sheets = ['закупки', 'продажи', 'остатки']

        for sheet_name in sheet_names:
            if sheet_name.lower() in expected_sheets:
                loger.info(f"Читаем лист: {sheet_name}")
                df_sheet = pd.read_excel(xls, sheet_name=sheet_name)
                # Приводим названия всех столбцов к нижнему регистру для унификации
                df_sheet.columns = [col.lower() for col in df_sheet.columns]
                df_sheet['name_report'] = sheet_name.lower()
                all_sheets_data.append(df_sheet)

        if not all_sheets_data:
            raise ValueError("В файле не найдены листы 'Закупки', 'Продажи' или 'Остатки'.")

        # Объединяем данные со всех листов
        df = pd.concat(all_sheets_data, ignore_index=True, sort=False)
        loger.info(f'Успешно объединено {len(df)} строк со всех листов.')

        # Нормализация названий столбцов
        rename_map = {
            'юр. лицо': 'legal_entity',
            'инн': 'inn',
            'номенклатура (словарь)': 'product',
            'кол-во': 'quantity',
            'дистрибьютор (ориг)': 'distributor',
            'производитель': 'manufacturer',
            'адрес': 'address',
            'дата': 'operation_date'
        }
        df.rename(columns=rename_map, inplace=True)

        # Преобразуем колонку с датой и находим минимальную и максимальную
        df['operation_date'] = pd.to_datetime(df['operation_date'], errors='coerce')
        start_date = df['operation_date'].min()
        end_date = df['operation_date'].max()

        if start_date == end_date:
            end_date = end_date + timedelta(days=1)
        
        loger.info(f'Определен период отчета по данным: {start_date.date()} - {end_date.date()}')

        # Заменяем NaN на None для корректной сериализации в JSON
        df = df.where(pd.notna(df), None)

        # Технические поля
        count_rows = len(df)
        df['uuid_report'] = [str(uuid.uuid4()) for _ in range(count_rows)]
        df['name_pharm_chain'] = name_pharm_chain
        df['start_date'] = str(start_date.date())
        df['end_date'] = str(end_date.date())
        df['processed_dttm'] = str(datetime.now())

        # Финальный порядок колонок
        final_column_order = [
            'uuid_report',
            'legal_entity',
            'inn',
            'product',
            'quantity',
            'distributor',
            'manufacturer',
            'address',
            'operation_date',
            'name_report',
            'name_pharm_chain',
            'start_date',
            'end_date',
            'processed_dttm'
        ]
        
        # Убедимся, что все колонки присутствуют
        for col in final_column_order:
            if col not in df.columns:
                df[col] = None
        
        df_report = df[final_column_order]

        return {
            'table_report': df_report
        }

    except Exception as e:
        loger.error(f'ERROR parsing file {path}: {str(e)}', exc_info=True)
        raise

if __name__ == "__main__":
    # --- Блок для локального тестирования ---
    main_loger = LoggingMixin().log

    test_file_path = r'C:\Users\nmankov\Desktop\отчеты\Весна\Закуп Продажи Остатки\2024\03_2024.xlsx'

    if os.path.exists(test_file_path):
        main_loger.info(f"Запуск локального теста для файла: {test_file_path}")
        try:
            # Вызов основной функции парсера
            result_data = extract_xls(path=test_file_path)
            df_result = result_data.get('table_report')


            output_path = os.path.join(os.path.dirname(__file__), 'vesna_test_result.csv')
            df_result.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
            main_loger.info(f"Результат успешно сохранен в файл: {output_path}")

        except Exception as e:
            main_loger.error(f"Во время локального теста произошла ошибка: {e}", exc_info=True)
    else:
        main_loger.warning(f"Тестовый файл не найден по пути: {test_file_path}")