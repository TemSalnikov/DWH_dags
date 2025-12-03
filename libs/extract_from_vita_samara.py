import pandas as pd
import uuid
import os
import calendar
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin

def extract_custom(path='', name_report='Закупки', name_pharm_chain='Вита Самара') -> dict:
    """
    Парсер для отчета 'Закупки' от 'Вита Самара'.
    """
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")

        df_raw = pd.read_excel(path, header=None, dtype=str)

        # --- Извлечение периода ---
        period_str = df_raw.iloc[2, 0] # 3-я строка, 1-я колонка
        start_date_str, end_date_str = period_str.replace('c ', '').split(' по ')
        start_date = datetime.strptime(start_date_str.strip(), "%d.%m.%Y")
        end_date = datetime.strptime(end_date_str.strip(), "%d.%m.%Y")
        loger.info(f"Определен период отчета: {start_date.date()} - {end_date.date()}")

        header_row_index = -1
        for i, row in df_raw.iterrows():
            if '№п.п' in row.astype(str).values:
                header_row_index = i
                break
        
        if header_row_index == -1:
            raise ValueError("Не удалось найти строку с заголовками в файле.")

        headers = df_raw.iloc[header_row_index]
        df = df_raw.iloc[header_row_index + 1:].copy()
        df.columns = headers
        df.dropna(how='all', inplace=True)
        df.reset_index(drop=True, inplace=True)
        loger.info(f'Успешно получено {len(df)} строк!')

        rename_map = {
            '№п.п': 'row_number',
            'Код': 'product_code',
            'Наименование': 'product_name',
            'Производитель': 'manufacturer',
            'Поставщик': 'supplier',
            'Дата прихода': 'receipt_date',
            'Номер накладной': 'invoice_number',
            'Количество': 'quantity'
        }
        df.rename(columns=rename_map, inplace=True)

        df_report = df.copy()

        df_report['address'] = None
        df_report['group_abc'] = None
        df_report['group_xyz'] = None
        df_report['group_abc_lower'] = None

        df_report.drop(columns=['row_number'], inplace=True)

        df_report['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_report))]
        df_report['name_report'] = name_report
        df_report['name_pharm_chain'] = name_pharm_chain
        df_report['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df_report['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df_report['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Финальный набор колонок для унификации
        final_columns = [
            'uuid_report', 'product_code', 'product_name', 'manufacturer', 'supplier', 'receipt_date',
            'invoice_number', 'quantity', 'address', 'group_abc', 'group_xyz', 'group_abc_lower',
            'name_report', 'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm'
        ]
        for col in final_columns:
            if col not in df_report.columns:
                df_report[col] = None
        
        df_report = df_report[final_columns]

        loger.info("Парсинг успешно завершен.")
        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def _extract_sales_or_remains(path='', name_report='Продажи', name_pharm_chain='Вита Самара') -> dict:
    """
    Универсальный парсер для отчетов 'Продажи' и 'Остатки' от 'Вита Самара'.
    """
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")

        filename = os.path.basename(path)
        date_part = os.path.splitext(filename)[0]
        file_month_date = datetime.strptime(date_part, "%m_%Y")
        start_date = file_month_date
        _, last_day = calendar.monthrange(file_month_date.year, file_month_date.month)
        end_date = file_month_date.replace(day=last_day)
        loger.info(f"Определен период отчета по имени файла: {start_date.date()} - {end_date.date()}")

        df = pd.read_excel(path, header=1, dtype=str)
        
        if 'итог' in str(df.columns[-1]).lower():
            df.drop(df.columns[-1], axis=1, inplace=True)

        df.dropna(how='all', inplace=True)
        if not df.empty:
            df.drop(df.tail(1).index, inplace=True)

        # Преобразуем "широкую" таблицу в "длинную"
        id_vars = ['Код', 'Наименование', 'Производитель'] # Колонки-идентификаторы
        
        address_vars = []
        group_vars = []
        for col in df.columns:
            if col not in id_vars:
                if str(col).lower().startswith('группа|'):
                    group_vars.append(col)
                else:
                    address_vars.append(col)
        
        df_melted = df.melt(id_vars=id_vars + group_vars, value_vars=address_vars, var_name='address', value_name='quantity')

        df_melted.dropna(subset=['quantity'], inplace=True)
        df_melted = df_melted[df_melted['quantity'] != '0'].copy()

        df_melted.reset_index(drop=True, inplace=True)
        loger.info(f'Успешно получено {len(df_melted)} строк после преобразования!')

        # Переименование и добавление технических полей
        rename_map = {
            'Код': 'product_code',
            'Наименование': 'product_name',
            'Производитель': 'manufacturer',
            'Группа|ABC': 'group_abc',
            'Группа|XYZ': 'group_xyz',
            'Группа|abc': 'group_abc_lower' # на случай если есть оба варианта
        }
        actual_rename_map = {k: v for k, v in rename_map.items() if k in df_melted.columns}
        df_melted.rename(columns=actual_rename_map, inplace=True)
        
        expected_group_cols = ['group_abc', 'group_xyz', 'group_abc_lower']
        for col in expected_group_cols:
            if col not in df_melted.columns:
                df_melted[col] = None

        df_melted['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_melted))]
        df_melted['name_report'] = name_report
        df_melted['name_pharm_chain'] = name_pharm_chain
        df_melted['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df_melted['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df_melted['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        df_melted['supplier'] = None
        df_melted['receipt_date'] = None
        df_melted['invoice_number'] = None

        final_columns = [
            'uuid_report', 'product_code', 'product_name', 'manufacturer', 'supplier', 'receipt_date',
            'invoice_number', 'quantity', 'address', 'group_abc', 'group_xyz', 'group_abc_lower',
            'name_report', 'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm'
        ]
        for col in final_columns:
            if col not in df_melted.columns:
                df_melted[col] = None
        
        df_report = df_melted[final_columns]

        return {'table_report': df_report}
    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_xls(path, name_report, name_pharm_chain) -> dict:
    """
    Диспетчер, вызывающий нужный парсер в зависимости от типа отчета.
    """
    loger = LoggingMixin().log
    loger.info(f"Диспетчер получил задачу: '{name_report}' для '{name_pharm_chain}'")

    if 'закуп' in name_report.lower():
        return extract_custom(path, name_report, name_pharm_chain)
    elif 'продажи' in name_report.lower() or 'остатки' in name_report.lower():
        return _extract_sales_or_remains(path, name_report, name_pharm_chain)
    else:
        loger.warning(f"Неизвестный тип отчета: '{name_report}'. Парсер не будет вызван.")
        return {}

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    test_file_path = r'C:\Users\nmankov\Desktop\отчеты\ВИТА Самара\Остатки\2024\10_2024.xlsx'
    
    if os.path.exists(test_file_path):
        main_loger.info(f"Запуск локального теста для файла: {test_file_path}")
        try:
            result_data = extract_xls(path=test_file_path, name_report='Остатки', name_pharm_chain='Вита Самара')
            df_result = result_data.get('table_report')
            
            if df_result is not None and not df_result.empty:
                output_path = os.path.join(os.path.dirname(test_file_path), f"{os.path.splitext(os.path.basename(test_file_path))[0]}_result.csv")
                df_result.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
                main_loger.info(f"Результат успешно сохранен в: {output_path}")
        except Exception as e:
            main_loger.error(f"Во время теста произошла ошибка: {e}", exc_info=True)
    else:
        main_loger.warning(f"Тестовый файл не найден: {test_file_path}")