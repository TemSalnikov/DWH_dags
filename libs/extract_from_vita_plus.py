from datetime import datetime, timedelta
import uuid
import pandas as pd
import os
from airflow.utils.log.logging_mixin import LoggingMixin

def extract_custom(path='', name_report='Закупки', name_pharm_chain='Вита плюс') -> dict:
    """Универсальный парсер для отчетов 'Закупки', 'Продажи', 'Остатки'."""
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")

        # Извлекаем дату из имени файла
        filename = os.path.basename(path)
        date_part = os.path.splitext(filename)[0] # убираем расширение .xlsx
        
        # Преобразуем 'ММ_ГГГГ' в дату
        start_date = datetime.strptime(date_part, "%m_%Y")
        
        # Вычисляем последний день месяца
        # Переходим к первому дню следующего месяца и отнимаем один день
        end_date = (start_date.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        
        loger.info(f"Определен период отчета по имени файла: {start_date.date()} - {end_date.date()}")
        
        # Чтение и обработка данных из Excel
        df_raw = pd.read_excel(path, header=None, dtype=str)
        
        # Поиск строки с заголовками по наличию столбца 'Аптека'
        header_row_index = -1
        # Ищем по одному из обязательных полей, чтобы найти строку с заголовками
        possible_mandatory_headers = [['Аптека', 'Склад'], ['Код аптеки', 'Склад.Код', 'Аптека.Код'], ['Код товара', 'Товар.Код']]
        
        for i, row in df_raw.iterrows():
            row_values = set(str(v).lower() for v in row.values)
            if any(h.lower() in row_values for group in possible_mandatory_headers for h in group):
                header_row_index = i
                break
        
        if header_row_index == -1:
            raise ValueError("Не удалось найти строку с заголовками в файле.")
            
        headers = df_raw.iloc[header_row_index]
        df = df_raw.iloc[header_row_index + 1:].copy()
        df.columns = headers
        df.dropna(how='all', subset=df.columns.difference(['Аптека']), inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        loger.info(f'Успешно получено {len(df)} строк!')
        
        # Специальная обработка для полей "Код" и "код" с учетом регистра
        special_rename = {}
        if 'Код' in df.columns:
            special_rename['Код'] = 'product_code'
        if 'код' in df.columns:
            special_rename['код'] = 'drugstore_code'
        df.rename(columns=special_rename, inplace=True)
        
        # Создаем карту переименования, нечувствительную к регистру
        rename_map_config = {
            # Основные названия
            'аптека': 'drugstore',
            'код аптеки': 'drugstore_code',
            'код товара': 'product_code',
            'товар': 'product',
            'поставщик': 'supplier',
            'юр. лицо': 'legal_entity',
            'инн': 'inn',
            'количество закуп. упак.': 'quantity',
            'сумма закуп': 'total_cost',
            'контракт': 'contract',
            'номер накладной': 'invoice_number',
            'дата накладной': 'invoice_date',
            # Альтернативные названия
            'склад': 'drugstore',
            'склад.код': 'drugstore_code',
            'товар.код': 'product_code',
            'юр лицо': 'legal_entity',
            'организация': 'legal_entity',
            'количество': 'quantity',
            'вх. номер': 'invoice_number',
            'вх дата накл.': 'invoice_date',
            'аптека.код': 'drugstore_code',
            'организация.инн': 'inn',
            'инн юрлица аптеки': 'inn',
            'кол-во продажи, упак.': 'sale_quantity_pack'
        }
        actual_rename_map = {col: rename_map_config.get(str(col).lower()) for col in df.columns if str(col).lower() in rename_map_config}
        df.rename(columns=actual_rename_map, inplace=True)
        
        # Добавляем опциональные колонки, если их нет в файле
        final_report_columns = [
            'drugstore', 'drugstore_code', 'product_code', 'product', 'supplier',
            'legal_entity', 'inn', 'quantity', 'total_cost', 'contract', 'invoice_number', 'invoice_date',
            'sale_quantity_pack'
        ]
        
        for col in final_report_columns:
            if col not in df.columns:
                df[col] = None
                loger.info(f"Добавлена отсутствующая колонка: '{col}'")

        # Создаем итоговый DataFrame только с нужными колонками
        df_report = df[final_report_columns].copy()
        
        # Добавляем технические поля
        df_report['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_report))]
        df_report['name_report'] = name_report
        df_report['name_pharm_chain'] = name_pharm_chain
        df_report['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df_report['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df_report['processed_dttm'] = str(datetime.now())

        # Упорядочиваем столбцы для наглядности
        final_columns = [
            'uuid_report', 'drugstore', 'drugstore_code', 'product_code', 'product', 'supplier',
            'legal_entity', 'inn', 'quantity', 'total_cost', 'contract', 'invoice_number', 'invoice_date', 'sale_quantity_pack',
            'name_report', 'name_pharm_chain', 
            'start_date', 'end_date', 'processed_dttm'
        ]
        df_report = df_report[final_columns]

        return {'table_report': df_report}
    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_xls(path, name_report, name_pharm_chain) -> dict:
    """
    Основная функция-диспетчер, вызывает нужный парсер в зависимости от типа отчета.
    """
    # Все типы отчетов обрабатываются единой функцией
    return extract_custom(path, name_report, name_pharm_chain)

if __name__ == "__main__":
    # Блок для локального тестирования
    loger = LoggingMixin().log
    main_loger = LoggingMixin().log

    # --- Укажите путь к папке с вашими тестовыми файлами ---
    test_folder_path = r'C:\Users\nmankov\Desktop\отчеты\Вита Плюс\Закупки\2024'

    if os.path.isdir(test_folder_path):
        main_loger.info(f"Запуск локального теста для папки: {test_folder_path}")
        
        for filename in os.listdir(test_folder_path):
            if filename.endswith(('.xlsx', '.xls')):
                file_path = os.path.join(test_folder_path, filename)
                main_loger.info(f"--- Обработка файла: {file_path} ---")
                
                try:
                    # Вызов основной функции парсера
                    # Определяем тип отчета по наличию ключевых слов в пути
                    report_type = 'Закупки' # по умолчанию
                    if 'остатки' in file_path.lower():
                        report_type = 'Остатки'
                    elif 'продажи' in file_path.lower():
                        report_type = 'Продажи'

                    result_data = extract_xls(path=file_path, name_report=report_type, name_pharm_chain='Вита плюс')

                    # Сохранение результатов в ту же папку
                    for table_name, df in result_data.items():
                        if not df.empty:
                            output_filename = f"{os.path.splitext(filename)[0]}_result.csv"
                            output_path = os.path.join(test_folder_path, output_filename)
                            df.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
                            main_loger.info(f"Результат '{table_name}' для файла '{filename}' успешно сохранен в: {output_path}")

                except Exception as e:
                    main_loger.error(f"Во время обработки файла {filename} произошла ошибка: {e}", exc_info=True)
    else:
        main_loger.warning(f"Папка для теста не найдена по пути: {test_folder_path}")