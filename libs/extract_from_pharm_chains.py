from datetime import datetime, timedelta
import json
import uuid
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
import hashlib
import os


def create_text_hash(row, columns):
    """Создание хеша из значений указанных колонок."""
    combined = ''.join(str(row[col]) for col in columns)
    return hashlib.sha256(combined.encode()).hexdigest()


def extract_data(path='', sheet_name='', name_pharm_chain=''):
    """Извлечение данных из одного листа Excel файла."""
    logger = LoggingMixin().log
    try:
        df = pd.read_excel(path, sheet_name)
        df = df.astype(str)
        logger.info(f'Успешно получено {df[df.columns[0]].count()} строк из листа "{sheet_name}"!')
        return df
    except Exception as e:
        logger.error(f'Ошибка при чтении листа {sheet_name}: {str(e)}', exc_info=True)
        raise


def table_conversion(df: pd.DataFrame, name_pharm_chain='', sheet_name=''):
    """
    Преобразование данных из вашего формата в структурированный DataFrame.
    Ваш файл имеет формат: ИНН, Наименование, Бренд, Признак.
    """
    logger = LoggingMixin().log
    
    try:
        # Копируем DataFrame для безопасности
        result = df.iloc[:, :3]
        
        # Переименовываем колонки для единообразия
        result.columns = ['inn', 'legal_name', 'brand']
        result = result.replace("nan", '')
        # Добавляем идентификаторы и метаданные
        result['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(result))]
        result['processed_dttm'] = [str(datetime.now()) for _ in range(len(result))]
        
        # Извлекаем даты из названия листа или файла
        # Формат: "1 кв 2025", "4 кв 2025" и т.д.
        logger.info(f'Успешно преобразовано {len(result)} записей')
        return result
        
    except Exception as e:
        logger.error(f'Ошибка при преобразовании данных: {str(e)}', exc_info=True)
        raise


def extract_xls(path='', name_report='', name_pharm_chain='Прямые сети') -> dict:
    """
    Основная функция для извлечения данных из всех листов Excel файла.
    Возвращает словарь с ключом 'table_report', содержащий объединенный DataFrame.
    """
    logger = LoggingMixin().log
    result_dfs = []
    
    try:
        # Получаем имя файла без расширения для использования в качестве name_report
        if not name_report:
            name_report = ''
        
        # Читаем все листы Excel файла
        xls = pd.ExcelFile(path)
        sheet_names = xls.sheet_names
        
        logger.info(f'Найдены листы: {sheet_names}')
        
        for sheet_name in sheet_names:
            try:
                # Извлекаем данные из листа
                df = extract_data(path, sheet_name, name_pharm_chain)
                
                # Преобразуем данные в нужный формат
                converted_df = table_conversion(df, name_pharm_chain, sheet_name)
                
                # Добавляем в общий список
                result_dfs.append(converted_df)
                
                logger.info(f'Обработан лист: {sheet_name} ({len(converted_df)} записей)')
                
            except Exception as e:
                logger.error(f'Ошибка при обработке листа {sheet_name}: {str(e)}')
                raise
        
        if result_dfs:
            # Объединяем все DataFrame
            result_df = pd.concat(result_dfs, ignore_index=True)
            
            # Добавляем дополнительный хеш для уникальности записей
            result_df['record_hash'] = result_df.apply(
                lambda row: create_text_hash(row, ['inn', 'legal_name', 'brand']), 
                axis=1
            )
            
            logger.info(f'Всего обработано {len(result_df)} записей из {len(sheet_names)} листов')
            
            return {
                'table_report': result_df
            }
        else:
            logger.error('Не удалось обработать ни одного листа')
            return {
                'table_report': pd.DataFrame()
            }
            
    except Exception as e:
        logger.error(f'Критическая ошибка при обработке файла: {str(e)}', exc_info=True)
        raise


# Дополнительная функция для сохранения результатов в JSON (опционально)
def save_to_json(data_dict, output_path):
    """Сохранение результатов в JSON файл."""
    try:
        # Преобразуем DataFrame в словарь
        if 'table_report' in data_dict and not data_dict['table_report'].empty:
            # Конвертируем DataFrame в список словарей
            records = data_dict['table_report'].to_dict('records')
            
            # Сохраняем в JSON
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            
            print(f'Результаты сохранены в {output_path}')
            return True
    except Exception as e:
        print(f'Ошибка при сохранении JSON: {str(e)}')
    return False




if __name__ == "__main__":
    # Пример использования
    input_file = "~/Загрузки/Прямые сети ИНН 4 кв 2025_СВОД_ФИНАЛ_с ассоциациями_.xlsx"
    
    # Извлекаем данные из всех листов
    result = extract_xls(
        path=input_file,
        name_report="Прямые сети 4 кв 2025",
        name_pharm_chain="Фармаимпекс, Антей, Невис"
    )
    
    # Выводим информацию о результате
    if 'table_report' in result and not result['table_report'].empty:
        df = result['table_report']
        print(f"\nОбщая статистика:")
        print(f"Всего записей: {len(df)}")
        print(f"Колонки: {list(df.columns)}")
        print(f"\nПервые 5 записей:")
        print(df.head())
        print(f"\nУникальные бренды: {df['brand'].unique()}")
        # print(f"Уникальные типы: {df['type'].unique()}")
        print(f"\nКоличество записей по брендам:")
        print(df['brand'].value_counts())
        
        # Сохраняем результаты
        save_to_json(result, "результат_выгрузки.json")
    else:
        print("Не удалось извлечь данные")