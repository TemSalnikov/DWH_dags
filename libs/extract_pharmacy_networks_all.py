from datetime import datetime, timedelta
import pandas as pd
import json
import uuid
import hashlib
import os
from typing import Dict, List, Optional

def create_text_hash(row, columns):
    """Создает хеш строки на основе указанных колонок"""
    combined = ''.join(str(row[col]) for col in columns)
    return hashlib.sha256(combined.encode()).hexdigest()

def extract_period_from_sheet_name(sheet_name: str) -> Dict:
    """
    Извлекает информацию о периоде из названия листа.
    Обрабатывает форматы: '1 кв 2025', 'Q1 2025', 'январь 2025' и т.д.
    """
    period_info = {'start_date': '', 'end_date': '', 'quarter': '', 'year': ''}
    
    sheet_name_lower = sheet_name.lower().strip()
    
    try:
        # Поиск года (4 цифры подряд)
        import re
        year_match = re.search(r'(\d{4})', sheet_name_lower)
        if year_match:
            year = int(year_match.group(1))
            period_info['year'] = str(year)
        
        # Поиск квартала
        quarter_match = re.search(r'(\d)\s*кв', sheet_name_lower)
        if quarter_match:
            quarter = int(quarter_match.group(1))
            period_info['quarter'] = f'Q{quarter}'
            
            # Определяем даты квартала
            if quarter == 1:
                start_date = f'{year}-01-01'
                end_date = f'{year}-03-31'
            elif quarter == 2:
                start_date = f'{year}-04-01'
                end_date = f'{year}-06-30'
            elif quarter == 3:
                start_date = f'{year}-07-01'
                end_date = f'{year}-09-30'
            elif quarter == 4:
                start_date = f'{year}-10-01'
                end_date = f'{year}-12-31'
            else:
                start_date = f'{year}-01-01'
                end_date = f'{year}-12-31'
            
            period_info['start_date'] = start_date
            period_info['end_date'] = end_date
        
        # Поиск месяца
        months = {
            'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4,
            'май': 5, 'июнь': 6, 'июль': 7, 'август': 8,
            'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
        }
        
        for month_name, month_num in months.items():
            if month_name in sheet_name_lower:
                period_info['month'] = month_name
                start_date = f'{year}-{month_num:02d}-01'
                # Определяем последний день месяца
                if month_num == 12:
                    end_date = f'{year}-12-31'
                else:
                    end_date = f'{year}-{month_num+1:02d}-01'
                    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=1)
                    end_date = end_date_dt.strftime('%Y-%m-%d')
                
                period_info['start_date'] = start_date
                period_info['end_date'] = end_date
                break
        
    except Exception as e:
        print(f"Не удалось извлечь период из названия листа '{sheet_name}': {e}")
    
    return period_info

def extract_data_from_sheet(file_path: str, sheet_name: str, 
                           name_pharm_chain: str = 'Прямые сети аптек') -> Optional[pd.DataFrame]:
    """
    Извлекает данные из конкретного листа Excel файла
    """
    try:
        # Чтение Excel листа
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        # Проверяем, есть ли данные в файле
        if df.empty:
            print(f"Лист '{sheet_name}' пустой, пропускаем...")
            return None
        
        # Приводим все данные к строковому типу
        df = df.astype(str)
        
        # Определяем структуру данных (колонки могут быть разные)
        print(f"\nАнализ структуры листа '{sheet_name}':")
        print(f"  Колонки: {list(df.columns)}")
        print(f"  Количество строк: {len(df)}")
        print(f"  Первые 2 строки данных:")
        print(df.head(2).to_string())
        
        # Извлекаем период из названия листа
        period_info = extract_period_from_sheet_name(sheet_name)
        
        # Если не удалось извлечь период, используем название листа как период
        if not period_info['start_date']:
            period_info['period_name'] = sheet_name
        
        # Определяем, какие колонки у нас есть
        columns_mapping = {}
        
        # Ищем стандартные колонки по разным возможным названиям
        for col in df.columns:
            col_lower = str(col).lower()
            
            if 'инн' in col_lower or 'inn' in col_lower:
                columns_mapping[col] = 'inn'
            elif 'наименование' in col_lower or 'юридическое лицо' in col_lower or 'юр. лицо' in col_lower:
                columns_mapping[col] = 'legal_entity_name'
            elif 'бренд' in col_lower or 'brand' in col_lower:
                columns_mapping[col] = 'brand'
            elif 'признак' in col_lower or 'тип' in col_lower or 'type' in col_lower:
                columns_mapping[col] = 'type'
            elif 'аптека' in col_lower and 'название' in col_lower:
                columns_mapping[col] = 'pharmacy_name'
        
        # Если не нашли стандартные колонки, используем первые 4 колонки
        if not columns_mapping and len(df.columns) >= 4:
            columns_mapping = {
                df.columns[0]: 'inn',
                df.columns[1]: 'legal_entity_name',
                df.columns[2]: 'brand',
                df.columns[3]: 'type'
            }
            print(f"  Используем стандартное сопоставление колонок для листа '{sheet_name}'")
        
        # Переименовываем колонки
        if columns_mapping:
            df = df.rename(columns=columns_mapping)
            print(f"  Переименованные колонки: {list(df.columns)}")
        
        # Добавляем служебные поля
        df['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df['processed_dttm'] = [str(datetime.now()) for _ in range(len(df))]
        df['sheet_name'] = sheet_name
        df['name_pharm_chain'] = name_pharm_chain
        
        # Добавляем информацию о периоде
        for key, value in period_info.items():
            df[key] = value
        
        # Создание уникального хеша для каждой записи
        if 'inn' in df.columns and 'legal_entity_name' in df.columns and 'brand' in df.columns:
            df['record_hash'] = df.apply(
                lambda row: create_text_hash(row, ['inn', 'legal_entity_name', 'brand']), 
                axis=1
            )
        else:
            # Если нет нужных колонок, используем все доступные
            available_cols = [col for col in df.columns if col not in ['uuid_report', 'processed_dttm', 'record_hash']]
            df['record_hash'] = df.apply(
                lambda row: create_text_hash(row, available_cols[:3]), 
                axis=1
            )
        
        print(f"  Успешно обработано {len(df)} записей")
        return df
        
    except Exception as e:
        print(f"Ошибка при обработке листа '{sheet_name}': {str(e)}")
        return None

def process_all_sheets(file_path: str, 
                      name_pharm_chain: str = 'Прямые сети аптек',
                      specific_sheets: List[str] = None) -> Dict:
    """
    Обрабатывает все листы Excel файла
    
    Args:
        file_path: путь к Excel файлу
        name_pharm_chain: название фармацевтической сети
        specific_sheets: список конкретных листов для обработки (если None, то все)
    """
    try:
        print(f"\n{'='*60}")
        print(f"НАЧАЛО ОБРАБОТКИ ФАЙЛА: {os.path.basename(file_path)}")
        print(f"{'='*60}")
        
        # Получаем список всех листов в файле
        excel_file = pd.ExcelFile(file_path)
        all_sheets = excel_file.sheet_names
        
        print(f"\nНайдены следующие листы:")
        for i, sheet in enumerate(all_sheets, 1):
            print(f"  {i}. {sheet}")
        
        # Определяем, какие листы обрабатывать
        if specific_sheets:
            sheets_to_process = [s for s in specific_sheets if s in all_sheets]
            if not sheets_to_process:
                print(f"\n⚠ Предупреждение: Указанные листы {specific_sheets} не найдены в файле")
                sheets_to_process = all_sheets
        else:
            sheets_to_process = all_sheets
        
        print(f"\nБудут обработаны следующие листы:")
        for i, sheet in enumerate(sheets_to_process, 1):
            print(f"  {i}. {sheet}")
        
        # Обрабатываем каждый лист
        all_dataframes = []
        sheet_stats = {}
        
        for sheet_name in sheets_to_process:
            print(f"\n{'─'*40}")
            print(f"ОБРАБОТКА ЛИСТА: {sheet_name}")
            print(f"{'─'*40}")
            
            df = extract_data_from_sheet(file_path, sheet_name, name_pharm_chain)
            
            if df is not None and not df.empty:
                all_dataframes.append(df)
                sheet_stats[sheet_name] = {
                    'records': len(df),
                    'columns': list(df.columns),
                    'brands': df['brand'].nunique() if 'brand' in df.columns else 0,
                    'unique_entities': df['legal_entity_name'].nunique() if 'legal_entity_name' in df.columns else 0
                }
                print(f"✓ Лист '{sheet_name}' успешно обработан ({len(df)} записей)")
            else:
                sheet_stats[sheet_name] = {'records': 0, 'error': 'Пустой или необрабатываемый лист'}
                print(f"✗ Лист '{sheet_name}' не содержит данных или произошла ошибка")
        
        # Объединяем все DataFrame
        if all_dataframes:
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            
            print(f"\n{'='*60}")
            print("ОБЪЕДИНЕНИЕ ДАННЫХ СО ВСЕХ ЛИСТОВ")
            print(f"{'='*60}")
            print(f"Всего листов обработано: {len(all_dataframes)}")
            print(f"Общее количество записей: {len(combined_df)}")
            print(f"Общие колонки в объединенном DataFrame:")
            for col in combined_df.columns:
                print(f"  - {col}")
            
            # Статистика по всем данным
            analyze_combined_data(combined_df, sheet_stats)
            
            return {
                'combined_dataframe': combined_df,
                'sheet_dataframes': {sheet: df for sheet, df in zip(sheets_to_process, all_dataframes) if df is not None},
                'sheet_stats': sheet_stats,
                'file_info': {
                    'file_name': os.path.basename(file_path),
                    'total_sheets': len(all_sheets),
                    'processed_sheets': len(all_dataframes),
                    'total_records': len(combined_df)
                }
            }
        else:
            print("\n⚠ Внимание: Не удалось обработать ни один лист!")
            return {}
            
    except Exception as e:
        print(f"\n❌ Критическая ошибка при обработке файла: {str(e)}")
        return {}

def analyze_combined_data(df: pd.DataFrame, sheet_stats: Dict):
    """Анализирует объединенные данные"""
    print(f"\n{'='*60}")
    print("АНАЛИЗ ОБЪЕДИНЕННЫХ ДАННЫХ")
    print(f"{'='*60}")
    
    # Статистика по листам
    print(f"\nСтатистика по листам:")
    for sheet_name, stats in sheet_stats.items():
        if 'records' in stats and stats['records'] > 0:
            print(f"\n  Лист: {sheet_name}")
            print(f"    Записей: {stats['records']}")
            if 'brands' in stats:
                print(f"    Уникальных брендов: {stats['brands']}")
            if 'unique_entities' in stats:
                print(f"    Уникальных юр. лиц: {stats['unique_entities']}")
        elif 'error' in stats:
            print(f"\n  Лист: {sheet_name}")
            print(f"    Статус: {stats['error']}")
    
    # Общая статистика
    if not df.empty:
        print(f"\nОбщая статистика по всем данным:")
        print(f"  Всего записей: {len(df)}")
        
        if 'sheet_name' in df.columns:
            unique_sheets = df['sheet_name'].nunique()
            print(f"  Уникальных листов: {unique_sheets}")
        
        if 'brand' in df.columns:
            brand_stats = df['brand'].value_counts()
            print(f"\n  Распределение по брендам:")
            for brand, count in brand_stats.head(10).items():
                print(f"    {brand}: {count} записей ({count/len(df)*100:.1f}%)")
        
        if 'legal_entity_name' in df.columns:
            unique_entities = df['legal_entity_name'].nunique()
            print(f"\n  Уникальных юридических лиц: {unique_entities}")
            
            # Топ аптек
            top_entities = df['legal_entity_name'].value_counts().head(10)
            print(f"  Топ-10 юридических лиц по количеству упоминаний:")
            for i, (entity, count) in enumerate(top_entities.items(), 1):
                print(f"    {i}. {entity}: {count} записей")
        
        if 'start_date' in df.columns and df['start_date'].notna().any():
            periods = df[['start_date', 'end_date']].drop_duplicates()
            print(f"\n  Периоды в данных:")
            for _, row in periods.iterrows():
                print(f"    {row['start_date']} - {row['end_date']}")

def save_to_json(df: pd.DataFrame, output_path: str = 'output.json', 
                metadata: Dict = None) -> str:
    """Сохраняет DataFrame в JSON файл с метаданными"""
    try:
        # Конвертируем в словарь
        result = {
            'metadata': metadata or {
                'total_records': len(df),
                'generated_at': str(datetime.now()),
                'data_sources': df['sheet_name'].unique().tolist() if 'sheet_name' in df.columns else [],
                'unique_brands': df['brand'].nunique() if 'brand' in df.columns else 0
            },
            'data': df.to_dict('records')
        }
        
        # Сохраняем в JSON
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ Данные сохранены в JSON: {output_path}")
        return output_path
        
    except Exception as e:
        print(f"\n✗ Ошибка при сохранении в JSON: {str(e)}")
        raise

def save_to_excel(df: pd.DataFrame, output_path: str = 'output.xlsx',
                 sheet_stats: Dict = None) -> str:
    """Сохраняет данные в Excel файл с несколькими листами"""
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Сохраняем объединенные данные
            df.to_excel(writer, sheet_name='Все данные', index=False)
            
            # Сохраняем статистику по листам
            if sheet_stats:
                stats_df = pd.DataFrame.from_dict(sheet_stats, orient='index')
                stats_df.to_excel(writer, sheet_name='Статистика по листам')
            
            # Сохраняем сводку по брендам
            if 'brand' in df.columns:
                brand_summary = df['brand'].value_counts().reset_index()
                brand_summary.columns = ['Бренд', 'Количество записей']
                brand_summary.to_excel(writer, sheet_name='Сводка по брендам', index=False)
            
            # Если есть информация о листах, сохраняем отдельные листы
            if 'sheet_name' in df.columns:
                for sheet in df['sheet_name'].unique():
                    sheet_df = df[df['sheet_name'] == sheet]
                    # Обрезаем имя листа, если оно слишком длинное
                    safe_sheet_name = sheet[:31]  # Excel ограничение 31 символ
                    sheet_df.to_excel(writer, sheet_name=f'Данные - {safe_sheet_name}', index=False)
        
        print(f"✓ Данные сохранены в Excel: {output_path}")
        return output_path
        
    except Exception as e:
        print(f"✗ Ошибка при сохранении в Excel: {str(e)}")
        raise

def main():
    """Основная функция для запуска обработки"""
    # Укажите путь к вашему файлу
    file_path = "Прямые сети ИНН 4 кв 2025_СВОД_ФИНАЛ_с ассоциациями_.xlsx"
    
    # Или укажите путь через диалог
    # import tkinter as tk
    # from tkinter import filedialog
    # root = tk.Tk()
    # root.withdraw()
    # file_path = filedialog.askopenfilename(
    #     title="Выберите Excel файл",
    #     filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")]
    # )
    
    if not os.path.exists(file_path):
        print(f"\n❌ Файл '{file_path}' не найден!")
        print("Убедитесь, что файл находится в правильной директории.")
        
        # Попробуем найти файл в текущей директории
        current_dir = os.getcwd()
        files = [f for f in os.listdir(current_dir) if f.endswith(('.xlsx', '.xls'))]
        if files:
            print(f"\nНайденные Excel файлы в текущей директории:")
            for i, f in enumerate(files, 1):
                print(f"  {i}. {f}")
            choice = input("\nВведите номер файла для обработки (или 0 для выхода): ")
            if choice.isdigit() and 0 < int(choice) <= len(files):
                file_path = files[int(choice) - 1]
            else:
                return
        else:
            return
    
    # Опционально: укажите конкретные листы для обработки
    # specific_sheets = ['1 кв 2025', '2 кв 2025', '3 кв 2025', '4 кв 2025']
    specific_sheets = None  # Обрабатывать все листы
    
    # Название фармацевтической сети
    name_pharm_chain = "Прямые сети аптек"
    
    # Обрабатываем файл
    result = process_all_sheets(
        file_path=file_path,
        name_pharm_chain=name_pharm_chain,
        specific_sheets=specific_sheets
    )
    
    if result and 'combined_dataframe' in result:
        # Генерируем имя файла на основе исходного
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Сохраняем результаты
        json_path = f"{base_name}_all_sheets_{timestamp}.json"
        excel_path = f"{base_name}_all_sheets_{timestamp}.xlsx"
        csv_path = f"{base_name}_all_sheets_{timestamp}.csv"
        
        # Сохраняем в JSON
        metadata = {
            'source_file': file_path,
            'processed_at': str(datetime.now()),
            'total_sheets': len(result['sheet_stats']),
            'total_records': len(result['combined_dataframe']),
            'pharm_chain': name_pharm_chain
        }
        
        save_to_json(result['combined_dataframe'], json_path, metadata)
        
        # Сохраняем в Excel с несколькими листами
        save_to_excel(result['combined_dataframe'], excel_path, result['sheet_stats'])
        
        # Сохраняем в CSV
        result['combined_dataframe'].to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"✓ Данные сохранены в CSV: {csv_path}")
        
        # Отчет о завершении
        print(f"\n{'='*60}")
        print("ОБРАБОТКА ЗАВЕРШЕНА УСПЕШНО!")
        print(f"{'='*60}")
        print(f"Исходный файл: {os.path.basename(file_path)}")
        print(f"Обработано листов: {result['file_info']['processed_sheets']}")
        print(f"Всего записей: {result['file_info']['total_records']}")
        print(f"\nРезультирующие файлы:")
        print(f"  JSON: {json_path}")
        print(f"  Excel: {excel_path}")
        print(f"  CSV: {csv_path}")
        
        # Создаем файл README с описанием
        readme_content = f"""# Результаты обработки файла аптек

## Исходные данные
- Файл: {os.path.basename(file_path)}
- Дата обработки: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- Обработано листов: {result['file_info']['processed_sheets']}
- Всего записей: {result['file_info']['total_records']}

## Созданные файлы
1. {json_path} - полные данные в формате JSON
2. {excel_path} - данные с разбивкой по листам
3. {csv_path} - объединенные данные в формате CSV

## Структура данных
Основные колонки:
- inn: ИНН аптеки
- legal_entity_name: Наименование юридического лица
- brand: Бренд/сеть аптек
- type: Тип (прямой/непрямой)
- sheet_name: Исходный лист Excel
- start_date/end_date: Период данных

## Статистика
"""
        
        # Добавляем статистику по брендам
        if 'brand' in result['combined_dataframe'].columns:
            brand_stats = result['combined_dataframe']['brand'].value_counts()
            readme_content += "\nРаспределение по брендам:\n"
            for brand, count in brand_stats.items():
                percentage = (count / len(result['combined_dataframe'])) * 100
                readme_content += f"- {brand}: {count} записей ({percentage:.1f}%)\n"
        
        # Сохраняем README
        readme_path = f"README_{base_name}_{timestamp}.txt"
        with open(readme_path, 'w', encoding='utf-8') as f:
            f.write(readme_content)
        
        print(f"\n✓ Создан файл с описанием: {readme_path}")

if __name__ == "__main__":
    main()