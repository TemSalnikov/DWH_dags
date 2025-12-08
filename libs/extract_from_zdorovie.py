import pandas as pd
import uuid
import os
import calendar
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin

# Общий список колонок для унификации структуры всех отчетов
FINAL_COLUMNS = [
    'uuid_report',
    # Поля из отчетов
    'product_name',
    'quantity',
    'supplier',
    'invoice_number',
    'invoice_date',
    'pharmacy_name',
    'legal_entity',
    'brand',
    'city_type',
    'city',
    'address',
    'inn',
    'category_ntz',
    'category_display',
    # Технические поля
    'name_report',
    'name_pharm_chain',
    'start_date',
    'end_date',
    'processed_dttm'
]

def _extract_custom(path='', name_report='Закупки', name_pharm_chain='Здоровье') -> dict:
    """
    Парсер для отчета 'Закупки' от 'Здоровье'.
    """
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")

        xls = pd.ExcelFile(path)
        sheet_name_purchases = None
        for sheet in xls.sheet_names:
            if 'закуп' in sheet.lower():
                sheet_name_purchases = sheet
                break

        if not sheet_name_purchases:
            raise ValueError("В файле не найден лист с закупками (не найдено 'закуп' в названии).")

        loger.info(f"Найден лист с закупками: '{sheet_name_purchases}'")

        # Динамический поиск строки с заголовком
        df_raw = pd.read_excel(xls, sheet_name=sheet_name_purchases, header=None, dtype=str)
        header_row_index = -1
        for i, row in df_raw.iterrows():
            if 'наименование товара' in row.astype(str).str.lower().values:
                header_row_index = i
                break
        if header_row_index == -1:
            raise ValueError("Не удалось найти строку с заголовками в отчете по закупкам.")
        df = pd.read_excel(xls, sheet_name=sheet_name_purchases, header=header_row_index, dtype=str)
        df.dropna(how='all', inplace=True)
        loger.info(f"Успешно прочитано {len(df)} строк из листа.")

        # Переименование колонок
        rename_map = {
            'Наименование товара': 'product_name',
            '№ накладной': 'invoice_number',
            'Дата накладной': 'invoice_date',
            'Поставщик': 'supplier',
            'Количество': 'quantity'
        }
        df.rename(columns=rename_map, inplace=True)

        # Определение периода отчета по имени файла
        try:
            filename = os.path.basename(path)
            date_part = os.path.splitext(filename)[0]
            report_date = datetime.strptime(date_part, "%m_%Y")
            start_date = report_date.replace(day=1)
            _, last_day = calendar.monthrange(report_date.year, report_date.month)
            end_date = report_date.replace(day=last_day)
            loger.info(f"Определен период отчета по имени файла: {start_date.date()} - {end_date.date()}")
        except ValueError:
            raise ValueError(f"Не удалось определить дату из имени файла '{os.path.basename(path)}'. Ожидаемый формат: ММ_ГГГГ.xlsx")


        # Добавление технических полей
        df['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df['name_report'] = name_report
        df['name_pharm_chain'] = name_pharm_chain
        df['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        df_report = df.astype(str).where(pd.notna(df), None)

        if 'invoice_date' in df_report.columns:
            invoice_dates = pd.to_datetime(df_report['invoice_date'], dayfirst=True, errors='coerce')
            df_report['invoice_date'] = invoice_dates.dt.strftime('%Y-%m-%d %H:%M:%S').where(invoice_dates.notna(), None)
        
        for col in FINAL_COLUMNS:
            if col not in df_report.columns:
                df_report[col] = None
        
        df_report = df_report[FINAL_COLUMNS]
        
        return {'table_report': df_report}
    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def _extract_sales_or_remains(path='', name_report='Продажи', name_pharm_chain='Здоровье', xls=None) -> dict:
    """
    Универсальный парсер для отчетов 'Продажи' и 'Остатки' от 'Здоровье'.
    """
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")

        try:
            filename = os.path.basename(path)
            date_part = os.path.splitext(filename)[0]
            report_date = datetime.strptime(date_part, "%m_%Y")
            start_date = report_date.replace(day=1)
            _, last_day = calendar.monthrange(report_date.year, report_date.month)
            end_date = report_date.replace(day=last_day)
            loger.info(f"Определен период отчета по имени файла: {start_date.date()} - {end_date.date()}")
        except ValueError:
            raise ValueError(f"Не удалось определить дату из имени файла '{os.path.basename(path)}'. Ожидаемый формат: ММ_ГГГГ.xlsx")

        target_sheet_name = None

        search_keyword = ''
        if 'продажи' in name_report.lower():
            search_keyword = 'продаж'
        elif 'остатки' in name_report.lower():
            search_keyword = 'остат'

        for sheet in xls.sheet_names:
            if search_keyword and search_keyword in sheet.lower():
                target_sheet_name = sheet
                break
        
        if not target_sheet_name:
            raise ValueError(f"В файле не найден лист для отчета '{name_report}'. Ожидался лист с '{search_keyword}' в названии.")

        loger.info(f"Найден лист '{target_sheet_name}' для отчета '{name_report}'.")

        df_directory = pd.DataFrame()
        pharmacy_directory_sheet = next((sheet for sheet in xls.sheet_names if 'аптек' in sheet.lower()), None)
        if pharmacy_directory_sheet:
            loger.info(f"Найден лист-справочник аптек: '{pharmacy_directory_sheet}'")
            df_directory = pd.read_excel(xls, sheet_name=pharmacy_directory_sheet, dtype=str)
            dir_rename_map = {
                'Аптека, №': 'pharmacy_name',
                'Бренд': 'brand',
                'Тип НП': 'city_type',
                'Населенный пункт': 'city',
                'Адрес': 'address',
                'Юр лицо': 'legal_entity',
                'ИНН': 'inn',
                'Категория (для НТЗ)': 'category_ntz',
                'Категория (для выкладки)': 'category_display'
            }
            df_directory.rename(columns={k: v for k, v in dir_rename_map.items() if k in df_directory.columns}, inplace=True)
        else:
            loger.info("Лист-справочник аптек не найден, будут использованы данные из основного отчета.")


        df_headers = pd.read_excel(xls, sheet_name=target_sheet_name, header=None, nrows=2, dtype=str)
        pharmacy_names = df_headers.iloc[0, 1:].fillna('').astype(str)
        legal_entities = df_headers.iloc[1, 1:].fillna('').astype(str)

        df_raw = pd.read_excel(xls, sheet_name=target_sheet_name, header=None, dtype=str)
        header_row_index = -1
        # Ищем первую строку, где есть слово "аптека" в любой колонке, кроме первой.
        # Это будет строка с названиями аптек.
        for i, row in df_raw.iterrows():
            if any('аптека' in str(cell).lower() for cell in row[1:]):
                header_row_index = i
                break
        
        if header_row_index == -1:
            raise ValueError("Не удалось найти строку с заголовками аптек (по ключевому слову 'аптека').")
        
        loger.info(f"Строка с названиями аптек найдена по индексу: {header_row_index}")

        # Считываем заголовки аптек и юрлиц с найденной строки
        pharmacy_names = df_raw.iloc[header_row_index, 1:].fillna('').astype(str)
        legal_entities = df_raw.iloc[header_row_index + 1, 1:].fillna('').astype(str)

        # Данные начинаются на 2 строки ниже строки с названиями аптек
        data_start_row = header_row_index + 2
        df = df_raw.iloc[data_start_row:].copy()
        df.reset_index(drop=True, inplace=True)
        # Устанавливаем названия колонок вручную, используя первую строку данных как шаблон
        df.columns = df_raw.iloc[data_start_row - 1].values

        # Удаляем последнюю строку с итогами, если она есть
        if not df.empty and 'итог' in str(df.iloc[-1, 0]).lower():
            df.drop(df.tail(1).index, inplace=True)
            loger.info("Удалена последняя строка (предположительно, 'Итог').")
        
        df.columns = [df.columns[0]] + pharmacy_names.tolist()
        df.rename(columns={df.columns[0]: 'product_name'}, inplace=True)

        # Проверка и удаление безымянной колонки после присвоения имен
        if len(df.columns) > 1 and 'unnamed' in str(df.columns[1]).lower():
            unnamed_col_name = df.columns[1]
            # Находим индекс колонки для удаления из списков заголовков
            unnamed_col_index = df.columns.get_loc(unnamed_col_name)
            df.drop(columns=[unnamed_col_name], inplace=True)
            pharmacy_names = pharmacy_names.drop(pharmacy_names.index[unnamed_col_index - 1])
            legal_entities = legal_entities.drop(legal_entities.index[unnamed_col_index - 1])
            loger.info(f"Обнаружена и удалена безымянная колонка: '{unnamed_col_name}' и соответствующий ей заголовок.")

        total_col = next((col for col in df.columns if 'итог' in str(col).lower()), None)
        if total_col:
            df.drop(columns=[total_col], inplace=True)
            loger.info(f"Колонка '{total_col}' удалена.")

        id_vars = ['product_name']
        value_vars = [col for col in df.columns if col not in id_vars]
        df_melted = df.melt(id_vars=id_vars, value_vars=value_vars, var_name='pharmacy_name', value_name='quantity')

        # Удаляем строки, где не удалось определить название аптеки (пустое значение)
        df_melted.dropna(subset=['pharmacy_name'], inplace=True)
        df_melted = df_melted[df_melted['pharmacy_name'].astype(str).str.strip() != '']

        df_melted.dropna(subset=['quantity'], inplace=True)
        df_melted = df_melted[pd.to_numeric(df_melted['quantity'], errors='coerce').notna()]
        df_melted.reset_index(drop=True, inplace=True)

        pharmacy_to_legal_entity_map = dict(zip(pharmacy_names, legal_entities))
        df_melted['legal_entity'] = df_melted['pharmacy_name'].map(pharmacy_to_legal_entity_map)

        if not df_directory.empty:
            original_cols = df_melted.columns.tolist()
            df_melted = pd.merge(df_melted, df_directory, on='pharmacy_name', how='left', suffixes=('', '_dir'))
            if 'legal_entity_dir' in df_melted.columns:
                df_melted['legal_entity'] = df_melted['legal_entity_dir'].where(df_melted['legal_entity_dir'].notna(), df_melted['legal_entity'])
                df_melted.drop(columns=['legal_entity_dir'], inplace=True)
            loger.info("Данные обогащены информацией из листа-справочника.")

        loger.info(f"Успешно получено {len(df_melted)} строк после преобразования.")

        # Добавление технических полей
        df_melted['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_melted))]
        df_melted['name_report'] = name_report
        df_melted['name_pharm_chain'] = name_pharm_chain
        df_melted['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df_melted['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df_melted['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Гарантируем наличие всех колонок из общего списка
        for col in FINAL_COLUMNS:
            if col not in df_melted.columns:
                df_melted[col] = None

        df_report = df_melted[FINAL_COLUMNS]
        df_report = df_report.astype(str).where(pd.notna(df_report), None)

        return {'table_report': df_report}
    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_xls(path, name_report, name_pharm_chain) -> dict:
    """
    Универсальный парсер, который обрабатывает все листы (закупки, продажи, остатки)
    в одном файле Excel и объединяет их в один отчет.
    """
    loger = LoggingMixin().log
    loger.info(f"Начинаем полный парсинг файла '{path}' для '{name_pharm_chain}'")

    try:
        xls = pd.ExcelFile(path)
        sheet_names = xls.sheet_names
        all_reports_df = []

        try:
            filename = os.path.basename(path)
            date_part = os.path.splitext(filename)[0]
            report_date = datetime.strptime(date_part, "%m_%Y")
            start_date = report_date.replace(day=1)
            _, last_day = calendar.monthrange(report_date.year, report_date.month)
            end_date = report_date.replace(day=last_day)
            loger.info(f"Определен общий период отчета по имени файла: {start_date.date()} - {end_date.date()}")
        except ValueError:
            raise ValueError(f"Не удалось определить дату из имени файла '{os.path.basename(path)}'. Ожидаемый формат: ММ_ГГГГ.xlsx")

        df_directory = pd.DataFrame()
        pharmacy_directory_sheet = next((sheet for sheet in sheet_names if 'аптек' in sheet.lower()), None)
        if pharmacy_directory_sheet:
            loger.info(f"Найден лист-справочник аптек: '{pharmacy_directory_sheet}'")
            df_directory_raw = pd.read_excel(xls, sheet_name=pharmacy_directory_sheet, dtype=str)
            dir_rename_map = {
                'Аптека, №': 'pharmacy_name', 'Бренд': 'brand', 'Тип НП': 'city_type',
                'Населенный пункт': 'city', 'Адрес': 'address', 'Юр лицо': 'legal_entity',
                'ИНН': 'inn', 'Категория (для НТЗ)': 'category_ntz',
                'Категория (для выкладки)': 'category_display'
            }
            df_directory_raw.rename(columns={k: v for k, v in dir_rename_map.items() if k in df_directory_raw.columns}, inplace=True)
            df_directory = df_directory_raw
        else:
            loger.info("Лист-справочник аптек не найден.")

        for sheet_name in sheet_names:
            current_report_type = None
            df_sheet = None

            if 'закуп' in sheet_name.lower():
                current_report_type = 'Закупки'
                temp_result = _extract_custom(path=path, name_report=current_report_type, name_pharm_chain=name_pharm_chain)
                df_sheet = temp_result.get('table_report')

            elif 'продаж' in sheet_name.lower() or 'остат' in sheet_name.lower():
                current_report_type = 'Продажи' if 'продаж' in sheet_name.lower() else 'Остатки'
                temp_result = _extract_sales_or_remains(path=path, name_report=current_report_type, name_pharm_chain=name_pharm_chain, xls=xls)
                df_sheet = temp_result.get('table_report')

            if df_sheet is not None and not df_sheet.empty:
                loger.info(f"Лист '{sheet_name}' успешно обработан как '{current_report_type}'. Получено {len(df_sheet)} строк.")
                df_sheet['name_report'] = current_report_type
                all_reports_df.append(df_sheet)

        if not all_reports_df:
            raise ValueError("В файле не найдено ни одного листа для обработки (закупки, продажи, остатки).")

        final_df = pd.concat(all_reports_df, ignore_index=True, sort=False)
        loger.info(f"Все листы объединены. Итоговое количество строк: {len(final_df)}")

        for col in FINAL_COLUMNS:
            if col not in final_df.columns:
                final_df[col] = None

        df_report = final_df[FINAL_COLUMNS]
        df_report = df_report.astype(str).where(pd.notna(df_report), None)

        if 'invoice_date' in df_report.columns:
            invoice_dates = pd.to_datetime(df_report['invoice_date'], errors='coerce')
            df_report['invoice_date'] = invoice_dates.dt.strftime('%Y-%m-%d %H:%M:%S').where(invoice_dates.notna(), None)

        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при полном парсинге файла '{path}': {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    import warnings
    warnings.filterwarnings("ignore", category=RuntimeWarning, module='airflow')

    test_file_path = r'C:\Users\nmankov\Desktop\отчеты\Здоровье\Закуп_Продажи_Остатки\2024\08_2024.xlsx'
    report_type_to_test = 'Закуп_Продажи_Остатки'

    if os.path.exists(test_file_path):
        main_loger.info(f"Запуск локального теста для отчета '{report_type_to_test}' из файла: {test_file_path}")
        try:
            result_data = extract_xls(path=test_file_path, name_report=report_type_to_test, name_pharm_chain='Здоровье')
            df_result = result_data.get('table_report')

            if df_result is not None and not df_result.empty:
                output_filename = f"zdorovie_full_report_test_result.csv"
                output_path = os.path.join(os.path.dirname(test_file_path), output_filename)
                df_result.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
                main_loger.info(f"Результат успешно сохранен в файл: {output_path}")
            else:
                main_loger.warning("Парсинг завершен, но итоговый датафрейм пуст.")
        except Exception as e:
            main_loger.error(f"Во время локального теста произошла ошибка: {e}", exc_info=True)
    else:
        main_loger.warning(f"Тестовый файл не найден по пути: {test_file_path}")