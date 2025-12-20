import pandas as pd
import uuid
import os
import calendar
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

def _get_dates_from_filename(path: str, loger) -> tuple[datetime, datetime]:
    try:
        filename = os.path.basename(path)
        date_part = os.path.splitext(filename)[0]
        report_date = datetime.strptime(date_part, "%m_%Y")
        start_date = report_date.replace(day=1)
        _, last_day = calendar.monthrange(report_date.year, report_date.month)
        end_date = report_date.replace(day=last_day)
        loger.info(f"Определен период отчета по имени файла: {start_date.date()} - {end_date.date()}")
        return start_date, end_date
    except Exception as e:
        loger.error(f"Не удалось определить дату из имени файла '{os.path.basename(path)}'. Ожидаемый формат: ММ_ГГГГ.xlsx. Ошибка: {e}")
        raise

def extract_nadezhda_farm(path='', name_report='Закупки', name_pharm_chain='Надежда Фарм') -> dict:
    """
    Парсер для отчетов 'Надежда Фарм'.
    """
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")

        start_date, end_date = _get_dates_from_filename(path, loger)

        xls = pd.ExcelFile(path)
        sheet_names = xls.sheet_names
        
        target_sheet = None
        if len(sheet_names) == 1:
            target_sheet = sheet_names[0]
        else:
            name_report_lower = name_report.lower()
            if 'закуп' in name_report_lower:
                target_sheet = next((s for s in sheet_names if s.strip().lower() == 'закупки'), None)
            elif 'продаж' in name_report_lower:
                target_sheet = next((s for s in sheet_names if s.strip().lower() == 'продажи'), None)
            elif 'остат' in name_report_lower:
                target_sheet = next((s for s in sheet_names if s.strip().lower() == 'остатки'), None)
        
        if not target_sheet:
             raise ValueError(f"Не найден подходящий лист для отчета '{name_report}' среди листов: {sheet_names}")

        loger.info(f"Выбран лист для парсинга: '{target_sheet}'")
        df = pd.read_excel(xls, sheet_name=target_sheet, dtype=str)
        
        df.columns = [str(col).strip() for col in df.columns]

        rename_map = {
            'МЕСЯЦ': 'month', 'Месяц': 'month',
            'Поставщик': 'supplier',
            'Название товара': 'product_name',
            'Покупатель': 'buyer',
            'ИНН': 'inn',
            'Кол-во': 'quantity',
            'ФИЛИАЛ': 'branch',
            'Аптека': 'pharmacy_name',
            'Регион аптеки': 'region'
        }
        
        df.rename(columns=rename_map, inplace=True)

        # Технические поля
        df['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df['name_report'] = name_report
        df['name_pharm_chain'] = name_pharm_chain
        df['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Унификация структуры
        final_columns = [
            'uuid_report', 'month', 'supplier', 'product_name', 'buyer', 'inn', 
            'quantity', 'branch', 'pharmacy_name', 'region',
            'name_report', 'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm'
        ]

        for col in final_columns:
            if col not in df.columns:
                df[col] = None
        
        df_report = df[final_columns]
        
        # Замена NaN на None
        df_report = df_report.where(pd.notna(df_report), None)

        loger.info(f"Парсинг Надежда Фарм успешно завершен. Строк: {len(df_report)}")
        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    test_file_path = r'C:\Users\nmankov\Desktop\отчеты\Надежда Фарм\Закуп\2023\12_2023.xlsx'
    test_report_type = 'Закупки'
    
    if os.path.exists(test_file_path):
        main_loger.info(f"Запуск локального теста для файла: {test_file_path}")
        try:
            result_data = extract_nadezhda_farm(path=test_file_path, name_report=test_report_type, name_pharm_chain='Надежда Фарм')
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