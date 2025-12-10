import pandas as pd
import uuid
import os
import calendar
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

def _get_dates_from_filename(path: str, loger) -> tuple[datetime, datetime]:
    """
    Извлекает начальную и конечную дату из имени файла формата 'ММ_ГGГГ.xlsx'.
    """
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

def extract_xls(path: str, name_report: str, name_pharm_chain: str = 'Моя аптека НСК') -> dict:
    """
    Парсер для отчетов 'Моя аптека НСК'.
    Обрабатывает листы 'закуп' и 'продажи' в одном файле.
    """
    loger = LoggingMixin().log
    loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")

    try:
        start_date, end_date = _get_dates_from_filename(path, loger)
        xls = pd.ExcelFile(path)
        sheet_names = xls.sheet_names

        all_sheets_data = []

        rename_map = {
            'юр лицо': 'legal_entity',
            'аптека.инн': 'pharmacy_inn',
            'аптека': 'pharmacy_name',
            'товар название': 'product_name',
            'остатки': 'stock_quantity',
            'продажа': 'sale_quantity',
            'поставщик': 'supplier',
            'штрих-код производителя': 'manufacturer_barcode',
            'приход кол-во товара': 'purchase_quantity',
        }

        expected_sheets = ['продажи', 'закуп']

        for sheet_name in sheet_names:
            sheet_name_lower = sheet_name.lower().strip()
            if sheet_name_lower in expected_sheets:
                loger.info(f"Обработка листа: '{sheet_name}'")
                df_sheet = pd.read_excel(xls, sheet_name=sheet_name, header=0, dtype=str)
                df_sheet.columns = [str(col).lower().strip() for col in df_sheet.columns]
                df_sheet.rename(columns=rename_map, inplace=True)
                all_sheets_data.append(df_sheet)

        if not all_sheets_data:
            raise ValueError(f"В файле не найдены ожидаемые листы: {expected_sheets}")

        df = pd.concat(all_sheets_data, ignore_index=True, sort=False)
        loger.info(f"Успешно объединено {len(df)} строк со всех листов.")

        df['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df['name_report'] = name_report
        df['name_pharm_chain'] = name_pharm_chain
        df['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        final_columns = [
            'uuid_report', 'legal_entity', 'pharmacy_inn', 'pharmacy_name',
            'product_name', 'supplier', 'manufacturer_barcode',
            'purchase_quantity', 'sale_quantity', 'stock_quantity',
            'name_report', 'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm'
        ]

        for col in final_columns:
            if col not in df.columns:
                df[col] = None

        df_report = df[final_columns]
        df_report = df_report.where(pd.notna(df_report), None)

        loger.info(f"Парсинг отчета '{name_report}' успешно завершен. Итого строк: {len(df_report)}")
        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    test_file_path = r'C:\Users\nmankov\Desktop\отчеты\Моя Аптека НСК\Остатки+Закуп+Продажи\2025\01_2025.xlsx'    
    if os.path.exists(test_file_path):
        main_loger.info(f"Запуск локального теста для файла: {test_file_path}")
        try:
            result = extract_xls(path=test_file_path, name_report='Остатки+Закуп+Продажи')
            df = result.get('table_report')
            if df is not None and not df.empty:
                output_filename = f"{os.path.splitext(os.path.basename(test_file_path))[0]}_result.csv"
                output_path = os.path.join(os.path.dirname(test_file_path), output_filename)
                df.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
                main_loger.info(f"Результат успешно сохранен в: {output_path}")
        except Exception as e:
            main_loger.error(f"Во время локального теста произошла ошибка: {e}", exc_info=True)
    else:
        main_loger.warning(f"Тестовый файл не найден по пути: {test_file_path}. Пожалуйста, создайте его для проверки.")