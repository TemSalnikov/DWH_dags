from datetime import datetime, timedelta
import uuid
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
import hashlib
import os
import re


def extract_custom(path='', name_report='Закупки', name_pharm_chain='Вита Томск') -> dict:

    loger = LoggingMixin().log
    try:
        xls = pd.ExcelFile(path)
        target_sheet = xls.sheet_names[0]
        if len(xls.sheet_names) > 1 and 'TDSheet' in xls.sheet_names:
            target_sheet = 'TDSheet'

        start_date = None
        end_date = None
        
        try:
            df_for_date = pd.read_excel(xls, sheet_name=target_sheet, header=None, nrows=2)
            if not df_for_date.empty and df_for_date.shape[0] > 1:
                date_val = df_for_date.iloc[1, 0]
                if pd.notna(date_val):
                    try:
                        report_date_from_cell = pd.to_datetime('1899-12-30') + pd.to_timedelta(int(date_val), 'D')
                    except (ValueError, TypeError):
                        report_date_from_cell = datetime.strptime(str(date_val), "%d.%m.%Y")
                    start_date = report_date_from_cell.replace(day=1)
                    end_date = start_date + pd.offsets.MonthEnd(1)
                    loger.info(f"Определен период отчета из ячейки: {start_date.date()} - {end_date.date()}")
        except Exception as e:
            loger.warning(f"Не удалось определить дату из ячейки: {e}")

        if start_date is None:
            filename = os.path.basename(path)
            match = re.search(r'(\d{2})_(\d{4})', filename)
            if match:
                report_date = datetime.strptime(f"{match.group(1)}_{match.group(2)}", "%m_%Y")
            else:
                date_part = os.path.splitext(filename)[0]
                report_date = datetime.strptime(date_part, "%m_%Y")
            
            start_date = report_date.replace(day=1)
            end_date = start_date + pd.offsets.MonthEnd(1)
            loger.info(f"Определен период отчета по имени файла: {start_date.date()} - {end_date.date()}")

        df = pd.read_excel(xls, sheet_name=target_sheet, header=0, dtype=str)
        df.dropna(how='all', inplace=True)
        loger.info(f'Успешно получено {len(df)} строк!')
        rename_map = {
            'Период отчета': 'report_period',
            'ЕКН': 'ekn', 
            'Товар': 'product',
            'Закуп шт.': 'quantity',
            'Закуп сумма с НДС': 'purchase_amount_with_vat',
            'Продажи шт.': 'sales_quantity',
            'Остаток на конец периода шт.': 'balance_end_period_quantity',
            'Текущий остаток шт.': 'current_balance_quantity',
            'ИНН Юр.лица': 'legal_entity_inn',
            'Юр.лицо': 'legal_entity',
            'Населенный пункт': 'settlement',
            'Адрес аптеки': 'pharmacy_address',
            'ИНН Поставщика': 'supplier_inn',
            'Поставщик': 'supplier',
            'Заводской ШК': 'factory_barcode',
            'Дата прихода': 'receipt_date',
            'Номер накладной': 'invoice_number',
            'Срок годности': 'expiration_date'
        }
        df.rename(columns=rename_map, inplace=True)
        df_report = df.copy()

        missing_cols_from_sales = [
            'product_amount', 'point_code', 'ofd_name', 'fn', 'fd', 'fpd',
            'receipt_amount', 'kkt'
        ]
        for col in missing_cols_from_sales:
            df_report[col] = None


        df_report['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_report))]
        df_report['name_report'] = name_report
        df_report['name_pharm_chain'] = name_pharm_chain
        df_report['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df_report['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df_report['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        final_columns = [
            'uuid_report', 'ekn', 'product', 'quantity', 'purchase_amount_with_vat',
            'sales_quantity', 'balance_end_period_quantity', 'current_balance_quantity',
            'legal_entity_inn', 'legal_entity', 'settlement', 'pharmacy_address',
            'supplier_inn', 'supplier', 'factory_barcode', 'receipt_date',
            'invoice_number', 'expiration_date', 'product_amount', 'point_code',
            'ofd_name', 'fn', 'fd', 'fpd', 'receipt_amount', 'kkt', 'name_report',
            'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm'
        ]

        df_report = df_report[final_columns]
        
        for col in df_report.columns:
            if df_report[col].dtype == 'object':
                df_report[col] = df_report[col].replace(['', 'nan', 'None', '<NA>', 'NaT'], None)

        return {
            "table_report": df_report
        }
    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

def extract_sale(path='', name_report='Продажи', name_pharm_chain='Вита Томск') -> dict:
    loger = LoggingMixin().log
    try:
        try:
            filename = os.path.basename(path)
            date_part = os.path.splitext(filename)[0]
            report_date = datetime.strptime(date_part, "%m_%Y")
            start_date = report_date # Начало месяца
            end_date = start_date + pd.offsets.MonthEnd(1) # Конец месяца
            loger.info(f"Определен период отчета по имени файла: {start_date.date()} - {end_date.date()}")
        except ValueError:
            loger.warning("Не удалось определить дату из имени файла")

        df = pd.read_excel(path, header=0, dtype=str)
        loger.info("Файл прочитан, первая строка используется как заголовок.")

        df.dropna(how='all', inplace=True)
        loger.info(f'Успешно получено {len(df)} строк!')

        rename_map = {
            # Сопоставление с форматом отчета "Закупки"
            "Код товара": "ekn",
            "Название товара": "product",
            "ИНН": "legal_entity_inn",
            
            # Остальные поля
            "Дата чека": "receipt_date",
            "Сумма товара": "product_amount",
            "Количество": "quantity",
            "Заводской ШК": "factory_barcode",
            "Код точки": "point_code",
            "Наименование ОФД": "ofd_name",
            "FN": "fn",
            "FD": "fd",
            "FPD": "fpd",
            "Сумма чека": "receipt_amount",
            "KKT": "kkt"
        }
        df.rename(columns=rename_map, inplace=True)
        df_report = df.copy()

        missing_cols = [
            'purchase_amount_with_vat', 'sales_quantity', 'balance_end_period_quantity',
            'current_balance_quantity', 'legal_entity', 'settlement', 'pharmacy_address',
            'supplier_inn', 'supplier', 'invoice_number', 'expiration_date'
        ]
        for col in missing_cols:
            df_report[col] = None

        df_report['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_report))]
        df_report['name_report'] = name_report
        df_report['name_pharm_chain'] = name_pharm_chain
        df_report['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df_report['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df_report['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Определяем финальный ОБЩИЙ порядок колонок
        final_columns = [
            'uuid_report', 'ekn', 'product', 'quantity', 'purchase_amount_with_vat',
            'sales_quantity', 'balance_end_period_quantity', 'current_balance_quantity',
            'legal_entity_inn', 'legal_entity', 'settlement', 'pharmacy_address',
            'supplier_inn', 'supplier', 'factory_barcode', 'receipt_date',
            'invoice_number', 'expiration_date', 'product_amount', 'point_code',
            'ofd_name', 'fn', 'fd', 'fpd', 'receipt_amount', 'kkt', 'name_report',
            'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm'
        ]
        df_report = df_report[final_columns]

        for col in df_report.columns:
            if df_report[col].dtype == 'object':
                df_report[col] = df_report[col].replace(['', 'nan', 'None', '<NA>', 'NaT'], None)

        return {"table_report": df_report}
    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise


def extract_xls(path, name_report, name_pharm_chain) -> dict:
    if name_report == 'Закупки':
        return extract_custom(path, name_report, name_pharm_chain)
    if name_report == 'Продажи':
        return extract_sale(path, name_report, name_pharm_chain)
    return {}


if __name__ == "__main__":
    main_loger = LoggingMixin().log
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты_аптек\Вита Томск\Закуп\2025\05_2025.xlsx'
    report_type_to_test = 'Закупки'
    pharm_chain_name = 'Вита Томск'

    if os.path.exists(test_file_path):
        main_loger.info(f"Запуск локального теста для '{pharm_chain_name}' с отчетом '{report_type_to_test}'")
        try:
            result = extract_xls(path=test_file_path, name_report=report_type_to_test, name_pharm_chain=pharm_chain_name)
            
            df_result = result.get('table_report')

            if df_result is not None and not df_result.empty:
                output_dir = os.path.dirname(test_file_path)

                base_filename = os.path.splitext(os.path.basename(test_file_path))[0]
                output_path = os.path.join(output_dir, f"{base_filename}_result.csv")

                df_result.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
                main_loger.info(f"Результат успешно сохранен в: {output_path}")
        except Exception as e:
            main_loger.error(f"Во время теста произошла ошибка: {e}", exc_info=True)
    else:
        main_loger.warning(f"Тестовый файл не найден: {test_file_path}")