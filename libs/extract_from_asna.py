import pandas as pd
import uuid
import os
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
from dateutil.relativedelta import relativedelta

def extract_xls(path='', name_report='Закуп_Продажи', name_pharm_chain='Асна') -> dict:
    """
    Парсер для отчетов "Закупки и Продажи" от аптечной сети "Асна".
    Ожидает excel-файл с листами 'закупки' и 'продажи'.
    """
    loger = LoggingMixin().log
    
    try:
        xls = pd.ExcelFile(path)
        sheet_names = xls.sheet_names
        
        processed_sheets = []

        rename_config = {
            'legal_entity': ['юр лицо (тек)'],
            'inn': ['инн сети - аптеки (тек)'],
            'product': ['товар'],
            'quantity': ['закупки шт', 'продажи шт'],
            'distributor': ['поставщик'],
            'address': ['фактический адрес (тек)'],
            'external_code': ['внешний код (тек)'],
            'pharmacy_name': ['аптека (тек)'],
            'chain_name': ['сеть (тек)'],
            'region': ['регион (тек)'],
            'status': ['статус'],
            'product_code_nnt': ['код товара (ннт)'],
            'purchase_price_cip': ['закупки цена руб cip'],
            'allowed_distributor': ['разрешенный дистрибьютор'],
            'distributor_inn': ['инн поставщика'],
            'os_group': ['группа по ос'],
            'internet_order': ['интернет'],
            'purchase_sum_cip': ['закупки сумма руб cip', 'закупки сумма руб асна-cip'],
            'purchase_sum_no_vat': ['закупки руб без ндс'],
            'sales_price_cip': ['продажи цена руб cip'],
            'sales_sum_cip': ['продажи сумма руб cip', 'продажи сумма руб асна-cip'],
            'sales_sum_no_vat': ['продажи руб без ндс'],
        }

        purchase_keyword = 'закуп'
        sales_keyword = 'продаж'

        for sheet_name in sheet_names:
            sheet_name_lower = sheet_name.lower()
            report_type = None

            if purchase_keyword in sheet_name_lower:
                report_type = 'закупки'
            elif sales_keyword in sheet_name_lower:
                report_type = 'продажи'

            if report_type:
                loger.info(f"Найден лист '{sheet_name}' (тип: {report_type}). Начинаем чтение.")
                
                header_row_index = 0
                found_header = False
                df_preview = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=10, dtype=str)
                
                anchor_columns = ['товар', 'аптека (тек)', 'юр лицо (тек)']
                for i, row in df_preview.iterrows():
                    row_values_lower = [str(v).lower() for v in row.values]
                    if any(anchor in row_values_lower for anchor in anchor_columns):
                        header_row_index = i
                        found_header = True
                        loger.info(f"Заголовки найдены на строке: {header_row_index + 1}")
                        break
                
                if not found_header:
                    loger.warning(f"Ключевые заголовки не найдены в первых 10 строках. Используется строка 1 по умолчанию.")

                df_sheet = pd.read_excel(xls, sheet_name=sheet_name, header=header_row_index, dtype=str)
                df_sheet.columns = [str(col).lower() for col in df_sheet.columns]

                if 'товар.1' in df_sheet.columns and 'товар.2' in df_sheet.columns:
                    loger.info("Обнаружен формат с тремя столбцами 'Товар'. Переименовываем.")
                    df_sheet.rename(columns={
                        'товар.1': 'код товара (ннт)',
                        'товар.2': 'закупки цена руб cip'
                    }, inplace=True)

                df_sheet['name_report'] = report_type
                processed_sheets.append(df_sheet)

        if not processed_sheets:
            raise ValueError("В файле не найдены ожидаемые листы ('Закупки', 'Продажи').")

        flat_rename_map = {source: target for target, sources in rename_config.items() for source in sources}

        for i in range(len(processed_sheets)):
            processed_sheets[i].rename(columns=flat_rename_map, inplace=True)

        df = pd.concat(processed_sheets, ignore_index=True, sort=False)
        loger.info(f'Успешно объединено {len(df)} строк со всех листов.')
        if 'месяц' in df.columns and 'год' in df.columns: 
            loger.info("Определяем период отчета по столбцам 'месяц' и 'год'.")
            month_map = {
                'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4, 'май': 5, 'июнь': 6,
                'июль': 7, 'август': 8, 'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
            }
            
            df['temp_date'] = pd.to_datetime(
                df['год'].astype(str) + '-' + df['месяц'].str.lower().map(month_map).astype(str) + '-01',
                errors='coerce'
            )

            min_date = df['temp_date'].min()
            max_date = df['temp_date'].max()
            
            df.drop(columns=['temp_date'], inplace=True)

            if pd.notna(min_date) and pd.notna(max_date):
                start_date = min_date.replace(day=1)
                end_date = max_date + relativedelta(months=1) - timedelta(days=1)
            else:
                raise ValueError("Не удалось определить даты из столбцов 'месяц' и 'год'.")

        else:
            loger.warning("Столбцы 'месяц' и/или 'год' не найдены. Используются заглушки для дат.")
            start_date = datetime.now() - timedelta(days=1)
            end_date = datetime.now()

        loger.info(f'Определен период отчета по данным: {start_date.date()} - {end_date.date()}')

        df = df.where(pd.notna(df), None)

        count_rows = len(df)
        df['uuid_report'] = [str(uuid.uuid4()) for _ in range(count_rows)]
        df['name_pharm_chain'] = name_pharm_chain
        df['start_date'] = str(start_date.date())
        df['end_date'] = str(end_date.date())
        df['processed_dttm'] = str(datetime.now())

        final_column_order = [
            'uuid_report',
            'legal_entity', 'inn', 'product', 'quantity', 'distributor', 'address',
            'external_code', 'pharmacy_name', 'chain_name', 'region', 'status',
            'product_code_nnt', 'purchase_price_cip', 'allowed_distributor',
            'distributor_inn', 'os_group', 'internet_order', 'purchase_sum_cip',
            'purchase_sum_no_vat',
            'sales_price_cip', 'sales_sum_cip', 'sales_sum_no_vat',
            'name_report', 'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm'
        ]
        loger.info(f"Итоговый набор колонок для отчета: {final_column_order}")
        
        for col in final_column_order:
            if col not in df.columns:
                df[col] = None
        
        df_report = df[final_column_order]
        
        if 'operation_date' in df_report.columns and pd.api.types.is_datetime64_any_dtype(df_report['operation_date']):
             df_report['operation_date'] = df_report['operation_date'].dt.strftime('%Y-%m-%d %H:%M:%S')

        return {
            'table_report': df_report
        }

    except Exception as e:
        loger.error(f'ERROR parsing file {path}: {str(e)}', exc_info=True)
        raise

if __name__ == "__main__":
    main_loger = LoggingMixin().log

    test_file_path = r'C:\Users\nmankov\Desktop\отчеты\АСНА\Закуп Продажи\2023\03_2023.xlsx'

    if os.path.exists(test_file_path):
        main_loger.info(f"Запуск локального теста для файла: {test_file_path}")
        try:
            result_data = extract_xls(path=test_file_path)
            df_result = result_data.get('table_report')

            report_dir = os.path.dirname(test_file_path)
            report_basename = os.path.basename(test_file_path)
            report_name_without_ext = os.path.splitext(report_basename)[0]
            output_filename = f"{report_name_without_ext}_parsed.csv"
            output_path = os.path.join(report_dir, output_filename)

            df_result.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
            main_loger.info(f"Результат успешно сохранен в файл: {output_path}")

        except Exception as e:
            main_loger.error(f"Во время локального теста произошла ошибка: {e}", exc_info=True)
    else:
        main_loger.warning(f"Тестовый файл не найден по пути: {test_file_path}")