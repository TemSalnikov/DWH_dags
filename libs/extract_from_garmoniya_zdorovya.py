import pandas as pd
import uuid
import numpy as np
import os
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin
from dateutil.relativedelta import relativedelta

def extract_xls(path='', name_report='Закуп_Остатки_Продажи', name_pharm_chain='Гармония здоровья') -> dict:
    loger = LoggingMixin().log
    
    try:
        filename = os.path.basename(path)
        date_part = os.path.splitext(filename)[0]
        month, year = map(int, date_part.split('_'))
        
        start_date = datetime(year, month, 1)
        end_date = start_date + relativedelta(months=1) - relativedelta(days=1)
        
        loger.info(f'Определен период отчета по имени файла: {start_date.date()} - {end_date.date()}')

        xls = pd.ExcelFile(path)
        sheet_names = xls.sheet_names
        
        all_sheets_data = []

        rename_map = {
            'поставщик': 'distributor',
            'инн поставщика': 'distributor_inn',
            'интернет-заказ': 'internet_order',
            'подразделение': 'subdivision',
            'наименование подразделения': 'subdivision',
            'адрес аптеки': 'address',
            'юрлицо': 'legal_entity',
            'инн': 'legal_entity_inn',
            'наименование товара': 'product',
            'код товара': 'product_code',
            'количество': 'quantity',
            'сумма количество': 'quantity',
            'сумма сумзак': 'sum_purchase',
            'сумзак': 'sum_purchase',
            'сумма сумма производителя': 'sum_manufacturer',
            'сумма производителя': 'sum_manufacturer',
            'сумма сумндс': 'sum_vat',
            'сумндс': 'sum_vat',
            'дата документа поставщика': 'doc_date',
            'номер документа поставщика': 'doc_number',
            'канал продаж': 'sales_channel',
            'группа в договоре': 'contract_group',
            'срок годности': 'expiration_date'
        }

        expected_sheets = ['закуп', 'остатки', 'продажи']

        for sheet_name in sheet_names:
            if sheet_name.lower() in expected_sheets:
                loger.info(f"Читаем лист: {sheet_name}")
                df_sheet_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)

                header_row_index = -1
                header_keywords = ['поставщик', 'подразделение', 'наименование товара']
                for i, row in df_sheet_raw.iterrows():
                    row_values = {str(cell).lower().strip() for cell in row if pd.notna(cell)}
                    if any(keyword in row_values for keyword in header_keywords):
                        header_row_index = i
                        break
                
                if header_row_index == -1:
                    raise ValueError(f"Не удалось найти строку с заголовками на листе '{sheet_name}'. Ожидалось одно из: {header_keywords}")

                headers = df_sheet_raw.iloc[header_row_index]
                df_sheet = df_sheet_raw.iloc[header_row_index + 1:].copy()
                df_sheet.columns = headers

                df_sheet.columns = [str(col).lower().strip() for col in df_sheet.columns]
                df_sheet.rename(columns=rename_map, inplace=True)
                df_sheet['name_report'] = sheet_name.lower()
                all_sheets_data.append(df_sheet)

        if not all_sheets_data:
            raise ValueError("В файле не найдены листы 'Закуп', 'Остатки' или 'Продажи'.")

        df = pd.concat(all_sheets_data, ignore_index=True, sort=False)
        loger.info(f'Успешно объединено {len(df)} строк со всех листов.')
        if 'quantity' in df.columns and isinstance(df['quantity'], pd.DataFrame):
            loger.info("Обнаружены дублирующиеся колонки 'quantity', объединяем их.")
            df['quantity'] = df['quantity'].apply(lambda x: x.dropna().iloc[0] if not x.dropna().empty else np.nan, axis=1)

        df = df.where(pd.notna(df), None)

        # Технические поля
        count_rows = len(df)
        df['uuid_report'] = [str(uuid.uuid4()) for _ in range(count_rows)]
        df['name_pharm_chain'] = name_pharm_chain
        df['start_date'] = str(start_date.date())
        df['end_date'] = str(end_date.date())
        df['processed_dttm'] = str(datetime.now())

        final_column_order = [
            'uuid_report','distributor', 'distributor_inn', 'internet_order', 'subdivision',
            'address', 'legal_entity', 'legal_entity_inn', 'product', 'product_code',
            'quantity', 'sum_purchase', 'sum_manufacturer', 'sum_vat', 'doc_date', 
            'doc_number', 'sales_channel', 'contract_group', 'expiration_date',
            'name_pharm_chain', 'name_report',
            'start_date', 'end_date', 'processed_dttm',
        ]
        
        for col in final_column_order:
            if col not in df.columns:
                df[col] = None
        
        df_report = df[final_column_order].copy()
        
        for date_col in ['doc_date', 'expiration_date']:
            if date_col in df_report.columns and not pd.isna(df_report[date_col]).all():
                df_report[date_col] = pd.to_datetime(df_report[date_col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

        # Финальная замена NaN на None, чтобы избежать проблем с сериализацией в JSON для Kafka
        df_report = df_report.replace({np.nan: None})

        return {
            'table_report': df_report
        }

    except Exception as e:
        loger.error(f'ERROR parsing file {path}: {str(e)}', exc_info=True)
        raise

if __name__ == "__main__":
    main_loger = LoggingMixin().log

    test_file_path = r'C:\Users\nmankov\Desktop\отчеты\Гармония здоровья\Закупки, продажи, остатки\2023\05_2023.xlsx'

    if os.path.exists(test_file_path):
        main_loger.info(f"Запуск локального теста для файла: {test_file_path}")
        try:
            result_data = extract_xls(path=test_file_path)
            df_result = result_data.get('table_report')

            if df_result is not None and not df_result.empty:
                base_filename = os.path.splitext(os.path.basename(test_file_path))[0]
                output_path = os.path.join(os.path.dirname(test_file_path), f"{base_filename}_result.csv")
                df_result.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
                main_loger.info(f"Результат успешно сохранен в файл: {output_path}")
            else:
                main_loger.warning("Парсинг завершился, но итоговый датафрейм пуст.")

        except Exception as e:
            main_loger.error(f"Во время локального теста произошла ошибка: {e}", exc_info=True)
    else:
        main_loger.warning(f"Тестовый файл не найден по пути: {test_file_path}")