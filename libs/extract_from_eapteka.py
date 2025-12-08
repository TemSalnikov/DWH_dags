import pandas as pd
import uuid
import numpy as np
import os
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin
from dateutil.relativedelta import relativedelta

def extract_xls(path='', name_report='Закуп_Остатки_Продажи', name_pharm_chain='Еаптека') -> dict:
    loger = LoggingMixin().log
    loger.info(f"Запуск парсера для '{name_pharm_chain}'")

    try:
        filename = os.path.basename(path)
        date_part = os.path.splitext(filename)[0]
        month, year = map(int, date_part.split('_'))
        
        start_date = datetime(year, month, 1)
        end_date = start_date + relativedelta(months=1) - relativedelta(days=1)
        
        loger.info(f'Определен период отчета по имени файла: {start_date.date()} - {end_date.date()}')
    except Exception as e:
        loger.error(f"Не удалось определить дату из имени файла '{path}'. Ожидаемый формат: ММ_ГГГГ.xlsx. Ошибка: {e}")
        raise

    xls = pd.ExcelFile(path)
    sheet_names = xls.sheet_names
    
    rename_map = {
        # Общие поля
        'адрес склада': 'address', 'склад': 'address',
        'код склада': 'warehouse_code',
        'юрлицо склада': 'legal_entity', 'юридическое лицо': 'legal_entity',
        'инн кпп': 'inn', 'инн': 'inn',
        'поставщик': 'supplier',
        '_код товара': 'product_code', 'код товара': 'product_code',
        'товар': 'product_name', 'наименование товара': 'product_name',

        # Закупки
        'закупки прнакл шт': 'purchase_quantity', 'закупки, шт.': 'purchase_quantity',
        'закупки прнакл руб без ндс': 'purchase_sum_no_vat', 'закупки, руб. без ндс': 'purchase_sum_no_vat',
        'сумма закупки в закупочных ценах без ндс': 'purchase_sum_no_vat',
        'закупки прнакл руб с ндс': 'purchase_sum_with_vat', 'закупки, руб.': 'purchase_sum_with_vat',
        'сумма закупки в закупочных ценах с ндс': 'purchase_sum_with_vat',

        # Продажи
        'продажи упаковки': 'sale_quantity', 'продажи, шт.': 'sale_quantity',
        'выручка в ценах закупки': 'sale_revenue_purchase_price', 'выручка в ценах закупки, руб.': 'sale_revenue_purchase_price',
        'сумма продаж в закупочных ценах': 'sale_revenue_purchase_price',

        # Остатки
        'остаток на дату шт': 'stock_quantity', 'остаток, шт.': 'stock_quantity',
        
        # Общее поле для количества, если отчет разбит на листы
        'кол-во упаковок': 'quantity'
    }

    all_data = []
    expected_sheets = ['закупки', 'остатки', 'продажи']
    found_sheets = [s for s in sheet_names if s.lower().strip() in expected_sheets]

    if len(found_sheets) >= 2: # Формат с 3-мя листами
        loger.info(f"Обнаружен формат отчета с раздельными листами: {found_sheets}")
        for sheet_name in found_sheets:
            loger.info(f"Обработка листа: '{sheet_name}'")
            df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)

            header_row_index = -1
            header_keywords = ['товар', 'склад', 'закупки, шт.', 'кол-во упаковок']
            for i, row in df_raw.iterrows():
                row_values = {str(cell).lower().strip() for cell in row if pd.notna(cell)}
                if any(keyword in row_values for keyword in header_keywords):
                    header_row_index = i
                    loger.info(f"Найдена строка с заголовками на листе '{sheet_name}' по индексу {i}.")
                    break
            
            if header_row_index == -1:
                loger.warning(f"Не удалось найти строку с заголовками на листе '{sheet_name}'. Пропускаем лист.")
                continue

            headers = df_raw.iloc[header_row_index]
            df_sheet = df_raw.iloc[header_row_index + 1:].copy()
            df_sheet.columns = [str(col).lower().strip() for col in headers]
            df_sheet.rename(columns=rename_map, inplace=True)
            
            sheet_type = sheet_name.lower().strip()
            if 'quantity' in df_sheet.columns:
                if sheet_type == 'закупки':
                    df_sheet.rename(columns={'quantity': 'purchase_quantity'}, inplace=True)
                elif sheet_type == 'продажи':
                    df_sheet.rename(columns={'quantity': 'sale_quantity'}, inplace=True)
                elif sheet_type == 'остатки':
                    df_sheet.rename(columns={'quantity': 'stock_quantity'}, inplace=True)
            
            all_data.append(df_sheet)
        
        # Объединение данных с разных листов
        df = pd.concat(all_data, ignore_index=True, sort=False)
        
    else: # Формат с 1-м листом
        loger.info("Обнаружен формат отчета с одним листом. Чтение первого листа.")
        df_raw = pd.read_excel(xls, sheet_name=sheet_names[0], header=None)

        header_row_index = -1
        header_keywords = ['товар', 'склад', 'закупки, шт.', 'продажи, шт.']
        for i, row in df_raw.iterrows():
            row_values = {str(cell).lower().strip() for cell in row if pd.notna(cell)}
            if any(keyword in row_values for keyword in header_keywords):
                header_row_index = i
                loger.info(f"Найдена строка с заголовками по индексу {i}.")
                break
        
        if header_row_index == -1:
            raise ValueError(f"Не удалось найти строку с заголовками в файле. Ожидалось одно из: {header_keywords}")

        headers = df_raw.iloc[header_row_index]
        df = df_raw.iloc[header_row_index + 1:].copy()
        df.columns = [str(col).lower().strip() for col in headers]
        df.rename(columns=rename_map, inplace=True)

    loger.info(f"Успешно обработано {len(df)} строк.")

    # Добавление технических полей
    count_rows = len(df)
    df['uuid_report'] = [str(uuid.uuid4()) for _ in range(count_rows)]
    df['name_pharm_chain'] = name_pharm_chain
    # Если отчет был с 1 листа, то name_report будет общий, иначе он уже добавлен
    if 'name_report' not in df.columns:
        df['name_report'] = name_report
    df['start_date'] = str(start_date.date())
    df['end_date'] = str(end_date.date())
    df['processed_dttm'] = str(datetime.now())

    # Финальный список колонок
    final_columns = [
        'uuid_report', 'address', 'warehouse_code', 'legal_entity', 'inn', 'supplier',
        'product_code', 'product_name', 'purchase_quantity', 'purchase_sum_no_vat',
        'purchase_sum_with_vat', 'sale_quantity', 'sale_revenue_purchase_price',
        'stock_quantity', 'name_pharm_chain', 'name_report', 'start_date',
        'end_date', 'processed_dttm'
    ]

    for col in final_columns:
        if col not in df.columns:
            df[col] = None
            
    df_report = df[final_columns].copy()
    df_report = df_report.replace({np.nan: None})

    return {'table_report': df_report}

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    main_loger.info("Запуск локального теста для парсера 'Еаптека'.")
    test_file = r'C:\Users\nmankov\Desktop\отчеты\ЕАптека\закуп+продажи+ остатки\2024\10_2024.xlsx'
    
    if os.path.exists(test_file):
        try:
            result = extract_xls(path=test_file)
            df_result = result.get('table_report')

            if df_result is not None and not df_result.empty:
                base_filename = os.path.splitext(os.path.basename(test_file))[0]
                output_path = os.path.join(os.path.dirname(test_file), f"{base_filename}_result.csv")
                df_result.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
                main_loger.info(f"Результат успешно сохранен в файл: {output_path}")
        except Exception as e:
            main_loger.error(f"Во время локального теста произошла ошибка: {e}", exc_info=True)
    else:
        main_loger.warning(f"Тестовый файл не найден: {test_file}")