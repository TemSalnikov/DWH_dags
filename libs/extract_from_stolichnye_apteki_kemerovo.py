import pandas as pd
import re
import uuid
from datetime import datetime
import os
from airflow.utils.log.logging_mixin import LoggingMixin

def extract_movement_report(path, name_report, name_pharm_chain) -> dict:
    loger = LoggingMixin().log
    loger.info(f"Начат парсинг отчета о движении товара (Столичные Аптеки): {path}")
    
    try:
        df_raw = pd.read_excel(path, header=None)
    except Exception as e:
        loger.error(f"Ошибка чтения Excel файла: {e}")
        return {}

    start_date = None
    end_date = None
    
    date_pattern = re.compile(r'с\s+(\d{2}\.\d{2}\.\d{4})\s+по\s+(\d{2}\.\d{2}\.\d{4})')
    
    for i in range(min(15, len(df_raw))):
        row_str = " ".join([str(x) for x in df_raw.iloc[i].values if pd.notna(x)])
        match = date_pattern.search(row_str)
        if match:
            try:
                start_date = datetime.strptime(match.group(1), "%d.%m.%Y")
                end_date = datetime.strptime(match.group(2), "%d.%m.%Y")
                break
            except ValueError:
                continue

    if not start_date:
        loger.warning("Не удалось найти даты периода. Используем текущий месяц.")
        start_date = datetime.now().replace(day=1)
        end_date = start_date + pd.offsets.MonthEnd(1)

    loger.info(f"Определен период: {start_date} - {end_date}")

    header_row_idx = None
    col_indices = {} 
    
    target_cols = {
        'product': ['Товар', 'Наименование'],
        'party_info': ['Счет', 'Ц.зак.', 'Информация о партии'],
        'price_sell': ['Цена прод', 'Цена розн'],      
        'balance_start': ['Остаток на', 'Начальный остаток'],  
        'incoming': ['Приход', 'Прих.', 'Количество приход'], 
        'outgoing': ['Расход', 'Чеки, расх.'],                 
        'balance_end': ['Остаток на конец', 'Конечный остаток']
    }

    for i, row in df_raw.iterrows():
        row_list = [str(x).strip() for x in row.values]
        if any(k in row_list for k in target_cols['product']) and \
           any(any(x in str(cell) for x in target_cols['balance_end']) for cell in row_list):
            header_row_idx = i
            
            for idx, val in enumerate(row_list):
                val_str = str(val).strip()
                
                if val_str in target_cols['product']:
                    col_indices['product'] = idx
                elif any(x in val_str for x in target_cols['party_info']) or 'Ц.изг' in val_str:
                    col_indices['party_info'] = idx
                elif any(x in val_str for x in target_cols['price_sell']):
                    col_indices['price_sell'] = idx
                elif any(x in val_str for x in target_cols['incoming']):
                    col_indices['incoming'] = idx
                elif any(x in val_str for x in target_cols['outgoing']):
                    col_indices['outgoing'] = idx
                elif any(x in val_str for x in target_cols['balance_end']):
                    col_indices['balance_end'] = idx
                elif any(x in val_str for x in target_cols['balance_start']) and idx < col_indices.get('incoming', 999):
                    col_indices['balance_start'] = idx
            break
            
    if header_row_idx is None:
        loger.error("Не найдена строка заголовка таблицы.")
        return {}

    if 'product' not in col_indices: col_indices['product'] = 0
    if 'party_info' not in col_indices: col_indices['party_info'] = 4
    if 'price_sell' not in col_indices: col_indices['price_sell'] = 5
    if 'balance_start' not in col_indices: col_indices['balance_start'] = 6
    if 'incoming' not in col_indices: col_indices['incoming'] = 7
    if 'outgoing' not in col_indices: col_indices['outgoing'] = 8
    if 'balance_end' not in col_indices: col_indices['balance_end'] = 9

    data_rows = []
    
    current_pharmacy = None
    current_supplier = None
    
    def clean_float(val):
        if pd.isna(val) or str(val).strip() == '-' or str(val).strip() == '':
            return 0.0
        try:
            return float(str(val).replace(',', '.').replace('\xa0', ''))
        except:
            return 0.0

    product_keywords_re = re.compile(r'(№|\d+\s?мг|\d+\s?мл|\d+\s?г\.|таб|капс|супп|амп|фл\.|туба|уп\.|аэроз)', re.IGNORECASE)

    for i in range(header_row_idx + 1, len(df_raw)):
        row = df_raw.iloc[i]
        
        idx_prod = col_indices.get('product')
        val_name = str(row[idx_prod]).strip() if (idx_prod is not None and pd.notna(row[idx_prod])) else ""
        
        if not val_name or val_name in ["Итого", "Всего"]:
            continue

        if "АПТЕКА" in val_name.upper() and ("Г." in val_name.upper() or "Г " in val_name.upper()):
            current_pharmacy = val_name
            current_supplier = None
            continue
            
        is_product_like = product_keywords_re.search(val_name) or (re.search(r'\d', val_name) and len(val_name) > 5)
        
        if current_pharmacy and not is_product_like:
            current_supplier = val_name
            continue

        if current_pharmacy and is_product_like:
            val_party = str(row[col_indices['party_info']]).strip() if pd.notna(row[col_indices['party_info']]) else ""
            val_price_sell = clean_float(row[col_indices['price_sell']])
            
            val_bal_start = clean_float(row[col_indices['balance_start']])
            val_incoming = clean_float(row[col_indices['incoming']])
            val_outgoing = clean_float(row[col_indices['outgoing']])
            val_bal_end = clean_float(row[col_indices['balance_end']])

            row_dict = {
                'uuid_report': str(uuid.uuid4()),
                'pharmacy_name': current_pharmacy,
                'supplier': current_supplier if current_supplier else 'Не определен',
                
                'product_name': val_name,
                'party_info': val_party,
                
                'price_selling': val_price_sell,
                
                'quantity_start': val_bal_start,
                'purchase_quantity': val_incoming,
                'sale_quantity': val_outgoing,
                'remains_quantity': val_bal_end,
                
                'name_pharm_chain': name_pharm_chain,
                'name_report': name_report,
                'start_date': start_date,
                'end_date': end_date
            }
            data_rows.append(row_dict)

    df_result = pd.DataFrame(data_rows)
    
    if df_result.empty:
        loger.warning("Результат пуст. Возможно, структура файла отличается от ожидаемой.")
    else:
        df_result['start_date'] = df_result['start_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_result['end_date'] = df_result['end_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_result['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        final_columns = [
            'uuid_report',
            'product_name',
            'party_info',
            'price_selling',
            'quantity_start',
            'purchase_quantity',
            'sale_quantity',
            'remains_quantity',
            'pharmacy_name',
            'supplier',
            'name_report',
            'name_pharm_chain',
            'start_date',
            'end_date',
            'processed_dttm'
        ]
        
        df_result = df_result[final_columns]
        loger.info(f"Успешно обработано {len(df_result)} строк товаров.")

    return {
        'table_report': df_result
    }

def extract_xls(path, name_report, name_pharm_chain) -> dict:
    if name_pharm_chain == 'Столичные Аптеки Кемерово':
        return extract_movement_report(path, name_report, name_pharm_chain)
        
    return {}

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты_аптек\Столичные аптеки Кемерово\06_2025.xls'
    report_type = 'Закуп+остатки+продажи'
    chain_name = 'Столичные Аптеки Кемерово'

    if os.path.exists(test_file_path):
        main_loger.info(f"Запуск теста для '{chain_name}'")
        res = extract_xls(test_file_path, report_type, chain_name)
        df = res.get('table_report')
        if df is not None and not df.empty:
            print(df.head())
            output_path = os.path.join(os.path.dirname(test_file_path), 'result_stolica.csv')
            df.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
            print(f"Результат сохранен в: {output_path}")
    else:
        print("Файл не найден, укажите корректный путь для теста.")