import pandas as pd
import re
import uuid
import hashlib
from datetime import datetime
import os
from airflow.utils.log.logging_mixin import LoggingMixin

def extract_purchases_lara(path, name_report, name_pharm_chain) -> dict:
    loger = LoggingMixin().log
    loger.info(f"Начат парсинг иерархического отчета Лара (динамические колонки): {path}")
    
    try:
        df_raw = pd.read_excel(path, header=None)
    except Exception as e:
        loger.error(f"Ошибка чтения Excel файла: {e}")
        return {}

    start_date = None
    end_date = None
    for i in range(min(15, len(df_raw))):
        for cell in df_raw.iloc[i].astype(str).values:
            if "Дата начала:" in cell:
                match = re.search(r'(\d{2}\.\d{2}\.\d{4}\s\d{1,2}:\d{2}:\d{2})', cell)
                if match: start_date = datetime.strptime(match.group(1), "%d.%m.%Y %H:%M:%S")
            if "Дата конец:" in cell:
                match = re.search(r'(\d{2}\.\d{2}\.\d{4}\s\d{1,2}:\d{2}:\d{2})', cell)
                if match: end_date = datetime.strptime(match.group(1), "%d.%m.%Y %H:%M:%S")

    if not start_date or not end_date:
        loger.warning("Не удалось найти 'Дата начала' или 'Дата конец' в файле.")
        start_date = datetime.now().replace(day=1)
        end_date = start_date + pd.offsets.MonthEnd(1)
    loger.info(f"Определен период отчета: {start_date} - {end_date}")

    header_row_idx = None
    col_indices = {} 

    for i, row in df_raw.iterrows():
        row_list = [str(x).strip() for x in row.values]
        if "Аптека" in row_list and "Документ" in row_list:
            header_row_idx = i
            for idx, val in enumerate(row_list):
                if val:
                    col_indices[val] = idx
            break
            
    if header_row_idx is None:
        loger.error("Не найдена строка заголовка таблицы")
        return {}

    idx_doc = col_indices.get('Документ') 
    idx_firm = col_indices.get('Фирма')
    idx_supp = col_indices.get('Поставщик')
    idx_manuf = col_indices.get('Изготовитель')
    idx_batch = col_indices.get('Партия')
    idx_qty = idx_batch + 1 if idx_batch is not None else 10

    if idx_doc is None:
        loger.error("Критическая ошибка: колонка 'Документ' не найдена в заголовке.")
        return {}

    data_rows = []
    current_product = None
    current_doc_date = None
    
    for i in range(header_row_idx + 1, len(df_raw)):
        row = df_raw.iloc[i]
        
        col_0 = str(row[0]).strip() if pd.notna(row[0]) else ""
        
        col_doc = str(row[idx_doc]).strip() if (idx_doc < len(row) and pd.notna(row[idx_doc])) else ""

        if re.match(r'^\d{2}\.\d{2}\.\d{4}', col_0) and col_doc == "":
            try:
                date_part = col_0.split(' ')[0]
                current_doc_date = datetime.strptime(date_part, "%d.%m.%Y")
                continue
            except ValueError:
                pass 

        is_data_row = col_doc != "" and col_doc != "Документ"
        
        if is_data_row:
            val_firm = str(row[idx_firm]).strip() if (idx_firm and idx_firm < len(row) and pd.notna(row[idx_firm])) else ""
            val_supp = str(row[idx_supp]).strip() if (idx_supp and idx_supp < len(row) and pd.notna(row[idx_supp])) else ""
            val_manuf = str(row[idx_manuf]).strip() if (idx_manuf and idx_manuf < len(row) and pd.notna(row[idx_manuf])) else ""
            val_batch = str(row[idx_batch]).strip() if (idx_batch and idx_batch < len(row) and pd.notna(row[idx_batch])) else ""
            
            try:
                val_qty = float(row[idx_qty]) if (idx_qty < len(row) and pd.notna(row[idx_qty])) else 0.0
            except (ValueError, TypeError):
                val_qty = 0.0

            row_dict = {
                'uuid': str(uuid.uuid4()),
                'report_date': current_doc_date if current_doc_date else start_date,
                'pharmacy_name': col_0,
                'product_name': current_product,
                'doc_number': col_doc,
                'manufacturer': val_manuf,
                'supplier_name': val_supp,
                'firm': val_firm,
                'batch_number': val_batch,
                'quantity': val_qty,
                'pharm_chain_name': name_pharm_chain,
                'type_report': name_report
            }
            
            data_rows.append(row_dict)
            continue

        if col_0 and not is_data_row and not re.match(r'^\d{2}\.\d{2}\.\d{4}', col_0):
            if col_0 not in ["Товар", "Итого", "Аптека"]:
                current_product = col_0

    df_result = pd.DataFrame(data_rows)
    
    if df_result.empty:
        loger.warning("Внимание: отчет пуст. Проверьте индексы или формат файла.")
    else:
        df_result['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df_result['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df_result['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        loger.info(f"Успешно обработано {len(df_result)} строк.")

    return {
        'table_report': df_result
    }

def extract_xls(path, name_report, name_pharm_chain) -> dict:
    """Диспетчер для парсеров 'Лара'."""
    if name_report == 'Закупки':
        return extract_purchases_lara(path, name_report, name_pharm_chain)
    return {}

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    test_file_path = r'C:\Users\nmankov\Desktop\отчеты\ЛАРА\Закуп\2024\03_2024.xls'
    report_type_to_test = 'Закупки'
    pharm_chain_name = 'Лара'

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