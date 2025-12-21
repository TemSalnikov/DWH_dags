import pandas as pd
import uuid
import os
import calendar
import re
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

FINAL_COLUMNS = [
    'uuid_report', 'invoice_number', 'doc_date', 'product_name', 'supplier', 
    'pharmacy_name', 'legal_entity', 'price', 'purchase_quantity', 
    'purchase_sum', 'vat', 'quantity',
    'name_report', 'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm'
]

def _get_dates_from_filename(path: str, loger, df_raw: pd.DataFrame = None) -> tuple[datetime, datetime]:
    try:
        filename = os.path.basename(path)
        match = re.search(r'(\d{2})[._](\d{4})', filename)
        if match:
            date_str = f"{match.group(1)}_{match.group(2)}"
            report_date = datetime.strptime(date_str, "%m_%Y")
        else:
            try:
                report_date = datetime.strptime(os.path.splitext(filename)[0], "%m_%Y")
            except:
                report_date = datetime.now()

        start_date = report_date.replace(day=1)
        _, last_day = calendar.monthrange(report_date.year, report_date.month)
        end_date = report_date.replace(day=last_day)
        
        loger.info(f"Период отчета: {start_date.date()} - {end_date.date()}")
        return start_date, end_date
    except Exception as e:
        loger.warning(f"Ошибка даты: {e}")
        now = datetime.now()
        return now.replace(day=1), now

def clean_num(val):
    if pd.isna(val): return 0.0
    s = str(val).strip().replace(',', '.')
    if not s or s == '-' or s.lower() == 'nan': return 0.0
    try:
        return float(s)
    except:
        return 0.0

def extract_sales_and_remains(path: str, name_report: str, name_pharm_chain: str) -> dict:
    loger = LoggingMixin().log
    loger.info(f"Парсинг Продажи + Остатки из: {path}")

    try:
        df_raw = pd.read_excel(path, header=None)
        start_date, end_date = _get_dates_from_filename(path, loger, df_raw)

        header_idx = None
        cols = {}
        
        for i, row in df_raw.head(30).iterrows():
            vals = [str(x).strip().lower() for x in row.values]
            if any("товар" in v for v in vals) and any("расход" in v or "чеки" in v for v in vals):
                header_idx = i
                for idx, val in enumerate(vals):
                    if "товар" in val: cols['product'] = idx
                    if "счет" in val or "ц.зак" in val: cols['price_purch'] = idx
                    if "цена" in val and "прод" in val: cols['price_sell'] = idx
                    if "расход" in val or "чеки" in val: cols['sales'] = idx
                    if "остаток" in val and "конец" in val: cols['remains'] = idx
                break

        if header_idx is None:
            raise ValueError("Не найдена шапка (Товар, Расход, Остаток).")

        if 'price_sell' not in cols:
            next_vals = [str(x).strip().lower() for x in df_raw.iloc[header_idx + 1].values]
            for idx, val in enumerate(next_vals):
                if "цена" in val and "прод" in val: cols['price_sell'] = idx
                if "счет" in val or "ц.зак" in val: cols['price_purch'] = idx

        data = []
        current_pharmacy = None
        pharmacy_regex = re.compile(r'^\d{3}\s+.*')

        idx_p = cols.get('product')
        idx_s = cols.get('sales')
        idx_r = cols.get('remains')
        idx_sell = cols.get('price_sell')
        idx_purch = cols.get('price_purch')

        for i in range(header_idx + 1, len(df_raw)):
            row = df_raw.iloc[i]
            prod_name = str(row[idx_p]).strip() if pd.notna(row[idx_p]) else ""
            
            if not prod_name or prod_name.lower() == 'nan': continue
            if pharmacy_regex.match(prod_name):
                current_pharmacy = prod_name
                continue
            if "Итого" in prod_name or "Отчет сформирован" in prod_name: continue

            if current_pharmacy:
                qty_s = clean_num(row[idx_s]) if idx_s else 0.0
                qty_r = clean_num(row[idx_r]) if idx_r else 0.0
                pr_sell = clean_num(row[idx_sell]) if idx_sell else 0.0
                pr_purch = clean_num(row[idx_purch]) if idx_purch else 0.0


                base_uuid = str(uuid.uuid4())
                
                common_data = {
                    'pharmacy_name': current_pharmacy,
                    'product_name': prod_name,
                    'price': pr_sell,
                    'name_pharm_chain': name_pharm_chain,
                    'start_date': start_date,
                    'end_date': end_date,
                    'processed_dttm': datetime.now()
                }
                if True: 
                    row_sales = common_data.copy()
                    row_sales['uuid_report'] = str(uuid.uuid4())
                    row_sales['name_report'] = 'Продажи'
                    row_sales['quantity'] = qty_s
                    data.append(row_sales)


                if True:
                    row_remains = common_data.copy()
                    row_remains['uuid_report'] = str(uuid.uuid4())
                    row_remains['name_report'] = 'Остатки'
                    row_remains['quantity'] = qty_r
                    data.append(row_remains)

        df = pd.DataFrame(data)
        
        if not df.empty:
            for c in ['start_date', 'end_date', 'processed_dttm']:
                df[c] = df[c].dt.strftime('%Y-%m-%d %H:%M:%S')

        
        for c in FINAL_COLUMNS:
            if c not in df.columns: df[c] = None

        loger.info(f"Успешно извлечено {len(df)} строк (продажи + остатки).")
        return {'table_report': df[FINAL_COLUMNS]}

    except Exception as e:
        loger.error(f"Ошибка парсинга: {e}", exc_info=True)
        raise

def extract_purchases(path, name_report, name_pharm_chain):
    loger = LoggingMixin().log
    loger.info(f"Парсинг ЗАКУПОК: {path}")
    
    rename_map = {
        '№ накладной': 'invoice_number', 'Дата поставки': 'doc_date',
        'Наименование препарата': 'product_name', 'Поставщик': 'supplier',
        'Получатель': 'pharmacy_name', 'Организация': 'legal_entity',
        'Цена': 'price', 'Кол-во': 'purchase_quantity', 'Сумма': 'purchase_sum', 'НДС': 'vat'
    }

    df_raw = pd.read_excel(path, header=None, dtype=str)
    start_date, end_date = _get_dates_from_filename(path, loger, df_raw)

    header_idx = -1
    keys = list(rename_map.keys())
    for i, row in df_raw.head(20).iterrows():
        vals = [str(x).strip() for x in row.values]
        if any(k in vals for k in keys):
            header_idx = i
            break
    if header_idx == -1: header_idx = 4

    headers = df_raw.iloc[header_idx]
    df = df_raw.iloc[header_idx + 1:].copy()
    df.columns = [str(h).strip() for h in headers]
    df.rename(columns=rename_map, inplace=True)
    df.dropna(how='all', inplace=True)
    
    if 'product_name' in df.columns:
        df = df[df['product_name'].notna()]

    df['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df))]
    df['name_report'] = name_report
    df['name_pharm_chain'] = name_pharm_chain
    df['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
    df['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
    df['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for c in FINAL_COLUMNS:
        if c not in df.columns: df[c] = None
            
    return {'table_report': df[FINAL_COLUMNS]}


def extract_xls(path, name_report, name_pharm_chain) -> dict:
    loger = LoggingMixin().log
    loger.info(f"Диспетчер 'Фармпомощь Бийск': '{name_report}' файл '{path}'")
    
    nm = name_report.lower()
    if 'закуп' in nm:
        return extract_purchases(path, name_report, name_pharm_chain)
    elif 'продажи' in nm or 'остатки' in nm:
        return extract_sales_and_remains(path, name_report, name_pharm_chain)
    else:
        loger.warning(f"Неизвестный отчет: {name_report}")
        return {}

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    main_loger.info("Запуск локального теста для парсера 'Фармпомощь Бийск'.")
    
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты_аптек\Фармпомощь Бийск\Продажи -Остатки\02_2025.xls'
    
    test_report_type = 'Продажи' 

    if os.path.exists(test_file_path):
        main_loger.info(f"Тестовый файл найден: {test_file_path}")
        try:
            result = extract_xls(path=test_file_path, name_report=test_report_type, name_pharm_chain='Фармпомощь Бийск')
            df = result.get('table_report')
            
            if df is not None and not df.empty:
                output_filename = f"{os.path.splitext(os.path.basename(test_file_path))[0]}_result.csv"
                output_path = os.path.join(os.path.dirname(test_file_path), output_filename)
                
                df.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
                main_loger.info(f"Результат успешно сохранен в: {output_path}")
                print(df.head(10))
            else:
                main_loger.warning("Результат пустой!")

        except Exception as e:
            main_loger.error(f"Во время локального теста произошла ошибка: {e}", exc_info=True)
    else:
        main_loger.warning(f"Тестовый файл не найден по пути: {test_file_path}")