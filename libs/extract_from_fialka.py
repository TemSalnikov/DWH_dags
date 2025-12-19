import pandas as pd
import uuid
import re
import os
from datetime import datetime, timedelta
import calendar
from airflow.utils.log.logging_mixin import LoggingMixin

def _get_dates_from_filename(path: str, logger) -> tuple[datetime, datetime]:
    try:
        filename = os.path.basename(path)
        match = re.search(r'(\d{2})_(\d{4})', filename)
        if match:
            month = int(match.group(1))
            year = int(match.group(2))
            start_date = datetime(year, month, 1)
            _, last_day = calendar.monthrange(year, month)
            end_date = datetime(year, month, last_day)
            logger.info(f"Определен период отчета по имени файла: {start_date.date()} - {end_date.date()}")
            return start_date, end_date
        else:
            logger.warning("Не удалось определить дату из имени файла. Используем текущий месяц.")
            now = datetime.now()
            start_date = now.replace(day=1)
            _, last_day = calendar.monthrange(now.year, now.month)
            end_date = now.replace(day=last_day)
            return start_date, end_date
    except Exception as e:
        logger.error(f"Ошибка при определении даты: {e}")
        raise

def extract_fialka(path='', name_report='Закупки', name_pharm_chain='Фиалка') -> dict:
    """
    Парсер для отчетов аптеки 'Фиалка' (Закупки, Остатки, Продажи).
    Файл имеет листы типа 'Приход' и 'Оборот'.
    """
    logger = LoggingMixin().log
    try:
        logger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")

        start_date, end_date = _get_dates_from_filename(path, logger)

        xls = pd.ExcelFile(path)
        sheet_names = xls.sheet_names
        logger.info(f"Найдены листы: {sheet_names}")

        sheet_prihod = next((s for s in sheet_names if 'приход' in s.lower()), None)
        sheet_oborot = next((s for s in sheet_names if 'оборот' in s.lower()), None)

        all_dfs = []
        reports_to_process = []
        
        if 'Закуп_Продажи_Остатки' in name_report:
            reports_to_process = ['Закупки', 'Продажи', 'Остатки']
        else:
            reports_to_process = [name_report]

        for current_report in reports_to_process:
            df_result = pd.DataFrame()

            if current_report == 'Закупки':
                if not sheet_prihod:
                    logger.warning(f"Не найден лист с ключевым словом 'приход' для отчета '{current_report}'")
                    continue
            
            df_raw = pd.read_excel(path, sheet_name=sheet_prihod, header=None)
            
            try:
                header_row_idx = df_raw[df_raw[0].astype(str).str.contains("Наименование", na=False, case=False)].index[0]
            except IndexError:
                raise ValueError("Не найдена строка заголовка с 'Наименование' в листе прихода")

            headers = df_raw.iloc[header_row_idx].astype(str)
            df_data = df_raw.iloc[header_row_idx + 1:].copy()
            df_data.columns = headers

            df_data.rename(columns={headers[0]: 'product_name'}, inplace=True)

            if not df_data.empty and 'общий итог' in str(df_data.iloc[-1]['product_name']).lower():
                df_data = df_data.iloc[:-1]

            pharmacy_cols = [
                c for c in df_data.columns 
                if c not in ['product_name'] 
                and 'итог' not in str(c).lower() 
                and str(c).lower() != 'nan'
            ]

            df_melted = df_data.melt(
                id_vars=['product_name'], 
                value_vars=pharmacy_cols,
                var_name='pharmacy_name', 
                value_name='quantity'
            )
            
            df_result = df_melted

            elif current_report in ['Продажи', 'Остатки']:
                if not sheet_oborot:
                    logger.warning(f"Не найден лист с ключевым словом 'оборот' для отчета '{current_report}'")
                    continue
            
            df_raw = pd.read_excel(path, sheet_name=sheet_oborot, header=None)


            try:
                sub_header_idx = df_raw[df_raw[0].astype(str).str.contains("Наименование", na=False, case=False)].index[0]
            except IndexError:
                raise ValueError("Не найдена строка заголовка с 'Наименование' в листе оборота")
            
            pharm_name_idx = sub_header_idx - 1
            
            pharm_row = df_raw.iloc[pharm_name_idx]
            sub_header_row = df_raw.iloc[sub_header_idx]
            
            data_start_idx = sub_header_idx + 1
            df_data = df_raw.iloc[data_start_idx:].copy()

            if not df_data.empty and 'итог' in str(df_data.iloc[-1, 0]).lower():
                df_data = df_data.iloc[:-1]

            records = []
            num_cols = df_raw.shape[1]

            for i in range(1, num_cols - 1, 2):
                pharm_name = pharm_row.iloc[i]

                header_1 = str(sub_header_row.iloc[i]).lower()
                header_2 = str(sub_header_row.iloc[i+1]).lower()

                if (pd.isna(pharm_name) or str(pharm_name).strip() == '' or 
                    'итог' in str(pharm_name).lower() or 'итог' in header_1 or 'итог' in header_2):
                    continue

                col_sales_idx = None
                col_stock_idx = None
                
                if 'оборот' in header_1: col_sales_idx = i
                elif 'оборот' in header_2: col_sales_idx = i+1
                
                if 'остаток' in header_1: col_stock_idx = i
                elif 'остаток' in header_2: col_stock_idx = i+1

                target_col_idx = None
                if current_report == 'Продажи':
                    target_col_idx = col_sales_idx
                elif current_report == 'Остатки':
                    target_col_idx = col_stock_idx

                if target_col_idx is not None:
                    chunk = df_data[[0, target_col_idx]].copy()
                    chunk.columns = ['product_name', 'quantity']
                    chunk['pharmacy_name'] = pharm_name
                    records.append(chunk)

            if records:
                df_result = pd.concat(records, ignore_index=True)
            else:
                logger.warning("Не удалось извлечь данные по аптекам (пустой список records).")

            else:
                logger.warning(f"Неизвестный тип отчета: {current_report}")
                continue

            if not df_result.empty:
                df_result['quantity'] = pd.to_numeric(df_result['quantity'], errors='coerce').fillna(0)
                
                df_result = df_result[df_result['quantity'] != 0]
                
                df_result.reset_index(drop=True, inplace=True)

                df_result['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_result))]
                df_result['name_report'] = current_report
                df_result['name_pharm_chain'] = name_pharm_chain
                df_result['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
                df_result['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
                df_result['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                final_columns = [
                    'uuid_report', 'product_name', 'pharmacy_name', 'quantity', 'name_report', 'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm'
                ]
                
                for col in final_columns:
                    if col not in df_result.columns:
                        df_result[col] = None
                
                df_result = df_result[final_columns]
                df_result = df_result.replace({pd.NA: None, float('nan'): None})
                
                all_dfs.append(df_result)

        if not all_dfs:
            logger.warning("Не удалось сформировать данные ни по одному из типов отчетов.")
            return {'table_report': pd.DataFrame()}

        final_df = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"Парсинг завершен. Получено строк: {len(final_df)}")
        return {'table_report': final_df}

    except Exception as e:
        logger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты\Фиалка\Закуп Продажи Остатки\2024\06_2024.xlsx' 
    
    if os.path.exists(test_file_path):
        main_loger.info(f"Запуск локального теста для файла: {test_file_path}")
        try:
            result_data = extract_fialka(path=test_file_path, name_report='Закуп_Продажи_Остатки')
            df_res = result_data.get('table_report')
            
            if df_res is not None and not df_res.empty:
                output_path = os.path.join(os.path.dirname(test_file_path), f"{os.path.splitext(os.path.basename(test_file_path))[0]}_result.csv")
                df_res.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
                main_loger.info(f"Общий результат успешно сохранен в: {output_path}")
                print(f"Всего строк в общем файле: {len(df_res)}")
                print(df_res.head().to_string())
                print(df_res['name_report'].value_counts())
            else:
                main_loger.warning(f"Пустой результат")
        except Exception as e:
            main_loger.error(f"Ошибка: {e}", exc_info=True)
    else:
        main_loger.warning("Не удалось получить данные ни по одному из отчетов.")
