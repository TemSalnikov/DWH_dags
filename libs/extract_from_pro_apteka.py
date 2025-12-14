import pandas as pd
import uuid
import os
import calendar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.utils.log.logging_mixin import LoggingMixin

def _get_dates_from_filename(path: str, loger) -> tuple[datetime, datetime]:
    try:
        filename = os.path.basename(path)
        date_part = os.path.splitext(filename)[0]
        report_date = datetime.strptime(date_part, "%m_%Y")
        
        start_date = (report_date - relativedelta(months=2)).replace(day=1)
        
        _, last_day = calendar.monthrange(report_date.year, report_date.month)
        end_date = report_date.replace(day=last_day)
        
        loger.info(f"Определен период отчета по имени файла: {start_date.date()} - {end_date.date()}")
        return start_date, end_date
    except Exception as e:
        loger.error(f"Не удалось определить дату из имени файла '{os.path.basename(path)}'. Ожидаемый формат: ММ_ГГГГ.xlsx. Ошибка: {e}")
        raise

def extract_xls(path='', name_report='Закупки', name_pharm_chain='проАптека') -> dict:
    """
    Парсер для отчетов 'проАптека'.
    """
    loger = LoggingMixin().log
    try:
        loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")

        start_date, end_date = _get_dates_from_filename(path, loger)

        xls = pd.ExcelFile(path)
        sheet_names = xls.sheet_names
        
        all_dfs = []
        
        purchases_map = {
            'инн': 'inn',
            'наименование контрагента': 'contractor_name',
            'единая коммерческая группа': 'commercial_group',
            'идентификатор ау': 'pharmacy_id',
            'наименование ау': 'pharmacy_name',
            'адрес ау': 'pharmacy_address',
            'идентификатор товара': 'product_id',
            'наименование товара': 'product_name',
            'наименование дистрибьютора': 'distributor_name',
            'месяц': 'period',
            'закупки. кол-во': 'purchase_quantity',
            'закупки. сумма': 'purchase_amount',
            'продажи. кол-во': 'sale_quantity',
            'продажи. сумма': 'sale_amount'
        }

        remains_map = {
            'код ау': 'pharmacy_id',
            'регион ау': 'region',
            'тип ау': 'pharmacy_type',
            'категория матрицы': 'matrix_category',
            'объем закупок': 'purchase_volume',
            'наименование ау': 'pharmacy_name',
            'адрес': 'pharmacy_address',
            'группа контрагента': 'contractor_group',
            'инн': 'inn',
            'контрагент': 'contractor_name',
            'екг': 'ekg',
            'аптечная сеть': 'pharmacy_chain',
            'актуальный контрагент': 'actual_contractor',
            'код товара': 'product_code',
            'наименование товара': 'product_name',
            'производитель товара': 'manufacturer',
            'период': 'period',
            'остатки на начало периода. кол-во': 'start_period_stock',
            'остатки на конец периода. кол-во': 'end_period_stock',
            'address': 'pharmacy_address',
            'departmentcode': 'pharmacy_id',
            'department_name': 'pharmacy_name',
            'goodscode': 'product_code',
            'goods_name': 'product_name',
            'producer_name': 'manufacturer',
            'quantity': 'end_period_stock',
            'stockbalancedate': 'period'
        }

        for sheet_name in sheet_names:
            sheet_lower = sheet_name.lower()
            current_map = None
            current_report_type = None
            
            if 'закуп' in sheet_lower:
                loger.info(f"Обработка листа закупок/продаж: {sheet_name}")
                current_map = purchases_map
                current_report_type = 'Закупки_Продажи'
            elif 'остат' in sheet_lower:
                loger.info(f"Обработка листа остатков: {sheet_name}")
                current_map = remains_map
                current_report_type = 'Остатки'
            else:
                loger.warning(f"Лист '{sheet_name}' пропущен (не содержит 'закуп' или 'остат').")
                continue

            df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=str)
            
            header_row_index = -1
            if current_report_type == 'Закупки_Продажи':
                keywords = ['инн', 'наименование контрагента', 'закупки. кол-во']
            else:
                keywords = ['код ау', 'регион ау', 'остатки на начало периода. кол-во', 'departmentcode', 'goodscode']

            for i, row in df_raw.head(20).iterrows():
                row_values = [str(v).lower().strip() for v in row.values]
                if any(k in row_values for k in keywords):
                    header_row_index = i
                    break
            
            if header_row_index == -1:
                loger.warning(f"Не найден заголовок на листе '{sheet_name}'. Пропуск.")
                continue

            df_sheet = pd.read_excel(xls, sheet_name=sheet_name, header=header_row_index, dtype=str)
            
            df_sheet.columns = [str(col).lower().strip() for col in df_sheet.columns]
            
            df_sheet.rename(columns=current_map, inplace=True)
            
            if 'period' in df_sheet.columns:
                def convert_excel_date(val):
                    if val is None: return None
                    s = str(val).strip()
                    if s.replace('.', '', 1).isdigit():
                        try:
                            d_val = float(s)
                            if 35000 < d_val < 60000:
                                dt = datetime(1899, 12, 30) + timedelta(days=d_val)
                                return dt.strftime('%d.%m.%Y')
                        except:
                            pass
                    return s
                df_sheet['period'] = df_sheet['period'].apply(convert_excel_date)

            all_dfs.append(df_sheet)

        if not all_dfs:
            raise ValueError("Не найдено ни одного подходящего листа для обработки.")

        df_report = pd.concat(all_dfs, ignore_index=True, sort=False)

        df_report['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df_report))]
        df_report['name_report'] = name_report
        df_report['name_pharm_chain'] = name_pharm_chain
        df_report['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df_report['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df_report['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        ordered_columns = [
            'uuid_report', 
            'inn', 'contractor_name', 'pharmacy_name', 'pharmacy_address', 'product_name',
            'pharmacy_id', 'commercial_group', 'product_id', 'distributor_name',
            'purchase_quantity', 'purchase_amount', 'sale_quantity', 'sale_amount', 'region', 'pharmacy_type', 'matrix_category', 'purchase_volume',
            'contractor_group', 'ekg', 'pharmacy_chain', 'actual_contractor', 'product_code',
            'manufacturer', 'period', 'start_period_stock', 'end_period_stock',
            'name_report', 'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm'
        ]
        
        for col in ordered_columns:
            if col not in df_report.columns:
                df_report[col] = None
        
        df_report = df_report[ordered_columns]
        
        df_report = df_report.where(pd.notna(df_report), None)

        loger.info(f"Парсинг '{name_pharm_chain}' успешно завершен. Итого строк: {len(df_report)}")
        return {'table_report': df_report}

    except Exception as e:
        loger.error(f"Ошибка при парсинге отчета '{name_report}' для '{name_pharm_chain}': {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main_loger = LoggingMixin().log
    test_file_path = r'c:\Users\nmankov\Desktop\отчеты\ПроАптека\Закуп Продажи Остатки\2024\09_2024.xlsb'
    test_report_type = 'Закупки+продажи+остатки'
    
    if os.path.exists(test_file_path):
        main_loger.info(f"Запуск локального теста для файла: {test_file_path}")
        try:
            result_data = extract_xls(path=test_file_path, name_report=test_report_type, name_pharm_chain='проАптека')
            df_result = result_data.get('table_report')
            
            if df_result is not None and not df_result.empty:
                output_path = os.path.join(os.path.dirname(test_file_path), f"{os.path.splitext(os.path.basename(test_file_path))[0]}_result.csv")
                df_result.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
                main_loger.info(f"Результат успешно сохранен в: {output_path}")
            else:
                main_loger.warning("Результирующий датафрейм пуст (это нормально для заглушки).")
        except Exception as e:
            main_loger.error(f"Во время теста произошла ошибка: {e}", exc_info=True)
    else:
        main_loger.warning(f"Тестовый файл не найден: {test_file_path}")