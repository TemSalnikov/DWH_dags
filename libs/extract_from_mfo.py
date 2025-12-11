import pandas as pd
import uuid
import os
import calendar
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

def _get_dates_from_period_column(df: pd.DataFrame, loger) -> tuple[datetime, datetime]:
    """
    Извлекает начальную и конечную дату из столбца 'Период'.
    start_date - первое число минимального месяца.
    end_date - последнее число максимального месяца.
    """
    try:
        loger.info("Определение периода отчета из столбца 'Период'.")
        if 'period_date' not in df.columns or df['period_date'].isnull().all():
            raise ValueError("Столбец 'period_date' отсутствует или пуст.")

        # Преобразуем столбец в datetime, игнорируя ошибки
        period_dates = pd.to_datetime(df['period_date'], dayfirst=True, errors='coerce')
        period_dates.dropna(inplace=True)

        if period_dates.empty:
            raise ValueError("Не найдено корректных дат в столбце 'Период'. Ожидаемый формат: ДД.ММ.ГГГГ или ДД.ММ.ГГГГ ЧЧ:ММ:СС")

        min_date = period_dates.min()
        max_date = period_dates.max()

        start_date = min_date.replace(day=1)
        _, last_day = calendar.monthrange(max_date.year, max_date.month)
        end_date = max_date.replace(day=last_day)

        loger.info(f"Определен период отчета: {start_date.date()} - {end_date.date()}")
        return start_date, end_date
    except Exception as e:
        loger.error(f"Не удалось определить даты из столбца 'Период'. Ошибка: {e}")
        raise

def extract_xls(path: str, name_report: str, name_pharm_chain: str = 'МФО') -> dict:
    """
    Парсер для отчетов 'МФО'.
    """
    loger = LoggingMixin().log
    loger.info(f"Начинаем парсинг отчета '{name_report}' для '{name_pharm_chain}' из файла: {path}")

    try:
        df = pd.read_excel(path, header=0, dtype=str)
        df.columns = [str(col).lower().strip() for col in df.columns]

        rename_map = {
            'период': 'period_date',
            'код партнера': 'partner_code',
            'клиент': 'client_name',
            'юридическое лицо': 'legal_entity',
            'регион': 'region',
            'город': 'city',
            'адрес грузополучателя': 'shipping_address',
            'полный адрес': 'full_address',
            'инн': 'pharmacy_inn',
            'поставщик мфо': 'mfo_supplier',
            'учитываемые дб': 'counted_db',
            'заказчик': 'customer',
            'номенклатура': 'product_name',
            'номенклатура группа': 'product_group',
            'распределение': 'distribution',
            'закупки': 'purchase_quantity',
            'сумма закупки': 'purchase_amount',
            'продажи': 'sale_quantity',
            'остаток на конецпериода': 'stock_quantity',
            'остаток на конец периода': 'stock_quantity',
            'накладная номер': 'invoice_number',
            'накладная дата': 'invoice_date',
        }
        df.rename(columns=rename_map, inplace=True)

        start_date, end_date = _get_dates_from_period_column(df, loger)

        df['uuid_report'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df['name_report'] = name_report
        df['name_pharm_chain'] = name_pharm_chain
        df['start_date'] = start_date.strftime('%Y-%m-%d %H:%M:%S')
        df['end_date'] = end_date.strftime('%Y-%m-%d %H:%M:%S')
        df['processed_dttm'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        final_columns = [
            'uuid_report','period_date', 'partner_code', 'client_name', 'legal_entity', 'region', 'city',
            'shipping_address', 'full_address', 'pharmacy_inn', 'mfo_supplier',
            'counted_db', 'customer', 'product_name', 'product_group',
            'distribution','purchase_quantity',  'purchase_amount', 'sale_quantity',
            'stock_quantity', 'invoice_number', 'invoice_date', 'name_report', 'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm',
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
    # Укажите путь к вашему тестовому файлу. Создайте его, если он не существует.
    test_file_path = r'C:\Users\nmankov\Desktop\отчеты\МФО\Закуп Продажи Остатки\2024\12_2024.xlsx'
    if os.path.exists(test_file_path):
        main_loger.info(f"Запуск локального теста для файла: {test_file_path}")
        try:
            result = extract_xls(path=test_file_path, name_report='Остатки+закуп+продажи')
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