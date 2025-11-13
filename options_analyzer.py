#!/usr/bin/env python3
"""
Микро-проект для анализа опционов на Московской бирже
"""

import requests
import pandas as pd
import json
import time
import argparse
import os
from datetime import datetime, timedelta
from tqdm import tqdm
import sys

# Параметры конфигурации
TICKERS = [
    'T', 'SBERP', 'ABIO','YDEX', 'SBER', 'TATN', 'TATNP', 'SVCB', 'FEES', 'AFKS', 'POSI', 'RTKM',
    'MGNT', 'PHOR', 'SNGS', 'SNGSP', 'MSNG', 'IRAO', 'VKCO', 'CHMF', 'RUAL',
    'GMKN', 'SMLT', 'NLMK', 'LKOH', 'NVTK', 'VTBR', 'SIBN', 'ALRS', 'PIKK',
    'AFLT', 'GAZP', 'ROSN', 'MTLR', 'MTSS', 'MOEX', 'MAGN'
    ]
MAX_DATE = '2026-06-01'
PERIOD = 15  # минут
TOP = 10
WAIT = 0.01  # секунды

# Базовый URL для API Московской биржи
BASE_URL = 'https://iss.moex.com/iss/statistics/engines/futures/markets/options/assets'

def get_expiration_dates(ticker):
    """
    Получение дат экспирации для указанного тикера
    """
    url = f"{BASE_URL}/{ticker}.json"
    
    try:
        time.sleep(WAIT)
        response = requests.get(url, timeout=30)
        
        if response.status_code != 200:
            print(f"⚠️  Ошибка при запросе {url}: статус {response.status_code}")
            return []
        
        data = response.json()
        
        if 'expirations' not in data or 'data' not in data['expirations']:
            print(f"⚠️  Нет данных об экспирации для {ticker}")
            return []
        
        # Фильтрация дат экспирации
        expirations = []
        max_date = datetime.strptime(MAX_DATE, '%Y-%m-%d')
        
        for item in data['expirations']['data']:
            exp_date = datetime.strptime(item[1], '%Y-%m-%d')
            if exp_date <= max_date:
                expirations.append(item[1])
        
        return expirations
        
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Ошибка сети при запросе {url}: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"⚠️  Ошибка парсинга JSON для {url}: {e}")
        return []
    except Exception as e:
        print(f"⚠️  Неожиданная ошибка при запросе {url}: {e}")
        return []

def get_options_data(ticker, exp_date):
    """
    Получение данных об опционах для указанного тикера и даты экспирации
    """
    url = f"{BASE_URL}/{ticker}/optionboard.json?expiration_date={exp_date}"
    
    try:
        time.sleep(WAIT)
        response = requests.get(url, timeout=30)
        
        if response.status_code != 200:
            print(f"⚠️  Ошибка при запросе {url}: статус {response.status_code}")
            return pd.DataFrame()
        
        data = response.json()
        all_data = []
        
        # Обработка данных по опционам (call и put)
        for option_type in ['call', 'put']:
            if option_type in data and 'data' in data[option_type]:
                columns = data[option_type]['columns']
                
                for row in data[option_type]['data']:
                    option_data = dict(zip(columns, row))
                    option_data['TICKER'] = ticker
                    option_data['EXP_DATE'] = exp_date
                    option_data['OPTION_TYPE'] = option_type.upper()
                    all_data.append(option_data)
        
        return pd.DataFrame(all_data)
        
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Ошибка сети при запросе {url}: {e}")
        return pd.DataFrame()
    except json.JSONDecodeError as e:
        print(f"⚠️  Ошибка парсинга JSON для {url}: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"⚠️  Неожиданная ошибка при запросе {url}: {e}")
        return pd.DataFrame()

def get_latest_quote(ticker):
    """
    Получение последней котировки для указанного тикера
    """
    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json?iss.reverse=true"
    try:
        time.sleep(WAIT)
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            print(f"⚠️  Ошибка при запросе последней котировки для {ticker}: статус {response.status_code}")
            return None, None
        data = response.json()
        if 'candles' not in data or 'data' not in data['candles'] or not data['candles']['data']:
            print(f"⚠️  Нет данных о свечах для {ticker}")
            return None, None
        # Последняя свеча находится в начале, так как iss.reverse=true
        latest_candle = data['candles']['data'][0]
        # Индексы для close и end (datetime) из метаданных
        columns = data['candles']['columns']
        close_index = columns.index('close')
        end_index = columns.index('end')
        last_price = latest_candle[close_index]
        last_price_datetime = latest_candle[end_index]
        return last_price, last_price_datetime
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Ошибка сети при запросе последней котировки для {ticker}: {e}")
        return None, None
    except json.JSONDecodeError as e:
        print(f"⚠️  Ошибка парсинга JSON для последней котировки {ticker}: {e}")
        return None, None
    except Exception as e:
        print(f"⚠️  Неожиданная ошибка при запросе последней котировки для {ticker}: {e}")
        return None, None

def analyze_options():
    """
    Основная функция анализа опционов
    """
    print("🚀 Начинаем анализ опционов на Московской бирже...")
    
    all_options_data = []
    
    # Обработка каждого тикера
    for ticker in tqdm(TICKERS, desc="Обработка тикеров"):
        print(f"\n📊 Обработка тикера: {ticker}")

        last_price, last_price_datetime = get_latest_quote(ticker)
        if last_price is None:
            print(f"⚠️  Не удалось получить последнюю котировку для {ticker}, пропускаем.")
            continue

        print(f"📈 Последняя котировка для {ticker}: {last_price} ({last_price_datetime})")
        
        # Получение дат экспирации
        exp_dates = get_expiration_dates(ticker)
        
        if not exp_dates:
            print(f"⚠️  Нет доступных дат экспирации для {ticker}")
            continue
        
        print(f"📅 Найдено {len(exp_dates)} дат экспирации для {ticker}")
        
        # Обработка каждой даты экспирации
        for exp_date in tqdm(exp_dates, desc=f"Обработка дат экспирации для {ticker}", leave=False):
            options_df = get_options_data(ticker, exp_date)
            
            if not options_df.empty:
                options_df['LAST_PRICE'] = last_price
                options_df['LAST_PRICE_DATETIME'] = last_price_datetime
                all_options_data.append(options_df)
    
    if not all_options_data:
        print("❌ Нет данных для анализа")
        return pd.DataFrame()
    
    # Объединение всех данных
    combined_df = pd.concat(all_options_data, ignore_index=True)
    
    if combined_df.empty:
        print("❌ Объединенный DataFrame пуст")
        return pd.DataFrame()
    
    print(f"📋 Всего получено {len(combined_df)} записей об опционах")
    
    # Фильтрация записей с ненулевым OFFER
    filtered_df = combined_df[combined_df['OFFER'] != 0].copy()
    
    if filtered_df.empty:
        print("❌ Нет записей с ненулевым OFFER")
        return pd.DataFrame()
    
    print(f"📋 После фильтрации по OFFER осталось {len(filtered_df)} записей")

    # Расчет метрик "денежности"
    filtered_df['PERCENT_DEVIATION'] = (abs(filtered_df['LAST_PRICE'] - filtered_df['STRIKE']) / filtered_df['LAST_PRICE']) * 100

    def get_option_state(row):
        strike = row['STRIKE']
        last_price = row['LAST_PRICE']
        option_type = row['OPTION_TYPE']
        percent_deviation = row['PERCENT_DEVIATION']
        
        # Порог для NTM (Near-the-Money)
        NTM_THRESHOLD_PERCENT = 10.0 # 10% как указано в задании
        
        if option_type == 'CALL':
            if last_price > strike:
                return 'ITM'
            elif percent_deviation <= NTM_THRESHOLD_PERCENT:
                return 'NTM'
            else:
                return 'OTM'
        elif option_type == 'PUT':
            if last_price < strike:
                return 'ITM'
            elif percent_deviation <= NTM_THRESHOLD_PERCENT:
                return 'NTM'
            else:
                return 'OTM'
        return 'UNKNOWN'

    filtered_df['STATE'] = filtered_df.apply(get_option_state, axis=1)

    # Фильтрация по ITM и NTM
    result_df = filtered_df[filtered_df['STATE'].isin(['ITM', 'NTM'])].copy()

    if result_df.empty:
        print("❌ После фильтрации по ITM/NTM нет данных для анализа")
        return pd.DataFrame()

    print(f"📋 После фильтрации по ITM/NTM осталось {len(result_df)} записей")
    
    # Расчет разницы между OFFER и THEORPRICE (дисконт)
    result_df['DISCOUNT'] = result_df['THEORPRICE'] - result_df['OFFER']
    result_df['DISCOUNT_PCT'] = (result_df['DISCOUNT'] / result_df['THEORPRICE']) * 100
    
    # Сортировка по дисконту (от большего к меньшему)
    result_df = result_df.sort_values('DISCOUNT_PCT', ascending=False)
    
    print(f"✅ Анализ завершен. Найдено {len(result_df)} опционов с дисконтом")
    
    return result_df

def save_table(df):
    """
    Сохранение таблицы в Excel файл
    """
    # Создание директории если не существует
    os.makedirs('output/tables', exist_ok=True)
    
    # Формирование имени файла
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"output/tables/{timestamp}.xlsx"
    
    try:
        # Сохранение в Excel
        df.to_excel(filename, index=False, engine='openpyxl')
        print(f"💾 Таблица сохранена в файл: {filename}")
        return filename
    except Exception as e:
        print(f"❌ Ошибка при сохранении файла: {e}")
        return None

def save_monitoring(df):
    """
    Сохранение результатов мониторинга в текстовый файл
    """
    # Создание директории если не существует
    os.makedirs('output/monitoring', exist_ok=True)
    
    # Формирование имени файла
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"output/monitoring/{timestamp}.txt"
    
    try:
        # Получение топ-10 опционов с наибольшим дисконтом
        top_options = df.head(TOP)
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"Мониторинг опционов - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
            
            if top_options.empty:
                f.write("Нет опционов с дисконтом для отображения\n")
            else:
                for idx, row in top_options.iterrows():
                    f.write(f"Тикер: {row['TICKER']}\n")
                    f.write(f"Тип опциона: {row['OPTION_TYPE']}\n")
                    f.write(f"Дата экспирации: {row['EXP_DATE']}\n")
                    f.write(f"SECID: {row['SECID']}\n")
                    f.write(f"Страйк: {row['STRIKE']:.2f}\n")
                    f.write(f"Последняя котировка: {row['LAST_PRICE']:.2f} ({row['LAST_PRICE_DATETIME']})\n")
                    f.write(f"Состояние: {row['STATE']}\n")
                    f.write(f"Отклонение от цены: {row['PERCENT_DEVIATION']:.2f}%\n")
                    f.write(f"Теоретическая цена: {row['THEORPRICE']:.2f}\n")
                    f.write(f"Оффер: {row['OFFER']:.2f}\n")
                    f.write(f"Дисконт: {row['DISCOUNT']:.2f} ({row['DISCOUNT_PCT']:.2f}%)\n")
                    f.write("-" * 40 + "\n")
        
        print(f"💾 Результаты мониторинга сохранены в файл: {filename}")
        return filename
        
    except Exception as e:
        print(f"❌ Ошибка при сохранении файла мониторинга: {e}")
        return None

def run_monitoring():
    """
    Запуск мониторинга с периодическим обновлением
    """
    print(f"🔍 Запуск мониторинга (обновление каждые {PERIOD} минут)...")
    
    try:
        while True:
            print(f"\n🔄 Обновление данных - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Получение и анализ данных
            df = analyze_options()
            
            if not df.empty:
                # Сохранение результатов мониторинга
                save_monitoring(df)
                
                # Вывод краткой информации в консоль
                print(f"📈 Топ-{min(TOP, len(df))} опционов с наибольшим дисконтом:")
                for idx, row in df.head(TOP).iterrows():
                    print(f"  {row['TICKER']} {row['OPTION_TYPE']} {row['STRIKE']:.0f}: "
                          f"{row['DISCOUNT']:.2f} ({row['DISCOUNT_PCT']:.2f}%)")
            else:
                print("⚠️  Нет данных для анализа")
            
            print(f"⏳ Ожидание следующего обновления через {PERIOD} минут...")
            time.sleep(PERIOD * 60)  # Конвертация минут в секунды
            
    except KeyboardInterrupt:
        print("\n🛑 Мониторинг остановлен пользователем")
    except Exception as e:
        print(f"❌ Ошибка в процессе мониторинга: {e}")

def main():
    """
    Главная функция
    """
    parser = argparse.ArgumentParser(description='Анализатор опционов Московской биржи')
    parser.add_argument('--table', action='store_true', help='Сохранить таблицу в Excel')
    parser.add_argument('--monitoring', action='store_true', help='Запустить мониторинг')
    
    args = parser.parse_args()
    
    # Если аргументы не указаны, показать справку
    if not args.table and not args.monitoring:
        parser.print_help()
        return
    
    try:
        if args.table:
            # Режим сохранения таблицы
            df = analyze_options()
            if not df.empty:
                save_table(df)
                
                # Вывод топ-10 в консоль
                print(f"\n📊 Топ-{min(TOP, len(df))} опционов с наибольшим дисконтом:")
                for idx, row in df.head(TOP).iterrows():
                    print(f"{idx+1:2d}. {row['TICKER']} {row['OPTION_TYPE']} {row['STRIKE']:.0f} "
                          f"(EXP: {row['EXP_DATE']}): {row['DISCOUNT']:.2f} ({row['DISCOUNT_PCT']:.2f}%)")
            else:
                print("❌ Нет данных для сохранения")
        
        elif args.monitoring:
            # Режим мониторинга
            run_monitoring()
    
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()