import requests
import pandas as pd
from datetime import datetime
import time

def get_spot_prices_coingecko(coin_id, currency="usd", days=365):
    """
    Загружает исторические данные по токену с CoinGecko API.
    Возвращает DataFrame с ценами и временными метками.
    """
    # Базовый URL API CoinGecko для получения исторических данных
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    
    # Параметры запроса
    params = {
        "vs_currency": currency,  # Валюта, в которой нужны котировки (по умолчанию USD)
        "days": days,             # Количество дней для получения данных
    }
    
    try:
        # Отправляем GET-запрос
        response = requests.get(url, params=params)
        response.raise_for_status()  # Проверяем, нет ли ошибок в ответе
        
        # Парсим JSON-ответ
        data = response.json()
        
        # Извлекаем данные о ценах
        prices = data.get('prices', [])
        
        if not prices:
            print(f"Нет данных для {coin_id}.")
            return None
        
        # Создаем DataFrame с ценами
        df = pd.DataFrame(prices, columns=["timestamp", "price"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")  # Конвертируем timestamp в дату
        df.set_index("timestamp", inplace=True)  # Устанавливаем timestamp как индекс
        df.rename(columns={"price": coin_id}, inplace=True)  # Переименовываем колонку
        
        return df
    
    except requests.exceptions.HTTPError as e:
        print(f"Ошибка HTTP: {e}")
        print(f"Статус код: {response.status_code}")
        print(f"Ответ сервера: {response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при запросе к API: {e}")
        return None
    

def get_spot_prices_bybit(symbol, interval='60', limit=1000, start_time=None, end_time=None, col_name='spot'):
    """
    Получает исторические данные для спотовой пары на Bybit.
    
    :param symbol: Торговая пара (например 'SOLUSDT')
    :param interval: Интервал ('1', '3', '5', '15', '30', '60', '120', 'D' и т.д.)
    :param limit: Количество свечей (макс. 1000)
    :param start_time: Начальное время (datetime или timestamp в ms)
    :param end_time: Конечное время (datetime или timestamp в ms)
    :return: DataFrame с данными или пустой DataFrame при ошибке
    """
    BASE_URL = "https://api.bybit.com/v5/market/kline"
    
    # Подготовка временных меток
    params = {
        'category': 'spot',
        'symbol': symbol,
        'interval': interval,
        'limit': limit
    }

    if start_time is not None:
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp() * 1000)
        params['start'] = start_time

    if end_time is not None:
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp() * 1000)
        params['end'] = end_time

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()

        if data['retCode'] == 0:
            klines = data['result']['list']
            if not klines:
                return pd.DataFrame()  # Пустой ответ

            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'
            ])

            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'turnover']
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
            df['timestamp'] = df['timestamp'].astype('int64')
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')

            # Формируем DataFrame с нужными колонками
            token = df[['datetime', 'open', 'high', 'low', 'close', 'volume', 'turnover']].copy()
            token.set_index('datetime', inplace=True)
            token.index.name = 'time'

            token[f'{col_name}_open'] = token['open']
            token[f'{col_name}_high'] = token['open']
            token[f'{col_name}_low'] = token['open']
            token[f'{col_name}_close'] = token['close']
            token[f'{col_name}_volume'] = token['volume']
            token[f'{col_name}_turnover'] = token['turnover']
            token.drop(columns=['open', 'high', 'low', 'close', 'volume', 'turnover'], inplace=True)

            return token.sort_index()
        else:
            print(f"Ошибка API: {data['retMsg']}")
            return pd.DataFrame()

    except Exception as e:
        print(f"Ошибка при запросе данных: {e}")
        return pd.DataFrame()
    

def get_spot_data_bybit_full_period(symbol, interval='60', start_time=None, end_time=None, col_name='spot'):
    """
    Загружает исторические данные за указанный период, обходя лимит в 1000 свечей.
    Останавливается ровно на start_time.
    
    :param symbol: Торговая пара (например 'SOLUSDT')
    :param interval: Интервал ('1', '3', '5', '15', '30', '60', '120', 'D' и т.д.)
    :param start_time: Начало периода (datetime объект или строка 'YYYY-MM-DD HH:MM:SS')
    :param end_time: Конец периода (по умолчанию — сейчас)
    :return: DataFrame с данными за весь период
    """
    # Конвертация времени в timestamp (ms)
    if isinstance(start_time, str):
        start_time = datetime.fromisoformat(start_time.replace(" ", "T"))
    if isinstance(end_time, str):
        end_time = datetime.fromisoformat(end_time.replace(" ", "T"))

    if not isinstance(start_time, datetime):
        raise ValueError("start_time должен быть datetime или строкой формата 'YYYY-MM-DD HH:MM:SS'")
    
    if end_time is None:
        end_time = datetime.now()
    elif not isinstance(end_time, datetime):
        raise ValueError("end_time должен быть datetime или None")

    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)

    all_dfs = []
    current_end = end_ms  # начинаем с конца

    print(f"Начинаем загрузку данных для {symbol} с {start_time} по {end_time}, интервал={interval}")

    while True:
        df = get_spot_prices_bybit(
            symbol=symbol,
            interval=interval,
            limit=1000,
            start_time=start_ms,
            end_time=current_end,
            col_name=col_name
        )

        if df.empty:
            print("Данные больше не приходят.")
            break

        # Фильтруем, чтобы не брать данные раньше start_time
        df = df[df.index >= start_time]
        if df.empty:
            print("Все загруженные данные раньше start_time — остановка.")
            break

        all_dfs.append(df)

        # Самая ранняя метка в этом фрейме
        earliest_timestamp_ms = int(df.index[0].timestamp() * 1000)

        # Если самая ранняя точка уже <= start_time — выходим
        if earliest_timestamp_ms <= start_ms:
            break

        # Сдвигаем конец на 1 мс раньше первой свечи
        current_end = earliest_timestamp_ms - 1

        print(f"Загружено до: {df.index[0]}")

        if len(all_dfs) > 10_000:
            print("Превышено максимальное количество итераций.")
            break

    # Объединяем и сортируем
    if all_dfs:
        full_df = pd.concat(all_dfs).sort_index()
        full_df = full_df[~full_df.index.duplicated(keep='first')]  # Убираем дубли
        # Финальная фильтрация — гарантируем, что все данные в нужном диапазоне
        full_df = full_df[(full_df.index >= start_time) & (full_df.index <= end_time)]
        print(f"✅ Загружено {len(full_df)} свечей за период от {start_time} до {end_time}.")
        return full_df
    else:
        print("❌ Не удалось загрузить данные.")
        return pd.DataFrame()
    

def get_funding_rates(symbol, start_time=None, end_time=None, limit=200):
    """Получает исторические данные по фандингу"""
    params = {
        'category': 'linear',
        'symbol': symbol,
        'limit': limit
    }
    if start_time:
        params['startTime'] = start_time
    if end_time:
        params['endTime'] = end_time
    
    try:
        response = requests.get("https://api.bybit.com/v5/market/funding/history", params=params)
        data = response.json()
        return data['result']['list'] if data.get('retCode') == 0 else None
    except Exception as e:
        print(f"Ошибка при запросе для {symbol}: {e}")
        return None


def get_funding_bybit(symbols, start_date, end_date, col_name='sol'):
    """
    Загружает исторические ставки фандинга с Bybit и дополняет их нулевыми значениями
    на каждый час между моментами выплаты, чтобы выровнять с часовыми данными.
    
    :param symbols: Список символов (например ['SOLUSDT'])
    :param start_date: Начало периода (строка или datetime)
    :param end_date: Конец периода
    :param col_name: Имя актива для префикса колонки
    :return: DataFrame с ежечасными строками и funding = 0 между реальными выплатами
    """
    start_time = int(pd.to_datetime(start_date).timestamp() * 1000)
    end_time = int(pd.to_datetime(end_date).timestamp() * 1000)
    
    results = {}
    
    for symbol in symbols:
        print(f"Загрузка фандинга для {symbol} с {start_date} по {end_date}")
        all_rates = []
        current_end = end_time
        
        while True:
            rates = get_funding_rates(symbol, end_time=current_end)
            if not rates or len(rates) == 0:
                break
            
            # Фильтруем по диапазону
            filtered = [r for r in rates if int(r['fundingRateTimestamp']) >= start_time]
            all_rates.extend(filtered)
            
            # Проверяем, достигли ли начала
            oldest_time = min(int(r['fundingRateTimestamp']) for r in rates)
            if oldest_time <= start_time:
                break
                
            current_end = oldest_time - 1
            time.sleep(0.1)  # Rate limit protection
        
        if not all_rates:
            print(f"Нет данных фандинга для {symbol}")
            continue
        
        # Создаём DataFrame
        df = pd.DataFrame(all_rates)[['symbol', 'fundingRate', 'fundingRateTimestamp']]
        df['fundingTime'] = pd.to_datetime(df['fundingRateTimestamp'], unit='ms')
        df['fundingRate'] = pd.to_numeric(df['fundingRate'])
        
        # Убираем дубликаты и фильтруем
        df = df.drop_duplicates('fundingTime').sort_values('fundingTime')
        df = df[(df['fundingTime'] >= pd.to_datetime(start_date)) & 
                (df['fundingTime'] <= pd.to_datetime(end_date))]
        
        results[symbol] = df
        print(f"Загружено {len(df)} выплат фандинга для {symbol}")

    if not results:
        return pd.DataFrame()

    # Берём первый символ (можно расширить под несколько)
    df_fund = results[symbols[0]].copy()
    
    # Создаём целевой hourly индекс
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    full_hourly_range = pd.date_range(start=start_dt.floor('H'), end=end_dt.ceil('H'), freq='H')

    # Создаём Series с реальными значениями фандинга
    fund_series = pd.Series(
        data=df_fund['fundingRate'].values,
        index=df_fund['fundingTime'],
        name=f'{col_name}_fund_h'
    ).drop_duplicates()

    # Пересоздаём Series с полным hourly индексом, заполняя NaN между выплатами
    fund_series = fund_series.reindex(full_hourly_range)
    
    # Заменяем NaN на 0 — потому что между выплатами фандинг = 0
    fund_series = fund_series.fillna(0.0)

    # Превращаем в DataFrame и настраиваем индекс
    fund_df = fund_series.to_frame()
    fund_df.index.name = 'time'

    print(f"Создан временной ряд с {len(fund_df)} часовыми точками (с нулевым фандингом между выплатами)")
    return fund_df


def get_future_price_bybit(symbol, interval='60', limit=1000, start_time=None, end_time=None, col_name='future'):
    """
    Получает исторические данные для фьючерса на Bybit с возможностью указать временной диапазон.
    
    :param symbol: Торговая пара (например 'SOLUSDT')
    :param interval: Интервал ('1', '3', '5', '15', '30', '60', '120', 'D' и т.д.)
    :param limit: Количество свечей за один запрос (макс. 1000)
    :param start_time: Начало периода (datetime или строка 'YYYY-MM-DD HH:MM:SS')
    :param end_time: Конец периода (по умолчанию — сейчас)
    :param col_name: Префикс для колонок (например 'sol', 'btc')
    :return: DataFrame с колонками [*_open, *_close, *_ret, *_ret_hedge]
    """
    BASE_URL = "https://api.bybit.com/v5/market/kline"
    
    # Конвертируем время
    def to_ms(dt):
        if isinstance(dt, str):
            dt = datetime.fromisoformat(dt.replace(" ", "T"))
        if isinstance(dt, datetime):
            return int(dt.timestamp() * 1000)
        return dt  # если уже timestamp

    params = {
        'category': 'linear',
        'symbol': symbol,
        'interval': interval,
        'limit': limit
    }

    if start_time is not None:
        params['start'] = to_ms(start_time)
    if end_time is not None:
        params['end'] = to_ms(end_time)

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()

        if data['retCode'] == 0:
            klines = data['result']['list']
            if not klines:
                return pd.DataFrame()

            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'
            ])

            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'turnover']
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
            df['timestamp'] = df['timestamp'].astype('int64')
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')

            # Формируем DataFrame с нужными колонками
            token = df[['datetime', 'open', 'high', 'low', 'close', 'volume', 'turnover']].copy()
            token.set_index('datetime', inplace=True)
            token.index.name = 'time'

            token[f'{col_name}_open'] = token['open']
            token[f'{col_name}_high'] = token['open']
            token[f'{col_name}_low'] = token['open']
            token[f'{col_name}_close'] = token['close']
            token[f'{col_name}_volume'] = token['volume']
            token[f'{col_name}_turnover'] = token['turnover']
            token.drop(columns=['open', 'high', 'low', 'close', 'volume', 'turnover'], inplace=True)

            return token.sort_index()
        else:
            print(f"Ошибка API: {data['retMsg']}")
            return pd.DataFrame()

    except Exception as e:
        print(f"Ошибка при запросе данных: {e}")
        return pd.DataFrame()
    

def get_future_data_bybit_full_period(symbol, interval='60', start_time=None, end_time=None, col_name='future'):
    """
    Загружает ВСЕ исторические данные по фьючерсу за указанный период.
    
    :param symbol: Торговая пара (например 'SOLUSDT')
    :param interval: Интервал ('1', '3', '5', '15', '30', '60', 'D' и т.д.)
    :param start_time: Начало периода (datetime или строка)
    :param end_time: Конец периода
    :param col_name: Префикс для колонок
    :return: DataFrame с полной историей
    """
    # Парсим время
    if isinstance(start_time, str):
        start_time = datetime.fromisoformat(start_time.replace(" ", "T"))
    if isinstance(end_time, str):
        end_time = datetime.fromisoformat(end_time.replace(" ", "T"))

    if not isinstance(start_time, datetime):
        raise ValueError("start_time должен быть datetime или строкой формата 'YYYY-MM-DD HH:MM:SS'")
    if end_time is None:
        end_time = datetime.now()
    elif not isinstance(end_time, datetime):
        raise ValueError("end_time должен быть datetime или None")

    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)

    all_dfs = []
    current_end = end_ms

    print(f"Загрузка фьючерсных данных для {symbol} с {start_time} по {end_time}, интервал={interval}")

    while True:
        df = get_future_price_bybit(
            symbol=symbol,
            interval=interval,
            limit=1000,
            start_time=start_ms,
            end_time=current_end,
            col_name=col_name
        )

        if df.empty:
            print("Данные не возвращены.")
            break

        # Фильтруем, чтобы не выйти за пределы start_time
        df = df[df.index >= start_time]
        if df.empty:
            print("Все данные раньше start_time — остановка.")
            break

        all_dfs.append(df)

        # Определяем самую раннюю точку
        earliest_timestamp_ms = int(df.index[0].timestamp() * 1000)
        if earliest_timestamp_ms <= start_ms:
            break

        current_end = earliest_timestamp_ms - 1

        print(f"Загружено до: {df.index[0]}")

        if len(all_dfs) > 10_000:
            print("Превышено максимальное количество итераций.")
            break

    # Объединение
    if all_dfs:
        full_df = pd.concat(all_dfs).sort_index()
        full_df = full_df[~full_df.index.duplicated(keep='first')]
        full_df = full_df[(full_df.index >= start_time) & (full_df.index <= end_time)]
        print(f"✅ Загружено {len(full_df)} свечей.")
        return full_df
    else:
        print("❌ Нет данных.")
        return pd.DataFrame()