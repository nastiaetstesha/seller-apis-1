import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """
    Получает список товаров из магазина на Ozon.

    Функция отправляет запрос к API магазина, используя предоставленные учетные данные и позицию в списке,
    и возвращает данные о товарах.

    Args:
        last_id (str): Последний идентификатор товара, с которого нужно продолжить загрузку.
        client_id (str): Идентификатор клиента (продавца).
        seller_token (str): Ключ для авторизации в магазине.

    Returns:
        dict: Словарь с информацией о товарах (список и сопутствующие данные).

    Пример корректного использования:
        products = get_product_list("", "123456", "abcdefg12345")

    Пример некорректного использования:
        products = get_product_list(None, None, None)  
    """

    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """
    Получает список артикулов (offer_id) всех товаров в магазине на Ozon.

    Функция перебирает все товары из магазина и возвращает их коды-артикулы.

    Args:
        client_id (str): Идентификатор клиента (продавца).
        seller_token (str): Ключ для авторизации в магазине.

    Returns:
        list: Список строк, каждая строка – это артикул товара.

    Пример корректного использования:
        offer_ids = get_offer_ids("123456", "abcdefg12345")

    Пример некорректного использования:
        offer_ids = get_offer_ids("", "")  # Пустые данные для авторизации
    """
    
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """
    Обновляет цены товаров в магазине на Ozon.

    Функция отправляет список новых цен для определенных товаров.  
    Каждый элемент списка должен содержать данные о товаре и его новой цене.

    Args:
        prices (list): Список словарей с обновленными ценами.
        client_id (str): Идентификатор клиента (продавца).
        seller_token (str): Ключ для авторизации в магазине.

    Returns:
        dict: Ответ от сервера с результатом обновления цен.

    Пример корректного использования:
        response = update_price(
            [{"offer_id": "1001", "price": "5990", "old_price": "0"}],
            "123456",
            "abcdefg12345"
        )

    Пример некорректного использования:
        response = update_price("не список", "123456", "abcdefg12345")  # Аргумент prices не является списком
    """

    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """
    Обновляет остатки товаров (наличие) в магазине на Ozon.

    Функция отправляет список остатков по товарам, чтобы магазин знал, сколько их в наличии.

    Args:
        stocks (list): Список словарей с данными об остатках, где каждый словарь
                       должен содержать "offer_id" и "stock".
        client_id (str): Идентификатор клиента (продавца).
        seller_token (str): Ключ для авторизации в магазине.

    Returns:
        dict: Ответ от сервера с результатом обновления остатков.

    Пример корректного использования:
        response = update_stocks(
            [{"offer_id": "1001", "stock": 10}],
            "123456",
            "abcdefg12345"
        )

    Пример некорректного использования:
        response = update_stocks("не список", "123456", "abcdefg12345")  # Аргумент stocks не является списком
    """

    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """
    Скачивает и подготавливает данные об остатках с сайта Casio.

    Функция загружает архив с сайта, извлекает файл с остатками,
    читает его и возвращает список товаров с их наличием и ценами.

    Returns:
        list: Список словарей, где каждый словарь – это данные по одному товару.

    Пример корректного использования:
        watch_remnants = download_stock()
    """

    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """
    Формирует список остатков для обновления в магазине на Ozon.

    Функция сопоставляет полученные от Casio данные о товарах с тем, что уже есть в магазине,
    и создает итоговый список, где для каждого товара указано актуальное количество.

    Args:
        watch_remnants (list): Список словарей с данными о товарах (наличие, коды).
        offer_ids (list): Список артикулов товаров, уже имеющихся на Ozon.

    Returns:
        list: Список словарей с полями "offer_id" и "stock" для каждого товара.

    Пример корректного использования:
        stocks = create_stocks(watch_remnants, ["1001", "1002", "1003"])

    Пример некорректного использования:
        stocks = create_stocks("не список", ["1001", "1002", "1003"])  # Аргумент watch_remnants не является списком
    """

    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Формирует список цен для обновления в магазине на Ozon.

    Функция анализирует данные из Casio и создает список, где для каждого товара указана актуальная цена.

    Args:
        watch_remnants (list): Список словарей с данными о товарах (цены, коды).
        offer_ids (list): Список артикулов товаров, уже имеющихся на Ozon.

    Returns:
        list: Список словарей с информацией о ценах для каждого товара
              (ключи: "offer_id", "price", "old_price" и др.).

    Пример корректного использования:
        prices = create_prices(watch_remnants, ["1001", "1002", "1003"])

    Пример некорректного использования:
        prices = create_prices(watch_remnants, "не список")  # Аргумент offer_ids должен быть списком
    """

    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """
    Преобразует цену из формата с символами и пробелами в чистое число.

    Функция удаляет лишние символы и возвращает цену в виде строки, состоящей только из цифр.

    Args:
        price (str): Цена в формате с лишними символами (например, "5'990.00 руб.").

    Returns:
        str: Цена, очищенная от лишних знаков (например, "5990").

    Пример корректного использования:
        new_price = price_conversion("5'990.00 руб.")  # вернёт "5990"

    Пример некорректного использования:
        new_price = price_conversion((59, 90))  # Аргумент должен быть строкой
    """

    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    Делит список на части по n элементов в каждой.

    Функция-генератор: при каждом вызове возвращает следующую часть списка,
    пока элементы не закончатся.

    Args:
        lst (list): Исходный список для разбиения.
        n (int): Размер каждой части.

    Returns:
        generator: Генератор, возвращающий по очереди фрагменты списка.

    Пример корректного использования:
        for chunk in divide([1, 2, 3, 4, 5], 2):
            print(chunk)  # Будет выводить [1,2], потом [3,4], потом [5]

    Пример некорректного использования:
        for chunk in divide("строка", 2):  # Вместо списка передана строка
            print(chunk)
    """

    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """
    Обновляет цены товаров на Ozon, отправляя их партиями.

    Функция подготавливает список актуальных цен и отправляет их частями, 
    чтобы обновить цены в магазине.

    Args:
        watch_remnants (list): Список данных о товарах (содержит цены).
        client_id (str): Идентификатор клиента (продавца).
        seller_token (str): Ключ для авторизации в магазине.

    Returns:
        list: Список обновлённых цен (те же данные, что были отправлены).

    Пример корректного использования (в асинхронном контексте):
        prices = await upload_prices(watch_remnants, "123456", "abcdefg12345")

    Пример некорректного использования:
        prices = upload_prices(watch_remnants, "123456", "abcdefg12345")  
        # Нельзя просто вызвать без await 
    """

    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """
    Обновляет остатки товаров на Ozon, отправляя их партиями.

    Функция подготавливает список остатков и отправляет их частями, 
    чтобы обновить данные о наличии товаров в магазине.

    Args:
        watch_remnants (list): Список данных о товарах (наличие).
        client_id (str): Идентификатор клиента (продавца).
        seller_token (str): Ключ для авторизации в магазине.

    Returns:
        tuple: Кортеж из двух списков:
            - Первый список содержит товары, у которых остаток не ноль.
            - Второй список содержит все отправленные остатки.

    Пример корректного использования (в асинхронном контексте):
        not_empty, all_stocks = await upload_stocks(watch_remnants, "123456", "abcdefg12345")

    Пример некорректного использования:
        not_empty, all_stocks = upload_stocks(watch_remnants, "123456", "abcdefg12345")  
        # Нельзя вызвать без await 
    """

    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
