import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """
    Получает список товаров из кампании на Яндекс Маркете.

    Функция отправляет запрос к API Яндекс Маркета, используя предоставленные учетные данные,
    и возвращает данные о товарах, используя номер страницы для последовательной загрузки.

    Args:
        page (str): Номер страницы (или токен страницы) для продолжения загрузки списка.
        campaign_id (str): Идентификатор кампании на Яндекс Маркете.
        access_token (str): Токен доступа для авторизации запросов.

    Returns:
        dict: Словарь с информацией о товарах (ключи могут включать 'offerMappingEntries', 'paging' и др.).

    Пример корректного использования:
        products = get_product_list("", "12345", "abcdefg12345")

    Пример некорректного использования:
        products = get_product_list(None, None, None)  # Аргументы не могут быть None
    """

    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """
    Обновляет остатки товаров на Яндекс Маркете.

    Функция отправляет новые данные о наличии товаров (остатки) для указанных артикулов в кампании,
    используя предоставленные учетные данные.

    Args:
        stocks (list): Список словарей, каждый из которых описывает остаток товара.
        campaign_id (str): Идентификатор кампании на Яндекс Маркете.
        access_token (str): Токен доступа для авторизации запросов.

    Returns:
        dict: Ответ сервера о результате обновления остатков.

    Пример корректного использования:
        response = update_stocks(
            [{"sku": "1001", "warehouseId": "111", "items": [{"count": 10, "type": "FIT", "updatedAt": "2023-01-01T00:00:00Z"}]}],
            "12345",
            "abcdefg12345"
        )

    Пример некорректного использования:
        response = update_stocks("не список", "12345", "abcdefg12345")  # Аргумент stocks должен быть списком
    """

    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """
    Обновляет цены товаров на Яндекс Маркете.

    Функция отправляет новые цены для указанных товаров и обновляет стоимость в кампании.

    Args:
        prices (list): Список словарей с информацией о ценах товаров (например, "id" и "price").
        campaign_id (str): Идентификатор кампании на Яндекс Маркете.
        access_token (str): Токен доступа для авторизации запросов.

    Returns:
        dict: Ответ сервера о результате обновления цен.

    Пример корректного использования:
        response = update_price(
            [{"id": "1001", "price": {"value": 5990, "currencyId": "RUR"}}],
            "12345",
            "abcdefg12345"
        )

    Пример некорректного использования:
        response = update_price("не список", "12345", "abcdefg12345")  # Аргумент prices должен быть списком
    """
    
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """
    Получает список артикулов (shopSku) товаров, доступных в кампании на Яндекс Маркете.

    Функция выполняет постраничную загрузку товаров и формирует список их уникальных идентификаторов.

    Args:
        campaign_id (str): Идентификатор кампании на Яндекс Маркете.
        market_token (str): Токен доступа для авторизации запросов.

    Returns:
        list: Список строк, где каждая строка — это уникальный артикул товара (shopSku).

    Пример корректного использования:
        offer_ids = get_offer_ids("12345", "abcdefg12345")

    Пример некорректного использования:
        offer_ids = get_offer_ids("", "")  # Пустые данные для авторизации
    """

    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """
    Формирует список остатков для обновления на Яндекс Маркете.

    Функция сравнивает данные о наличии товаров из источника с уже имеющимися на Маркете,
    создавая список остатков для каждого товара.

    Args:
        watch_remnants (list): Список данных о товарах (цены, количество).
        offer_ids (list): Список артикулов товаров, уже присутствующих на Маркете.
        warehouse_id (str): Идентификатор склада, к которому привязаны остатки.

    Returns:
        list: Список словарей, содержащих информацию о остатках (sku, warehouseId, items).

    Пример корректного использования:
        stocks = create_stocks(watch_remnants, ["1001", "1002"], "111")

    Пример некорректного использования:
        stocks = create_stocks("не список", ["1001"], "111")  # watch_remnants должен быть списком
    """

    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Формирует список цен для обновления на Яндекс Маркете.

    Функция использует данные о товарах и их цене, чтобы сформировать список цен для отправки в кампанию.

    Args:
        watch_remnants (list): Список товаров с информацией о цене.
        offer_ids (list): Список артикулов товаров, для которых нужно сформировать цены.

    Returns:
        list: Список словарей с информацией о ценах (id и price).

    Пример корректного использования:
        prices = create_prices(watch_remnants, ["1001", "1002"])

    Пример некорректного использования:
        prices = create_prices(watch_remnants, "не список")  # offer_ids должен быть списком
    """

    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """
    Обновляет цены на товары в кампании Яндекс Маркета, отправляя их частями.

    Функция разбивает список цен на небольшие части и последовательно обновляет данные,
    чтобы цены на Маркете соответствовали актуальным данным.

    Args:
        watch_remnants (list): Список товаров с информацией о ценах.
        campaign_id (str): Идентификатор кампании на Яндекс Маркете.
        market_token (str): Токен доступа для авторизации запросов.

    Returns:
        list: Список цен, которые были обновлены.

    Пример корректного использования (в асинхронном контексте):
        updated_prices = await upload_prices(watch_remnants, "12345", "abcdefg12345")

    Пример некорректного использования:
        updated_prices = upload_prices(watch_remnants, "12345", "abcdefg12345")  
        # Нельзя просто вызвать без await
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """
    Обновляет остатки товаров в кампании Яндекс Маркета, отправляя их частями.

    Функция разбивает список остатков на небольшие части и поочередно обновляет данные,
    чтобы остатки на Маркете соответствовали актуальным данным.

    Args:
        watch_remnants (list): Список товаров с информацией о наличии.
        campaign_id (str): Идентификатор кампании на Яндекс Маркете.
        market_token (str): Токен доступа для авторизации запросов.
        warehouse_id (str): Идентификатор склада, к которому привязаны остатки.

    Returns:
        tuple: Кортеж из двух списков:
               - Первый список: товары с ненулевым остатком.
               - Второй список: все отправленные остатки.

    Пример корректного использования (в асинхронном контексте):
        not_empty, all_stocks = await upload_stocks(watch_remnants, "12345", "abcdefg12345", "111")

    Пример некорректного использования:
        not_empty, all_stocks = upload_stocks(watch_remnants, "12345", "abcdefg12345", "111")
        # Нельзя просто вызвать без await
    """

    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
