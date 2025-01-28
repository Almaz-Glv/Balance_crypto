from concurrent.futures import ThreadPoolExecutor
import requests
import time

from web3 import Web3
import pandas as pd


POLYGON_RPC_URL = 'https://polygon-rpc.com'
web3 = Web3(Web3.HTTPProvider(POLYGON_RPC_URL))

if not web3.is_connected():
    raise Exception('Не удалось подключиться к сети Polygon')

token_address_lower = '0x1a9b54a3075119f1546c52ca0940551a6ce5d2d0'

TOKEN_ADDRESS = Web3.to_checksum_address(token_address_lower)

TOKEN_ABI = [
    {
        'constant': True,
        'inputs': [{'name': '_owner', 'type': 'address'}],
        'name': 'balanceOf',
        'outputs': [{'name': 'balance', 'type': 'uint256'}],
        'type': 'function'
    },
    {
        'constant': True,
        'inputs': [],
        'name': 'decimals',
        'outputs': [{'name': '', 'type': 'uint8'}],
        'type': 'function'
    },
    {
        'constant': True,
        'inputs': [],
        'name': 'symbol',
        'outputs': [{'name': '', 'type': 'string'}],
        'type': 'function'
    }
]


token_contract = web3.eth.contract(address=TOKEN_ADDRESS, abi=TOKEN_ABI)
token_symbol = token_contract.functions.symbol().call()
token_decimals = token_contract.functions.decimals().call()


def measure_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Функция '{func.__name__}' выполнилась за \
              {end_time - start_time:.6f} секунд")
        return result
    return wrapper


# Функция для получения баланса токена на адресе
def get_token_balance(address):
    if not web3.is_address(address):
        return 'Некорректный адрес'
    checksum_address = Web3.to_checksum_address(address)

    try:
        balance = token_contract.functions.balanceOf(checksum_address).call()
        formatted_balance = balance / (10 ** token_decimals)
        return address, formatted_balance
    except Exception as e:
        return address, f'Ошибка: {str(e)}'


# Функция для обработки списка адресов
def get_balances_for_addresses(addresses):
    results = []
    with ThreadPoolExecutor() as executor:
        # Параллельное выполнение запросов
        futures = [executor.submit(get_token_balance, address)
                   for address in addresses]
        for future in futures:
            results.append(future.result())
    return results


def format_balance(balance):
    if isinstance(balance, (int, float)):
        return f"{balance:.6f} {token_symbol}"
    return balance


@measure_time
def get_balances_for_specific_addresses():
    user_input = input('Введите адреса через запятую: ')
    addresses = [address.strip() for address in user_input.split(',')]
    balances = get_balances_for_addresses(addresses)
    df = pd.DataFrame(balances, columns=['Address', 'Balance'])
    df['Balance'] = df['Balance'].apply(format_balance)
    print('\nБалансы токенов:')
    print(df.to_string(index=False))


@measure_time
def get_top_10_addresses():
    api_url = 'https://api.polygonscan.com/api'
    api_key = '6YZ353E9TU6TMTWYPQTMIG17D7156A534M'
    contract_address = '0x1a9b54a3075119f1546c52ca0940551a6ce5d2d0'

    # Получаем список транзакций токена
    params = {
        "module": "account",
        "action": "tokentx",
        "contractaddress": contract_address,
        "sort": "desc",
        "apikey": api_key
    }

    response = requests.get(api_url, params=params)
    # Проверка статуса ответа
    if response.status_code != 200:
        print(f"Ошибка: Сервер вернул код {response.status_code}")
        return

    # Проверка содержимого ответа
    if not response.text:
        print("Ошибка: Пустой ответ от сервера")
        return

    try:
        data = response.json()
    except ValueError as e:
        print(f"Ошибка при декодировании JSON: {e}")
        return

    # Собираем уникальные адреса из транзакций
    addresses = set()
    for tx in data['result']:
        addresses.add(tx["from"])
        addresses.add(tx["to"])

    balances = get_balances_for_addresses(addresses)

    sorted_balances = sorted(balances, key=lambda x: x[1] if isinstance (x[1], (int, float)) else -1, reverse=True)
    top_10 = sorted_balances[:10]

    df = pd.DataFrame(top_10, columns=["Address", "Balance"])
    df["Balance"] = df["Balance"].apply(format_balance)
    print("\nТоп-10 адресов по балансу:")
    print(df.to_string(index=False))


# Меню выбора
def show_menu():
    print('Выберите режим работы:')
    print('1. Получить балансы для конкретных адресов')
    print('2. Получить топ-10 адресов по балансу')
    choice = input('Введите номер режима (1 или 2): ')
    return choice


# Основная логика программы
def main():
    choice = show_menu()
    if choice == "1":
        get_balances_for_specific_addresses()
    elif choice == "2":
        get_top_10_addresses()
    else:
        print("Неверный выбор. Пожалуйста, введите 1 или 2.")


if __name__ == "__main__":
    main()
