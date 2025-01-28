from concurrent.futures import ThreadPoolExecutor
import requests
import time

from web3 import Web3
import pandas as pd


POLYGON_RPC_URL = 'https://polygon-rpc.com'
web3 = Web3(Web3.HTTPProvider(POLYGON_RPC_URL))

if not web3.is_connected():
    raise Exception('Не удалось подключиться к сети Polygon')

TOKEN_ADDRESS_LOWER = '0x1a9b54a3075119f1546c52ca0940551a6ce5d2d0'

TOKEN_ADDRESS = Web3.to_checksum_address(TOKEN_ADDRESS_LOWER)

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
    """Декоратор измерящий время выполнение функции."""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Функция '{func.__name__}' выполнилась за \
              {end_time - start_time:.6f} секунд")
        return result
    return wrapper


def get_token_balance(address):
    """Функция для получения баланса токена на адресе."""
    if not web3.is_address(address):
        return 'Некорректный адрес'
    checksum_address = Web3.to_checksum_address(address)

    try:
        balance = token_contract.functions.balanceOf(checksum_address).call()
        formatted_balance = balance / (10 ** token_decimals)
        return address, formatted_balance
    except Exception as e:
        return address, f'Ошибка: {str(e)}'


def get_balances_for_addresses(addresses):
    """Функция для обработки списка адресов."""
    results = []
    with ThreadPoolExecutor() as executor:
        # Параллельное выполнение запросов
        futures = [executor.submit(get_token_balance, address)
                   for address in addresses]
        for future in futures:
            results.append(future.result())
    return results


def format_balance(balance):
    """Приводит в читабельный формат."""
    if isinstance(balance, (int, float)):
        return f'{balance:.6f} {token_symbol}'
    return balance


@measure_time
def get_balances_for_specific_addresses():
    """Возвращает баланс введенных пользователем адресов."""
    user_input = input('Введите адреса через запятую: ')
    addresses = [address.strip() for address in user_input.split(',')]
    balances = get_balances_for_addresses(addresses)
    df = pd.DataFrame(balances, columns=['Address', 'Balance'])
    df['Balance'] = df['Balance'].apply(format_balance)
    print('\nБалансы токенов:')
    print(df.to_string(index=False))


@measure_time
def get_top_10_addresses():
    """Возвращает топ 10 по балансу адресов."""
    api_url = 'https://api.polygonscan.com/api'
    api_key = '6YZ353E9TU6TMTWYPQTMIG17D7156A534M'
    contract_address = '0x1a9b54a3075119f1546c52ca0940551a6ce5d2d0'

    # Получаем список транзакций токена
    params = {
        'module': 'account',
        'action': 'tokentx',
        'contractaddress': contract_address,
        'sort': 'desc',
        'apikey': api_key
    }

    response = requests.get(api_url, params=params)
    # Проверка статуса ответа
    if response.status_code != 200:
        print(f'Ошибка: Сервер вернул код {response.status_code}')
        return

    # Проверка содержимого ответа
    if not response.text:
        print('Ошибка: Пустой ответ от сервера')
        return

    try:
        data = response.json()
    except ValueError as e:
        print(f'Ошибка при декодировании JSON: {e}')
        return

    # Собираем уникальные адреса и их последние транзакции
    address_data = {}
    for tx in data['result']:
        address = tx['from']
        timestamp = int(tx['timeStamp'])
        address_data[address] = max(address_data.get(address, 0), timestamp)

        address = tx['to']
        timestamp = int(tx['timeStamp'])
        address_data[address] = max(address_data.get(address, 0), timestamp)

    balances = get_balances_for_addresses(address_data.keys())

    balances_with_dates = [
        (
            addr,
            bal,
            time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(address_data[addr]))
        )
        for addr, bal in balances
        if isinstance(bal, (int, float))
    ]

    sorted_balances = sorted(balances_with_dates, key=lambda x: x[1], reverse=True)
    top_10 = sorted_balances[:10]

    df = pd.DataFrame(top_10, columns=['Address', 'Balance', 'Last Transaction'])
    df['Balance'] = df['Balance'].apply(format_balance)
    print('\nТоп-10 адресов по балансу с датами последних транзакций:')
    print(df.to_string(index=False))


def show_menu():
    """Меню выбора."""
    print('Выберите режим работы:')
    print('1. Получить балансы для конкретных адресов')
    print('2. Получить топ-10 адресов по балансу c датами транзакций')
    choice = input('Введите номер режима (1 или 2): ')
    return choice


def main():
    """Основная логика программы."""
    choice = show_menu()
    if choice == '1':
        get_balances_for_specific_addresses()
    elif choice == '2':
        get_top_10_addresses()
    else:
        print('Неверный выбор. Пожалуйста, введите 1 или 2.')


if __name__ == '__main__':
    main()
