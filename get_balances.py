from concurrent.futures import ThreadPoolExecutor
import time

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests

from web3 import Web3

API_URL = 'https://api.polygonscan.com/api'
API_KEY = '6YZ353E9TU6TMTWYPQTMIG17D7156A534M'

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
    },
    {
        'constant': True,
        'inputs': [],
        'name': 'name',
        'outputs': [{'name': '', 'type': 'string'}],
        'type': 'function'
    },
    {
        'constant': True,
        'inputs': [],
        'name': 'totalSupply',
        'outputs': [{'name': '', 'type': 'uint256'}],
        'type': 'function'
    },
]

TOKEN_CONTRACT = web3.eth.contract(address=TOKEN_ADDRESS, abi=TOKEN_ABI)
TOKEN_SYMBOL = TOKEN_CONTRACT.functions.symbol().call()
# TOKEN_DECIMALS = TOKEN_CONTRACT.functions.decimals().call()

app = FastAPI()


def format_balance(balance):
    """Приводит в читабельный формат."""
    if isinstance(balance, (int, float)):
        return f'{balance:.6f} {TOKEN_SYMBOL}'
    return balance


def get_address_info(address_data: dict):
    """Получение информации о балансе и последней транзакции для адресов."""
    address_balances = []
    for address, data in address_data.items():
        balance = TOKEN_CONTRACT.functions.balanceOf(Web3.to_checksum_address(address)).call()
        formatted_balance = format_balance(balance)
        last_transaction_date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(data['last_tx']))
        address_balances.append((address, formatted_balance, last_transaction_date))
    return address_balances


class AdressBatchRequest(BaseModel):
    addresses: list


class TokenInfoRequest(BaseModel):
    address: str


@app.get('/get_balance')
def get_balance(address: str):
    """Функция для получения баланса токена на адресе через HTTP GET запрос."""
    if not web3.is_address(address):
        raise HTTPException(status_code=400, detail='Некорректный адрес')

    checksum_address = Web3.to_checksum_address(address)

    try:
        balance = TOKEN_CONTRACT.functions.balanceOf(checksum_address).call()
        formatted_balance = format_balance(balance)
        return {'address': address,
                'balance': formatted_balance,
                'symbol': TOKEN_SYMBOL,
                }
    except Exception as e:
        return address, f'Ошибка: {str(e)}'


@app.post('/get_balance_batch')
def get_balances_batch(request: AdressBatchRequest):
    """Функция для обработки списка адресов."""
    addresses = request.addresses

    invalid_addresses = [address for address in addresses if not web3.is_address(address)]

    if invalid_addresses:
        raise HTTPException(status_code=400, detail=f"Некорректные адреса: {', '.join(invalid_addresses)}")

    with ThreadPoolExecutor() as executor:
        # Параллельное выполнение запросов
        results = list(executor.map(get_balance, addresses))

    return {'balances': results}


def get_recent_transactions():
    """Получает последние транзакции для заданного контракта."""
    params = {
        'module': 'account',
        'action': 'tokentx',
        'contractaddress': TOKEN_ADDRESS,
        'sort': 'desc',
        'apikey': API_KEY
    }

    response = requests.get(API_URL, params=params)
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail='Ошибка на сервере')

    try:
        data = response.json()
    except ValueError as e:
        raise HTTPException(status_code=500, detail=f'Ошибка при декодировании JSON: {e}')

    if data['status'] != 1:
        raise HTTPException(status_code=404, detail='Нет данных о транзакциях для этого контракта.')

    return data.get('result', [])


@app.get('/get_top_with_transactions')
def get_top_with_transactions(n: int):
    """Возвращает топ N адресов с последней транзакцией, отсортированные по балансу."""
    transactions = get_recent_transactions()

    if not transactions:
        raise HTTPException(status_code=403,
                            detail='Не удалось получить транзакции')

    # Собираем уникальные адреса и их последние транзакции
    address_data = {}
    for tx in transactions:
        address_from = tx['from']
        address_to = tx['to']
        timestamp = int(tx['timeStamp'])

        if address_from not in address_data:
            address_data[address_from] = {
                'balance': 0,
                'last_tx': timestamp,
            }
        else:
            address_data[address_from]['last_tx'] = max(address_data[address_from]['last_tx'], timestamp)

        if address_to not in address_data:
            address_data[address_to] = {
                'balance': 0,
                'last_tx': timestamp,
            }
        else:
            address_data[address_to]['last_tx'] = max(address_data[address_to]['last_tx'], timestamp)

    address_balances = get_address_info(address_data)

    sorted_address_balances = sorted(address_balances, key=lambda x: x[1], reverse=True)

    return {'top': sorted_address_balances[:n]}


@app.get('/get_token_info')
def get_token_info(address: str):
    """Функция для получения информации о токене."""
    if not web3.is_address(address):
        raise HTTPException(status_code=400, detail="Некорректный адрес контракта")

    try:
        symbol = TOKEN_CONTRACT.functions.symbol().call()
        name = TOKEN_CONTRACT.functions.name().call()
        total_supply = TOKEN_CONTRACT.functions.totalSupply().call()

        total_supply = format_balance(total_supply)

        return {'symbol': symbol, 'name': name, 'totalSupply': total_supply}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Ошибка при получении данных о токене: {str(e)}')
