import json
import random
import asyncio
import aiohttp
import questionary
import pandas as pd
from web3 import Web3
from sys import stderr
from loguru import logger
from web3.eth import AsyncEth
from eth_account.messages import encode_defunct

delay_wallets = [1, 2]
min_balance = 0.00015  # Check min balance ETH and print if balance < min_balance

logger.remove()
logger.add(stderr,
           format="<lm>{time:HH:mm:ss}</lm> | <level>{level}</level> | <blue>{function}:{line}</blue> "
                  "| <lw>{message}</lw>")

abi = json.loads('[{"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"balanceOf",'
                 '"outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view",'
                 '"type":"function"}]')


class Worker:
    def __init__(self, private_key: str, proxy: str, number_acc: int, cex_address: str = None) -> None:
        self.proxy: str = f"http://{proxy}" if proxy is not None else None
        self.private_key = private_key
        self.cex_address, self.client, self.id = cex_address, None, number_acc
        self.rpc: str = 'https://rpc.ankr.com/arbitrum'
        self.scan: str = 'https://arbiscan.io/tx/'
        self.w3 = Web3(
            provider=Web3.AsyncHTTPProvider(endpoint_uri=self.rpc),
            modules={"eth": AsyncEth},
            middlewares=[])
        if proxy is not None:
            self.web3 = Web3(
                provider=Web3.AsyncHTTPProvider(endpoint_uri=self.rpc,
                                                request_kwargs={"proxy": self.proxy}),
                modules={"eth": AsyncEth},
                middlewares=[])

        self.account = self.w3.eth.account.from_key(private_key=private_key)
        self.contract_erc20 = self.w3.eth.contract(
            address=Web3.to_checksum_address('0x1cd9a56c8c2ea913c70319a44da75e99255aa46f'), abi=abi)

    async def send_tx(self, data: str, to: str) -> bool:
        try:

            latest_block = await self.w3.eth.get_block("latest")
            base_fee_per_gas = latest_block["baseFeePerGas"]
            priority_fee = self.w3.to_wei(0.5, 'gwei')
            max_fee_per_gas = base_fee_per_gas + priority_fee

            tx_data = {
                "chainId": 42161,
                "from": self.account.address,
                "to": self.w3.to_checksum_address(to),
                "nonce": await self.w3.eth.get_transaction_count(self.account.address),
                "value": 0,
                "data": data,
                "maxFeePerGas": max_fee_per_gas,
                "maxPriorityFeePerGas": priority_fee,
                "gas": await self.w3.eth.estimate_gas({
                    "from": self.account.address,
                    "to": self.w3.to_checksum_address(to),
                    "value": 0,
                    "data": data,
                }),
            }

            signed_txn = self.w3.eth.account.sign_transaction(tx_data, self.private_key)
            tx_hash = await self.w3.eth.send_raw_transaction(signed_txn.raw_transaction)
            logger.info(f'#{self.id} | send txs...')
            tx_hash = self.w3.to_hex(tx_hash)
            await asyncio.sleep(6)

            receipt = await self.w3.eth.get_transaction_receipt(tx_hash)
            if receipt['status'] == 1:
                logger.success(f'#{self.id} | Success send tx | hash: {tx_hash}')
                return True

            else:
                logger.error(f'#{self.id} | Failed send tx | hash: {tx_hash}')
                return False

        except Exception as e:
            if '0xe4ca4c0b' in str(e):
                logger.info(f'#{self.id} | Already claimed...')
                exel.loc[(self.id - 1), 'Status'] = 'Claimed'
                exel.to_excel('accounts_data.xlsx', header=True, index=False)
            else:
                logger.error(f'#{self.id} | {e}')

    async def claim(self):
        async with aiohttp.ClientSession() as client:
            self.client: aiohttp.ClientSession = client

            msg = encode_defunct(text='Orbiter Airdrop')
            text_signature = self.w3.eth.account.sign_message(msg, private_key=self.private_key)
            self.client.headers['token'] = f'0x{text_signature.signature.hex()}'

            response: aiohttp.ClientResponse = await self.client.post(
                url=f'https://airdrop-api.orbiter.finance/airdrop/snapshot',
                proxy=self.proxy
            )

            if response.status == 201:
                response_json: dict = await response.json()
                if response_json['code'] == 0:
                    if response_json['result'] is None:
                        logger.info(f"#{self.id} | Not eligible :(")
                        exel.loc[(self.id - 1), 'Status'] = 'Not eligible'
                        exel.to_excel('accounts_data.xlsx', header=True, index=False)
                        return

                    logger.success(f"#{self.id} | Claim {float(response_json['result']['proof'][0]['amount'])} OBT")
                    proof = ''.join(i[2:] for i in response_json["result"]['proof'][0]['data'])
                    amount = response_json["result"]['proof'][0]['amount']

                    data = f'0xfa5c4e99' \
                           f'071cbb2ff029ddaf4b691745b2ba185cbe9ca2f5fa9e7358bada8fbdce764291' \
                           f'{hex(int(amount.replace(".", "")))[2:].zfill(64)}' \
                           f'0000000000000000000000000000000000000000000000000000000000000060' \
                           f'0000000000000000000000000000000000000000000000000000000000000013' \
                           f'{proof}'

                    if await Worker.send_tx(self, data=data, to='0x13dFDd3a9B39323F228Daf73B62C23F7017E4679'):
                        exel.loc[(self.id - 1), 'Status'] = 'Claimed'
                        exel.to_excel('accounts_data.xlsx', header=True, index=False)

    async def send_to_cex(self):
        async def get_balance():
            return await self.contract_erc20.functions.balanceOf(self.account.address).call()

        if self.cex_address is None:
            logger.info(f'#{self.id} | Cex address not found')
            return

        balance = await get_balance()
        if balance == 0:
            logger.info(f'#{self.id} | Balance is 0 OBT')
            return

        data = f'0xa9059cbb' \
               f'{self.cex_address.strip()[2:].zfill(64)}' \
               f'{hex(balance)[2:].zfill(64)}'

        await Worker.send_tx(self, data=data, to='0x1cd9a56c8c2ea913c70319a44da75e99255aa46f')


async def start(account: list, id_acc: int, semaphore) -> None:
    async with semaphore:
        acc = Worker(private_key=account[0].strip(), proxy=account[1], number_acc=id_acc, cex_address=account[2])
        try:
            if choice.__contains__('Check min balance'):
                balance = await acc.w3.eth.get_balance(acc.account.address)
                # print(acc.w3.from_wei(balance, 'ether'))
                if acc.w3.from_wei(balance, 'ether') < min_balance:
                    print(acc.account.address)
            if choice.__contains__('Claim OBT'):
                await acc.claim()
            if choice.__contains__('Send to CEX'):
                if choice.__contains__('Claim OBT -> Send to CEX'):
                    logger.info(f'Sleep 2-4 sec...')
                    await asyncio.sleep(random.randint(2, 4))
                await acc.send_to_cex()
        except Exception as e:
            logger.error(f'{id_acc} {acc.account.address} Failed: {str(e)}')

        sleep_time = random.randint(delay_wallets[0], delay_wallets[1])
        if sleep_time != 0 and not choice.__contains__('Check min balance'):
            logger.info(f'Sleep {sleep_time} sec...')
            await asyncio.sleep(sleep_time)


async def main() -> None:
    semaphore: asyncio.Semaphore = asyncio.Semaphore(1)

    tasks: list[asyncio.Task] = [
        asyncio.create_task(coro=start(account=account, id_acc=idx, semaphore=semaphore))
        for idx, account in enumerate(accounts, start=1)
    ]
    await asyncio.gather(*tasks)
    print()


if __name__ == '__main__':
    with open('accounts_data.xlsx', 'rb') as file:
        exel = pd.read_excel(file)
    exel = exel.astype({'Status': 'str'})

    choice = questionary.select(
        "Select work mode:",
        choices=[
            "Claim OBT",
            "Send to CEX",
            "Check min balance ETH",
            "Claim OBT -> Send to CEX",
            "Exit",
        ]
    ).ask()

    if 'Exit' in choice:
        exit()

    accounts: list[list] = [
        [
            row["Private Key"],
            row["Proxy"] if isinstance(row["Proxy"], str) else None,
            row["Cex Address"] if isinstance(row["Cex Address"], str) else None
        ]
        for index, row in exel.iterrows()
    ]

    logger.info(f'My channel: https://t.me/CryptoMindYep')
    logger.info(f'Total wallets: {len(accounts)}\n')
    asyncio.run(main())

    logger.info('The work completed')
    logger.info('Thx for donat: 0x5AfFeb5fcD283816ab4e926F380F9D0CBBA04d0e')
