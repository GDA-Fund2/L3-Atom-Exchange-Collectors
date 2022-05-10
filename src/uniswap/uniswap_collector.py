import re
from python_graphql_client import GraphqlClient
import time
import json
import asyncio
import websockets
import dotenv

# Start the client with an endpoint.
client = GraphqlClient(endpoint="https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2")
conf = dotenv.dotenv_values(".env")

res_template = {
    "block_no": -1,
    "total_pairs": -1,
    "total_transactions": -1,
    "total_USD_volume": -1,
    "total_USD_liquidity": -1,
    "fees": -1,
    "no_burns": 0,
    "no_mints": 0,
    "no_swaps": 0,
    "amount_burned_USD": 0,
    "amount_minted_USD": 0,
    "amount_swapped_USD": 0,
}


def query_univ2(request, params=None):

    response = client.execute(query=request, params=params)
    return response

def get_global_stats(result):
    request = \
    """{
        uniswapFactory(id: "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"){
            pairCount
            totalVolumeUSD
            totalLiquidityUSD
            txCount
        }
    }"""
    response = query_univ2(request)
    result["total_pairs"] = response["data"]["uniswapFactory"]["pairCount"]
    result["total_transactions"] = int(response["data"]["uniswapFactory"]["txCount"])
    result["total_USD_volume"] = float(response["data"]["uniswapFactory"]["totalVolumeUSD"])
    result["total_USD_liquidity"] = float(response["data"]["uniswapFactory"]["totalLiquidityUSD"])

def get_transaction_amounts(block_no, result):
    request = """
    {
        transactions(where: {blockNumber: %d}) {
            blockNumber
            mints {
                amountUSD
            }
            burns {
                amountUSD
            }
            swaps {
                amountUSD
            }
        }
    }
    """ % (block_no)
    response = query_univ2(request)
    while "errors" in response or not response["data"]["transactions"]:
        time.sleep(0.1)
        print(json.dumps(response, indent=4))
        response = query_univ2(request)
    mints = 0
    burns = 0
    swaps = 0
    mint_amounts = 0
    burn_amounts = 0
    swap_amounts = 0
    for transaction in response["data"]["transactions"]:
        for mint in transaction["mints"]:
            mint_amounts += float(mint["amountUSD"])
            mints += 1
        for burn in transaction["burns"]:
            burn_amounts += float(burn["amountUSD"])
            burns += 1
        for swap in transaction["swaps"]:
            swap_amounts += float(swap["amountUSD"])
            swaps += 1
    result["amount_minted_USD"] = mint_amounts
    result["amount_burned_USD"] = burn_amounts
    result["amount_swapped_USD"] = swap_amounts
    result["no_mints"] = mints
    result["no_burns"] = burns
    result["no_swaps"] = swaps

def get_eth_usdt_swaps():
    request = \
        """{
            swaps(orderBy: timestamp, orderDirection: desc, where:
                { pair: "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc" }
            )   {
                    pair {
                        token0 {
                            symbol
                        }
                        token1 {
                            symbol
                        }
                    }
                    amount0In
                    amount0Out
                    amount1In
                    amount1Out
                    amountUSD
                    to
                    timestamp
                }
            }"""
    response = query_univ2(request)

def get_100_most_liquid_pairs():
    request = """
    {pairs(first: 100, orderBy: reserveUSD, orderDirection: desc){
        id
        reserveUSD
        reserveETH
        totalSupply
        token0Price
        token1Price
        reserve0
        reserve1
        token0{id, symbol, name, tradeVolume, totalLiquidity}
        token1{id, symbol, name, tradeVolume, totalLiquidity}}}"""
    response = query_univ2(request)

def get_metrics(block_no, result, last_transaction_count):
    result["block_no"] = block_no
    get_global_stats(result)
    if last_transaction_count > 0 and last_transaction_count < result["total_transactions"]:
        get_transaction_amounts(block_no, result)

async def main():
    last_transaction_count = 0
    async with websockets.connect(conf["INFURA_WS_ENDPOINT"]) as ws:
        await ws.send(json.dumps({"jsonrpc":"2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}))
        await ws.recv()
        while True:
            block = await ws.recv()
            print("New block!")
            start = time.time()
            block = json.loads(block)
            block_no = int(block["params"]["result"]["number"], 16)
            result = res_template.copy()
            get_metrics(block_no, result, last_transaction_count)
            print(json.dumps(result, indent=4))
            last_transaction_count = result["total_transactions"]
            end = time.time()
            print("Time: %f" % (end - start))

if __name__ == "__main__":
    asyncio.run(main())