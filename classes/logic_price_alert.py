import logging
import telegram
import gspread
import httpx
import asyncio
import json
import pandas as pd
from decimal import Decimal
import time


class PriceAlertLogic:
    def __init__(self, config):
        self.config = config
        self.bot, self.chat_id = self.connect_telegram_bot()
        self.gsheet = self.connect_google_spreadsheet().sheet1

    async def run(self):
        logging.info("PriceAlertLogic.run()")
        self.df = self.get_google_alert_dataframe()
        if self.df is not None:
            await self.scan()
        else:
            await self.send(f'\U00002757 ERROR: {self.config["google_spreadsheet"]["file_name"]} spreadsheet does not have header columns')
 
    async def create_query_task(self, client, symbols, quote_type):
        url = "https://query1.finance.yahoo.com/v1/finance/screener?crumb=LKFH6LnWCm2&lang=en-US&region=US&formatted=true&corsDomain=finance.yahoo.com"
        header = {
            "cookie": "B=6upmke5hvjjuh&b=3&s=59; A1=d=AQABBNHP-WMCEKMQh8VhePnVCOUHhXHUZm8FEgEBCAF-P2RrZFpOb2UB_eMBAAcI0c_5Y3HUZm8&S=AQAAAg_MngjDSx_G5gJ6ncC4l7o; A3=d=AQABBNHP-WMCEKMQh8VhePnVCOUHhXHUZm8FEgEBCAF-P2RrZFpOb2UB_eMBAAcI0c_5Y3HUZm8&S=AQAAAg_MngjDSx_G5gJ6ncC4l7o; GUC=AQEBCAFkP35ka0IirQUd; A1S=d=AQABBNHP-WMCEKMQh8VhePnVCOUHhXHUZm8FEgEBCAF-P2RrZFpOb2UB_eMBAAcI0c_5Y3HUZm8&S=AQAAAg_MngjDSx_G5gJ6ncC4l7o&j=WORLD; PRF=t%3DGC%253DF%252BZN%253DF%252BNGT%253DF%252BSGD%253DX%252BA%252BGOOG%252BGFQ23.CME%252B%255EJKSE%252BEC%26newChartbetateaser%3D1; cmp=t=1681981788&j=0&u=1---",
            "content-type": "application/json"
        }
        payload = {
            "size": 100,
            "offset": 0,
            "sortField": "ticker",
            "sortType": "DESC",
            "quoteType": quote_type,
            "topOperator": "AND",
            "query": {
                "operator": "AND",
                "operands": [
                    {
                        "operator": "or",
                        "operands": []
                    }
                ]
            },
            "userId": "",
            "userIdType": "guid"
        }

        for symbol in symbols:
            payload["query"]["operands"][0]["operands"].append({
                    "operator": "EQ",
                    "operands": [
                        "ticker",
                        symbol
                    ]
                }) 

        logging.info(f"Downloading {quote_type}")
        response = await client.post(url=url, headers=header, data=json.dumps(payload))
        if response is not None:
            json_response = response.json()
            return [{"symbol": a["symbol"], "price": a["regularMarketPrice"]["raw"]} for a in json_response["finance"]["result"][0]["quotes"]]

        return None

    async def get_quote_with_failover(self, symbols):
        result = pd.DataFrame()
        retry = 0
        while retry < 3:
            try:    
                if retry > 0: 
                    time.sleep(1)
                
                response = httpx.get(f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={','.join(symbols)}&fields=regularMarketPrice")
                if response.status_code == 200:
                    json_response = response.json()
                    result_list = [{"symbol": a["symbol"], "price": a["regularMarketPrice"]} for a in json_response["quoteResponse"]["result"]]
                    return pd.DataFrame(result_list)
            except:
                retry += 1                
                if retry > 2:
                    await self.send(f"\U00002757 ERROR: 3 attempts have failed reading quotes {self.df.symbol.to_list()}")

        return result

    async def scan(self):
        try:
            quotes = await self.get_quote_with_failover(self.df.symbol.drop_duplicates().str.upper().to_list())

            if not quotes.empty:
                new_sheet = [["symbol", "operator", "value"]]
                for index, item in self.df.iterrows():
                    is_triggered = await self.validate(quotes[quotes.symbol == item.symbol].iloc[0], item)
                    if not is_triggered:
                        new_sheet.append(item.to_list())

                self.gsheet.clear()
                self.gsheet.insert_rows(new_sheet)
        except Exception as ex:
            await self.send(f"\U00002757 ERROR: reading quotes {self.df.symbol.to_list()}")
            logging.exception(ex)

    async def validate(self, quote, item):
        result = False
        try:
            if quote is not None:
                quote_price = quote.price
                target_price = Decimal(item.value)
                match item.operator:
                    case "gt":
                        if quote_price >= target_price:
                            await self.send(f"\U00002714 {item.symbol} is greater or equal than {target_price}")
                            result = True
                    case "lt":
                        if quote_price <= target_price:
                            await self.send(f"\U00002714 {item.symbol} is less or equal than {target_price}")
                            result = True
                    case _:
                        await self.send(f"\U00002757 ERROR: quote {item.symbol} has invalid operator {item.operator}")            
                        result = True
        except Exception as ex: 
            await self.send(f"\U00002757 ERROR: validating quote {item.symbol}")
            logging.exception(ex)          
        return result

    def connect_google_spreadsheet(self):
        gc = gspread.service_account_from_dict(self.config["google_spreadsheet"]["credential"])
        return gc.open(self.config["google_spreadsheet"]["file_name"])

    def connect_telegram_bot(self):
        bot_token = self.config["bot"]["telegram_token"]
        chat_id = httpx.get(f"https://api.telegram.org/bot{bot_token}/getUpdates").json()['result'][0]['message']['from']['id']
        return telegram.Bot(bot_token), chat_id

    def get_google_alert_dataframe(self) -> pd.DataFrame:
        rows = self.gsheet.get()
        if len(rows) > 0:
            result = pd.DataFrame(rows[1:], columns=rows[0])
            result.symbol = result.symbol.str.upper()
            return result
        else: 
            return None        

    async def send(self, msg):
        await self.bot.sendMessage(chat_id=self.chat_id, text=msg)

