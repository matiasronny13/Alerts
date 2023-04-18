import logging
import telegram
import gspread
import httpx
import pandas as pd
import pandas_datareader as pdr
from decimal import Decimal


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
 
    async def scan(self):
        try:
            quotes = pdr.get_quote_yahoo(self.df.symbol.to_list())

            if not quotes.empty:
                new_sheet = [["symbol", "operator", "value"]]
                for index, item in self.df.iterrows():
                    is_triggered = await self.validate(quotes.loc[item.symbol], item)
                    if not is_triggered:
                        new_sheet.append(item.to_list())

                self.gsheet.clear()
                self.gsheet.insert_rows(new_sheet)
        except Exception as ex:
            await self.send(f"\U00002757 ERROR: reading quotes {self.df.symbol.to_list()}")
            logging.exception(ex)

    async def validate(self, quote, item):
        try:
            if quote is not None:
                quote_price = quote.price
                target_price = Decimal(item.value)
                match item.operator:
                    case "gt":
                        if quote_price > target_price:
                            await self.send(f"\U00002714 {item.symbol} is greater than {target_price}")
                            return True
                    case "lt":
                        if quote_price < target_price:
                            await self.send(f"\U00002714 {item.symbol} is less than {target_price}")
                            return True
                    case _:
                        await self.send(f"\U00002757 ERROR: quote {item.symbol} has invalid operator {item.operator}")            
                        return True
        except Exception as ex: 
            await self.send(f"\U00002757 ERROR: validating quote {item.symbol}")
            logging.exception(ex)          
        return False

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
            return pd.DataFrame(rows[1:], columns=rows[0])
        else: 
            return None        

    async def send(self, msg):
        await self.bot.sendMessage(chat_id=self.chat_id, text=msg)

