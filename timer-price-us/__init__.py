import datetime
import logging
import asyncio
import json
import pathlib
from ..classes.logic_price_alert import PriceAlertLogic

import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    
    with open(f"{pathlib.Path(__file__).parent}/config.json", 'r') as f:    #TODO: replace with function app local json setting
            input_config = json.load(f)

    processor = PriceAlertLogic(input_config)
    asyncio.run(processor.run())

