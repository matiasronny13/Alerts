import logging
import asyncio
import json
import pathlib
from ..classes.logic_price_alert import PriceAlertLogic

import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    with open(f"{pathlib.Path(__file__).parent}/config.json", 'r') as f:    #TODO: replace with function app local json setting
            input_config = json.load(f)

    processor = PriceAlertLogic(input_config)
    asyncio.run(processor.run())

    return func.HttpResponse(f"This HTTP triggered function executed successfully.", status_code=200)
        