from datetime import datetime
from copy import deepcopy
import json

import pandas

from apis import IEXAPI


class IEXRequestExecutor:

    def __init__(self, path, tickers):
        self.path = path
        self.tickers = tickers

    @staticmethod
    def _epoch_to_datetime(epoch):
        datetime_output = datetime.fromtimestamp(epoch / 1000)
        return datetime_output.strftime("%Y-%m-%d %H:%M:%S")

    def _update_quote_body_attributes(self, quote_body):
        epoch_datetime = deepcopy(quote_body['latestUpdate'])
        quote_body['latestUpdate'] = self._epoch_to_datetime(epoch_datetime)
        for key, item in quote_body.items():
            quote_body[key] = [item]
        return quote_body

    @staticmethod
    def _write_output(path, ticker, ticker_response_frame):
        pricing_file_path = deepcopy(path).format(ticker=ticker)
        ticker_response_frame.to_csv(pricing_file_path, index=False, header=False)

    def __call__(self):
        api = IEXAPI()
        response = api.get_latest_price(self.tickers)
        deserialized_response = response.json()
        restructured_response = {
            ticker: self._update_quote_body_attributes(quote['quote'])
            for ticker, quote in deserialized_response.items()
        }
        for ticker in restructured_response:
            ticker_response_frame = pandas.DataFrame(restructured_response[ticker])
            self._write_output(self.path, ticker, ticker_response_frame)
