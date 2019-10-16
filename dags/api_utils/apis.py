import os
import requests
from airflow import models

class IEXAPI:

    TOKEN = models.Variable.get('IEX_API_KEY')
    PROTOCOL = 'https'
    DOMAIN = 'cloud.iexapis.com'
    PATH = """stable/stock/market/batch?symbols={tickers}&types=quote&filter=latestPrice,latestUpdate,symbol&token="""

    def _url(self, tickers_delimited):
        url = ''.join([self.PROTOCOL, '://', self.DOMAIN, '/', self.PATH, self.TOKEN])
        return url.format(tickers=tickers_delimited)

    def get_latest_price(self, tickers):
        tickers_comma_delimited = ','.join(tickers)
        response = requests.get(self._url(tickers_comma_delimited))
        if response.status_code != 200:
            raise ApiError('GET /stable/ {}'.format(response.status_code))
        return response
