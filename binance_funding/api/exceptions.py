class BinanceRequestException(Exception):
    pass


class BinanceAPIException(Exception):
    def __init__(self, status: int, body: str):
        self.status = status
        self.body = body
        super().__init__(f"Binance API error {status}: {body}")
