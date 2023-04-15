import json
from typing import Any, Optional
from dataclasses import dataclass


class GracefulExit(SystemExit):
    code = 1


def raise_graceful_exit():
    raise GracefulExit(1)


def read_json(path: str) -> Any:
    with open(path, "r") as f:
        return json.load(f)


def parse_symbols_dict(
    symbols_map: dict[str, dict[str, dict[str, str]]], exch: str, key: str = "ws"
) -> tuple[dict[str, str], dict[str, str]]:
    """returns a tuple of dicts (symbol to exchange symbol, exchange symbol to symbol)"""
    sym2ws = {k: v[exch][key] for k, v in symbols_map.items() if v.get(exch)}
    return sym2ws, {v: k for k, v in sym2ws.items()}


class SymbolsMeta:
    """
    Global symbols object (for mapping symbols to exchange format).
    This is a singleton class to avoid loading symbols.json at each
    new instance.
    """

    _instance = None
    _symbols_path = "symbols.json"
    _exchanges = ("binance", "kraken")

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.__init()
        return cls._instance

    def __init(self):
        self.sym2ws, self.ws2sym = {}, {}
        self.sym2rest, self.rest2sym = {}, {}
        self.__load_symbols()

    def __load_symbols(self):
        self.symbols_map: dict[str, dict[str, Any]] = read_json(self._symbols_path)
        for exch in self._exchanges:
            self.sym2ws[exch], self.ws2sym[exch] = parse_symbols_dict(
                self.symbols_map, exch, "ws"
            )
            self.sym2rest[exch], self.rest2sym[exch] = parse_symbols_dict(
                self.symbols_map, exch, "rest"
            )

    def sym2ws_sym(self, exchange: str, symbol: str) -> str:
        """map symbol to ws symbol"""
        return self.sym2ws[exchange][symbol]

    def ws_sym2sym(self, exchange: str, symbol: str) -> str:
        """map ws symbol to symbol"""
        return self.ws2sym[exchange][symbol]

    def sym2rest_sym(self, exchange: str, symbol: str) -> str:
        """map symbol to rest symbol"""
        return self.sym2rest[exchange][symbol]

    def rest_sym2sym(self, exchange: str, symbol: str) -> str:
        """map rest symbol to symbol"""
        return self.rest2sym[exchange][symbol]
