from pycoin.encoding import wif_to_secret_exponent
from pycoin.tx.pay_to import build_hash160_lookup

class LazySecretExponentDB(object):
    def __init__(self, wif_iterable, secret_exponent_db_cache, allowable_wif_prefixes):
        self.wif_iterable = iter(wif_iterable)
        self.secret_exponent_db_cache = secret_exponent_db_cache
        self.allowable_wif_prefixes = allowable_wif_prefixes

    def get(self, v):
        if v in self.secret_exponent_db_cache:
            return self.secret_exponent_db_cache[v]
        for wif in self.wif_iterable:
            secret_exponent = wif_to_secret_exponent(wif, self.allowable_wif_prefixes)
            d = build_hash160_lookup([secret_exponent])
            self.secret_exponent_db_cache.update(d)
            if v in self.secret_exponent_db_cache:
                return self.secret_exponent_db_cache[v]
        self.wif_iterable = []
        return None
