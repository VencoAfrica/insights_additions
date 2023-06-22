from insights.cache_utils import get_or_set_cache, make_digest
from insights.insights.doctype.insights_data_source.sources.frappe_db import FrappeDB


class FrappeDBTest(FrappeDB):
    def connect(self):
        return self.engine.connect()


def is_frappe_db(db_params):
    def _is_frappe_db():
        try:
            db = FrappeDBTest(**db_params)
            return db.test_connection()
        except BaseException:
            return False

    key = make_digest("is_frappe_db", db_params)
    return get_or_set_cache(key, _is_frappe_db, expiry=None)
