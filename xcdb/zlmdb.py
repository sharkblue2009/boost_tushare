import lmdb
from .xcdb import *
from logbook import Logger

log = Logger('lmdb')

LMDB_NAME = 'D:/Database/stock_db/TusDBv2'


class XcLMDB(XCacheDB):
    """
    Database for caching.
    """

    def __init__(self, name, readonly=True, **kwargs):
        global DBS_OPENED
        self.name = name
        log.info('LMDB version: {}'.format(lmdb.version()))
        if not self.name in DBS_OPENED.keys():
            log.info('Open DB: {}'.format(self.name))
            DBS_OPENED[self.name] = lmdb.open(
                self.name, create=True, max_dbs=1000000, readonly=readonly, map_size=64 * 0x40000000, **kwargs)
        else:
            log.info("Already Opened, use exist one")
            # raise FileExistsError('Already opened')

        self.env = DBS_OPENED[self.name]
        self.show()

    def close(self):
        if isinstance(self.env, lmdb.Environment):
            self.env.close()
            self.env = None
        global DBS_OPENED
        if self.name in DBS_OPENED.keys():
            print('Close DB: {}'.format(self.name))
            DBS_OPENED.pop(self.name)

    def __del__(self):
        self.close()

    def show(self):
        """
        db used_space = psize *(leaf_pages + branch_pages + overflow_pages)
        :return:
        """
        info = self.env.info()
        log.info('DB Size is: {}MB'.format(info['map_size'] // 0x100000))
        # stat = self.env.stat()
        # log.info(stat)
        # info = self.env.max_key_size()
        # log.info(info)

    def detail(self):
        info = self.env.info()
        db_map_size = info['map_size'] // 0x100000
        log.info('DB Total Size is: {}MB'.format(db_map_size))
        
        txn = self.env.begin(db=None, write=False, parent=None)
        total_size = 0
        st = self.env.stat()
        total_size += st['psize'] * (st['leaf_pages'] + st['branch_pages'] + st['overflow_pages'])

        # get sub_db stats
        all_stats = []
        cursor = txn.cursor()
        first = cursor.first()
        if first:
            while True:
                k = cursor.key()
                db = self.env.open_db(k, txn=txn, create=False)
                try:
                    stat = txn.stat(db)
                    all_stats.append(stat)
                except:
                    print(k)

                if not cursor.next():
                    break

        cursor.close()
        txn.commit()

        for st in all_stats:
            total_size += st['psize'] * (st['leaf_pages'] + st['branch_pages'] + st['overflow_pages'])

        total_size = total_size // 0x100000
        log.info('Used space: {}MB, Percentage: {}%'.format(total_size, total_size*100//db_map_size))

    @staticmethod
    def _get_sdb(master_db, sdb_path: str):
        cur_db = master_db.open_db(force_bytes(sdb_path), create=True)
        return cur_db

    def get_sdb(self, sdb_path: str):
        try:
            # log.info('>>SDB>>{}'.format(sdb_path))
            sdb = self._get_sdb(self.env, sdb_path)
        except Exception as e:
            print(e)
            print('sdb_path not exist: {}'.format(sdb_path))
            raise ValueError('create sdb error')

        return sdb


class XcLMDBAccessor(XcAccessor):
    """
    对于序列保存的数据，由于原始不存在的值不存储(如股票停牌，则没有对应时间的价格值)，
    因此我们必须保证数据库中，头和尾之间的数据是完整的。
    这样才能和原始数据对齐， 从而能判断key(head<key<tail)对应的数据是否存在。
    例如价格数据，如果所取得key没有对应的value,且key位于head和tail之间，那么可判断该价格数据不存在（股票停牌）
    Note: read-write transactions may be nested.
        write transition begin-commit block can not insert other transition without nesting.

    Lmdb Txn begin and commit need to be paired. so every API called Accessor [Init] must call [DEL/Commit]
                before next [Init].

    db = self.facc(TusSdbs.SDB_CALENDAR.value, GENERAL_OBJ_META)
    ...
    ...
    del db/ db.commit()

    a Readonly transaction block can be reentrant by multi-thread, but a writeable txn-block cannot.
    """

    def __init__(self, master_db: XcLMDB, sdb: str, metadata=None, readonly=False):
        self.master = master_db
        self.db = master_db.get_sdb(sdb)
        self.metadata = metadata
        write_mode = not readonly
        self.txn = self.master.env.begin(db=self.db, write=write_mode, parent=None)
        return

    def stat(self):
        """
        :return: Dict
            psize
                Size of a database page in bytes.
            depth
                Height of the B-tree.
            branch_pages
                Number of internal (non-leaf) pages.
            leaf_pages
                Number of leaf pages.
            overflow_pages
                Number of overflow pages.
            entries
                Number of data items.
        """
        return self.txn.stat()

    def __del__(self):
        """"""
        try:
            self.txn.commit()
        except:
            pass

    def load(self, key, raw_mode=False):
        """

        :param key:
        :param raw_mode: override value type for output.
        :return:
        """
        # self.txn = self.master.env.begin(db=self.db, write=True)
        key = self.to_db_key(key)
        if key:
            val = self.txn.get(key)
            if val:
                return self.to_val_out(val, raw_mode)
        return None

    def save(self, key, val, raw_mode=False):
        """"""
        # self.txn = self.master.env.begin(db=self.db, write=True)
        if val is None:
            return
        key = self.to_db_key(key)
        dbval, appval = self.to_val_in(val, raw_mode)
        if key and dbval:
            self.txn.put(key, dbval)
        return appval

    def remove(self, key):
        """"""
        key = self.to_db_key(key)
        if key:
            self.txn.delete(key)

    def commit(self):
        self.txn.commit()

    def load_range(self, kstart, kend, raw_mode):
        """"""
        out = {}
        with self.txn.cursor(self.db) as cur:
            bstr = cur.set_range(force_bytes(kstart))
            if bstr:
                while True:
                    k, v = cur.item()
                    sk = force_string(k)
                    out[sk] = self.to_val_out(v, raw_mode)
                    if sk >= kend:
                        break
                    vld = cur.next()
                    if not vld:
                        break
            cur.close()

        return out

    def drop(self):
        """
        Drop the DB.
        :return:
        """
        return self.txn.drop(self.db, delete=True)
