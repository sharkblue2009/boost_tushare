import pickle

import lmdb
import numpy as np
from boost_tushare.xcdb.xcdb import force_bytes

LMDB_NAME = 'D:/Database/stock_db/TEST_LMDB'
key1 = force_bytes('abc')
rec1 = np.array([('123', 1.0, 2, 3.9, 5.6, '2002-10-31T02:30')],
                dtype='U3, f4, f4, f4, f4, U18').view(np.recarray)
# rec1 = np.array([(1, 1.0, 2, 3.9, 5.6, 8)],
#                 dtype='f4, f4, f4, f4, f4, f4').view(np.recarray)
rec1 = np.array([(1, 1.0, 2, 3.9, 5.6, 8)],
                dtype='f8, f8, f8, f8, f8, f8').view(np.recarray)
dbval1 = np.full((20,), rec1[0])
inval1 = pickle.dumps(dbval1)

key2 = force_bytes('def')
rec2 = np.array([('123', 1.0, 2, 3.9, 5.6, '2002-10-31T02:30')],
                 dtype='U3, f4, f4, f4, f4, U18')
# rec2 = np.array([('123', 1.0, 2, 3.9, 5.6, '2002-10-31T02:30')],
#                 dtype='O, f4, f4, f4, f4, O')
# rec2 = np.array([(1, 1.0, 2, 3.9, 5.6, 8)],
#                 dtype='f8, f8, f8, f8, f8, f8')
dbval2 = np.full((20,), rec2[0])
dbval2 = np.full((20, 6), 56789.1, dtype=np.dtype('f8'))
inval2 = pickle.dumps(dbval2)

############################
db_env = lmdb.open(LMDB_NAME, create=True, max_dbs=1000000, readonly=False, map_size=1 * 0x400000)
sdb = db_env.open_db(force_bytes('sdb_path'))

txn = db_env.begin(db=sdb, write=True, parent=None)
txn.put(key1, inval1)
txn.put(key2, inval2)
txn.commit()

txn = db_env.begin(db=sdb, write=True, parent=None)

val = txn.get(key1)
outval = pickle.loads(val)

import timeit
import pandas as pd
from boost_tushare.utils.xcutils import df_to_sarray, sarray_to_df

def get_val(k, tx):
    out = {}
    for n in range(100):
        val = tx.get(k)
        out[n] = pickle.loads(val)
    outval = np.concatenate(list(out.values()), axis=0)
    # outval = pd.DataFrame.from_records(outval)
    # outval = sarray_to_df(outval, None)
    return

sdb = db_env.open_db(force_bytes('sdb_path'))
txn = db_env.begin(db=sdb, write=True, parent=None)
print(timeit.Timer(lambda: get_val(key1, txn)).timeit(5000))
txn.commit()
