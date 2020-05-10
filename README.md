# cntus
local cache for tushare(https://tushare.pro)data, database is LMDB/LevelDB

# 特性
- LMDB/LevelDB本地数据库
- 支持历史行情数据(日线、分钟线)，财务数据，基础数据
- 本地缓存，需要时下载。
- 集中下载支持，daily update.
- 本地数据完整性检查。
- 多线程更新。
- 性能优化：加载2015年至2020全A股日线数据，约20秒。

# API
## class TusXcReader
### Read APIs
- Read mode flag: 
    + IOFLAG.READ_XC: Cache mode reading, load from DB first, if miss, load from network
    + IOFLAG.READ_NETDB: From network(tushare.pro), then flush to DB.
    + IOFLAG.READ_DBONLY: From database only

- get_trade_cal
- get_index_info
- get_stock_info

### Update APIs
- update_suspend_d
- update_price_daily
- update_price_minute

### Database erase APIs

# 部署
- Developed by Python 3.6
| Tushare | 1.2.54 |
| Pandas  | 0.23.4 |
| Numpy  | 1.18.1 |

## Tushare
- pip install tushare

## lmdb 安装
- pip install lmdb

# leveldb 安装
- https://github.com/google/leveldb
git clone --recurse-submodules https://github.com/google/leveldb.git

- Building for Windows
1. Install cmake first
2. First generate the Visual Studio 2017 project/solution files:
    mkdir build
    cd build
    cmake -G "Visual Studio 15" ..
The default default will build for x86. For 64-bit run:
    cmake -G "Visual Studio 15 Win64" ..
To compile the Windows solution from the command-line:
    devenv /build Debug leveldb.sln
or open leveldb.sln in Visual Studio and build from within.

- Build using VS2015:
  cmake -G "Visual Studio 14 Win64" ..

## 构建 plyvel
确认编译器可以访问 .lib 和 leveldb 头文件，然后将它们复制到 MSVC 构建工具目录下：
1. 从 vcpkg\installed\x64-windows\include\
复制到 e:\Program Files\Microsoft Visual Studio\2017\Community\VC\Tools\MSVC\14.12.25827\include
从 vcpkg\installed\x64-windows\lib\libleveldb.lib 
复制到 e:\Program Files\Microsoft Visual Studio\2017\Community\VC\Tools\MSVC\14.12.25827\lib\x64\leveldb.lib
"C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\include"
"C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\lib\amd64"

2.然后克隆存储库并进入所需版本，安装 cython，从 C++ 文件构建 python 扩展，最后安装 plyvel，如下所示：

git clone https://github.com/wbolster/plyvel
cd plyvel
git checkout e3887a5fae5d7b8414eac4c185c1e3b0cebbdba8
pip install cython
cython --cplus --fast-fail --annotate plyvel/_plyvel.pyx
python setup.py build_ext --inplace --force
python setup.py install
