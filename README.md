# cntus 
local cache for tushare(https://tushare.pro)data, database is LMDB/LevelDB
Tushare数据本地快速缓冲池

# 特性
- LMDB/LevelDB本地数据库
- 支持历史行情数据(日线、分钟线)，财务数据，基础数据的缓存
- 数据更新方式
    - 本地缓存，需要时下载支持。
    - 集中下载支持，daily update.
- 本地数据完整性检查。
    + 根据停牌数据，交易日历，对日线行情、DailyInfo数据做完整性检查
        - 单月日线行情长度+停牌日期长度>=当月交易日历长度
        - 快速判断: 当月日线行情头尾，必须和交易日历头尾对齐，否则需要做完整性检查
    + 根据停牌数据做分钟线行情完整性检查
    + 分钟线、日线行情的交叉验证
- 多线程更新。
- 性能优化：加载2015年至2020全A股日线数据，约20秒。
- 流控， 支持tushare的访问速度控制

# Bug Report
欢迎提交bug report, 会尽力做及时修复

# API接口
## class TusXcReader
### Data Read APIs
    数据读取API
- Read mode flag: 
    + IOFLAG.READ_XC: Cache mode reading, load from cache database first, if miss, load from network
    + IOFLAG.READ_NETDB: From network(tushare.pro), then flush to cache database.
    + IOFLAG.READ_DBONLY: From database only

- get_trade_cal
- get_index_info
- get_stock_info
- get_price_daily
- get_price_minute
- get_stock_dailyinfo

### Data Update APIs
    数据集中更新API
- update_suspend_d
- update_price_daily
- update_price_minute

### Data erase APIs
    数据擦除API


# 部署
- Developed by Python 3.6
| Tushare | 1.2.54 |
| Pandas  | 0.23.4 |
| Numpy  | 1.18.1 |

- git clone ...cntus
    put the package path to you python env site-packages/easy_install.pth file.
- create _passwd.py file in top level source code folder, put your tushare token in the file as below.
    TUS_TOKEN = '928374091274092174092740971203487120934870129734801024'
- tushare对部分API依据权限做了访问控制和流控，请自行根据需求调整访问速度（文件: proloader.py）。
    self.ts_token.block_consume(10) # 10代表每分钟不超过500次。

## Dependent libraries
- Tushare
    pip install tushare

- lmdb
    pip install lmdb

- leveldb 安装
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
    
    - 构建 plyvel
        确认编译器可以访问 .lib 和 leveldb 头文件，然后将它们复制到 MSVC 构建工具目录下：
        1.  从 vcpkg\installed\x64-windows\include\
            复制到 
            e:\Program Files\Microsoft Visual Studio\2017\Community\VC\Tools\MSVC\14.12.25827\include
            
            从 vcpkg\installed\x64-windows\lib\libleveldb.lib 
            复制到 e:\Program Files\Microsoft Visual Studio\2017\Community\VC\Tools\MSVC\14.12.25827\lib\x64\leveldb.lib
            "C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\include"
            "C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\lib\amd64"
        
        2. 克隆库并进入所需版本，安装 cython，从 C++ 文件构建 python 扩展，最后安装 plyvel，如下所示：
            git clone https://github.com/wbolster/plyvel
            cd plyvel
            git checkout e3887a5fae5d7b8414eac4c185c1e3b0cebbdba8
            pip install cython
            cython --cplus --fast-fail --annotate plyvel/_plyvel.pyx
            python setup.py build_ext --inplace --force
            python setup.py install
        
