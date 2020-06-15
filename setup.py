# ! /usr/bin/env python
# -*- coding: utf-8 -*-
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
import setuptools

setup(
    name='boost_tushare',  # 包的名字
    author='sharkblue2009',  # 作者
    version='0.1.0',  # 版本号
    license='MIT',

    description='boost_tushare',  # 描述
    long_description='''tushare access accelerator by using local cache database''',
    author_email='aaa@aaa.com',  # 你的邮箱**
    url='https://github.com/sharkblue2009/boost_tushare',
    # 包内需要引用的文件夹
    # packages=setuptools.find_packages(exclude=['url2io',]),
    packages=["boost_tushare"],
    # keywords='NLP,tokenizing,Chinese word segementation',
    # package_dir={'jieba':'jieba'},
    # package_data={'jieba':['*.*','finalseg/*','analyse/*','posseg/*']},

    # 依赖包
    install_requires=[
        'numpy >= 1.18.1',
        'pandas >= 0.23.4,<0.24.0',
        "tushare >= 1.2.54",
        'lmdb >=0.98',
    ],
    classifiers=[
        # 'Development Status :: 4 - Beta',
        # 'Operating System :: Microsoft'  # 你的操作系统  OS Independent      Microsoft
        'Intended Audience :: Developers',
        # 'License :: OSI Approved :: MIT License',
        # 'License :: OSI Approved :: BSD License',  # BSD认证
        'Programming Language :: Python',  # 支持的语言
        'Programming Language :: Python :: 3',  # python版本 。。。
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries'
    ],
    zip_safe=True,
)
