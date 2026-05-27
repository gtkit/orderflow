#!/usr/bin/env bash
# modules.sh —— 多 module 仓库共享模块清单。
#
# 本文件只定义 module 元数据，供 lint / audit / release 脚本 source。
# 新增 driver module 时只需要在这里追加一行。

MODULES=(
    "orderflow|.|v"
    "gormstore|drivers/gormstore|drivers/gormstore/v"
    "paymgrgw|drivers/paymgrgw|drivers/paymgrgw/v"
    "rediscache|drivers/rediscache|drivers/rediscache/v"
    "rediszq|drivers/rediszq|drivers/rediszq/v"
)

DRIVER_MODULES=(
    "drivers/gormstore"
    "drivers/paymgrgw"
    "drivers/rediscache"
    "drivers/rediszq"
)
