-- 反向 DROP（与 0001_init.up.sql 对应）
-- 注意：DROP 顺序与 up 相反，避免外键场景下的依赖问题（即使本初始版本未声明外键）。

DROP TABLE IF EXISTS order_logs;
DROP TABLE IF EXISTS order_bills;
DROP TABLE IF EXISTS orders;
