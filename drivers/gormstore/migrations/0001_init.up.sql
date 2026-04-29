-- gormstore 标准建表脚本（版本 0001）
--
-- 三张表对应 gormstore.Store 默认配置：
--   - orders        ←  Config.OrderTable + ColumnMap 默认列名
--   - order_bills   ←  Config.BillTable + 默认 BillWriter (OrderBill struct)
--   - order_logs    ←  Config.LogTable + 默认 LogStore  (OrderLog struct)
--
-- 修改 ColumnMap 列名时，本脚本对应字段需同步调整；自定义 BillWriter / LogStore
-- 时本脚本仅作 orders 表参考，bills / logs 表由实现方自行建表。
--
-- 方言：MySQL 8.0+ / MariaDB 10.5+。其他方言（PostgreSQL / SQLite）需调整数据类型，
-- 但列名与索引语义保持一致。

-- =========================================
-- orders：订单主表
-- =========================================
CREATE TABLE IF NOT EXISTS orders (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    order_no        VARCHAR(64)     NOT NULL,
    order_token     VARCHAR(64)     NOT NULL,
    user_id         BIGINT          NOT NULL,
    status          TINYINT         NOT NULL DEFAULT 0,         -- 0/10/20/30/40/50 见 orderflow.OrderStatus
    product_id      BIGINT UNSIGNED NOT NULL,
    product_type    TINYINT         NOT NULL DEFAULT 0,
    product_title   VARCHAR(255)    NOT NULL DEFAULT '',
    pay_method      TINYINT         NOT NULL DEFAULT 0,
    pay_amount      BIGINT          NOT NULL DEFAULT 0,
    original_price  BIGINT          NOT NULL DEFAULT 0,
    trade_no        VARCHAR(128)    NOT NULL DEFAULT '',
    paid_at         DATETIME        NULL,
    delivered_at    DATETIME        NULL,
    expire_at       DATETIME        NOT NULL,
    updated_at      DATETIME        NOT NULL,
    channel_id      BIGINT          NOT NULL DEFAULT 0,         -- 启用 ColumnMap.ChannelID 时使用
    PRIMARY KEY (id),
    UNIQUE KEY uk_order_no    (order_no),
    UNIQUE KEY uk_order_token (order_token),
    KEY idx_user_id           (user_id),
    KEY idx_status_expire     (status, expire_at),              -- FindExpiredPending
    KEY idx_status_paid       (status, paid_at)                 -- FindPaidUndelivered
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================
-- order_bills：账单表（默认 BillWriter 写入）
-- =========================================
CREATE TABLE IF NOT EXISTS order_bills (
    id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    user_id          BIGINT          NOT NULL,
    order_no         VARCHAR(64)     NOT NULL,
    trade_no         VARCHAR(128)    NOT NULL DEFAULT '',
    product_id       BIGINT UNSIGNED NOT NULL,
    product_type     TINYINT         NOT NULL DEFAULT 0,
    product_title    VARCHAR(255)    NOT NULL,
    original_price   BIGINT          NOT NULL,
    discount_amount  BIGINT          NOT NULL DEFAULT 0,
    pay_amount       BIGINT          NOT NULL,
    pay_method       TINYINT         NOT NULL DEFAULT 0,
    pay_channel      VARCHAR(32)     NOT NULL DEFAULT '',
    channel_id       BIGINT          NOT NULL DEFAULT 0,
    paid_at          DATETIME        NOT NULL,
    created_at       DATETIME        NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_order_no (order_no),
    KEY idx_user_id        (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================
-- order_logs：状态流水表（默认 LogStore 写入）
-- =========================================
CREATE TABLE IF NOT EXISTS order_logs (
    id           BIGINT          NOT NULL AUTO_INCREMENT,
    created_at   DATETIME        NOT NULL,
    order_id     BIGINT UNSIGNED NOT NULL DEFAULT 0,
    order_no     VARCHAR(64)     NOT NULL,
    user_id      BIGINT          NOT NULL,
    from_status  TINYINT         NOT NULL,
    to_status    TINYINT         NOT NULL,
    actor        VARCHAR(64)     NOT NULL DEFAULT 'system',
    remark       VARCHAR(512)    NOT NULL DEFAULT '',
    PRIMARY KEY (id),
    KEY idx_order_id   (order_id),
    KEY idx_order_no   (order_no, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
