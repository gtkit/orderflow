-- gormstore 参考 schema（reference only，非权威迁移）
--
-- ⚠ 本文件用途：仅作为新项目起步时的列名 / 类型 / 索引参考模板。
--   gormstore 不持有"标准订单 / 账单 / 流水模型"——业务方负责创建并维护
--   自己的表结构，本包不主动建表，也不提供版本化迁移。
--
-- 使用方式：
--   1. 复制本文件到业务工程的迁移目录（goose / golang-migrate / Atlas / GORM AutoMigrate
--      ……任选其一管理版本），按需重命名；
--   2. 根据 gormstore.Config 中实际配置的 OrderTable / BillTable / LogTable 改表名；
--   3. 根据 gormstore.ColumnMap 实际配置（或自定义 GORM Model 的 column tag）调整 orders
--      表的列名、类型、长度；
--   4. 自定义 BillWriter / LogStore 时，order_bills / order_logs 段落可以删除；
--   5. 业务订单表必备索引清单见 drivers/gormstore/doc.go 的"必备索引清单"章节，
--      务必核对，否则 fallback worker 会全表扫描。
--
-- 三段对应关系（默认配置下）：
--   - orders        ←  Config.OrderTable + ColumnMap 默认列名
--   - order_bills   ←  Config.BillTable + 默认 BillWriter (OrderBill struct)
--   - order_logs    ←  Config.LogTable + 默认 LogStore  (OrderLog struct)
--
-- 方言：MySQL 8.0+ / MariaDB 10.5+。其他方言（PostgreSQL / SQLite）需调整数据类型，
-- 但列名与索引语义保持一致。

-- =========================================
-- orders：订单主表（业务方自行掌握，本段仅作起步参考）
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

-- ⚠ 强烈建议：未注入 orderflow.Locker 时，必须依靠 DB 兜底"同 user 同 product 至多
--   一条 Pending"的语义。否则并发下单可能产生多条 Pending，破坏一码一单约束。
--
--   PostgreSQL 直接用部分唯一索引（推荐）：
--       CREATE UNIQUE INDEX uk_user_product_pending
--           ON orders (user_id, product_id) WHERE status = 0;
--
--   MySQL 不支持 partial index，可用生成列模拟：
--       ALTER TABLE orders ADD COLUMN pending_key BIGINT UNSIGNED
--           GENERATED ALWAYS AS (IF(status = 0, product_id, NULL)) STORED;
--       CREATE UNIQUE INDEX uk_user_pending_product ON orders (user_id, pending_key);
--
--   注入 Locker 时，DB 唯一约束作为二级防御仍然推荐保留——Locker 在 Redis 主从切换
--   等场景下有可能短暂失效（详见 rediscache.Locker 的"主从切换不安全"说明）。

-- =========================================
-- order_bills：账单表（默认 BillWriter 写入；自定义 BillWriter 时本段可删）
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
-- order_logs：状态流水表（默认 LogStore 写入；自定义 LogStore 时本段可删）
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
