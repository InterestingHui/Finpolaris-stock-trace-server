create table stock_min_cache
(
    id             int auto_increment
        primary key,
    stock_code     varchar(20)                        not null comment '股票代码（含后缀）',
    trade_datetime datetime                           not null comment '交易时间（精确到分钟）',
    open_price     decimal(10, 3)                     null comment '开盘价',
    close_price    decimal(10, 3)                     null comment '收盘价',
    high_price     decimal(10, 3)                     null comment '最高价',
    low_price      decimal(10, 3)                     null comment '最低价',
    volume         bigint                             null comment '成交量（股）',
    amount         decimal(20, 2)                     null comment '成交额（元）',
    updated_at     datetime default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP comment '更新时间',
    constraint uk_stock_datetime
        unique (stock_code, trade_datetime)
)
    comment '股票分钟级行情缓存表';

create index idx_stock_code
    on stock_min_cache (stock_code);

create index idx_trade_datetime
    on stock_min_cache (trade_datetime);

alter table trade_logs
    modify intended_date datetime not null comment '意图执行时间';

alter table trade_logs
    modify target_date datetime null comment '目标执行时间';

alter table trade_logs
    modify actual_date datetime null comment '实际执行时间';

alter table trade_logs
    modify price decimal(10, 3) null comment '成交价';

alter table trade_logs
    modify open_price decimal(10, 3) null comment '开盘价';

alter table trade_logs
    modify close_price decimal(10, 3) null comment '收盘价';

alter table trade_logs
    modify high_price decimal(10, 3) null comment '最高价';

alter table trade_logs
    modify low_price decimal(10, 3) null comment '最低价';

alter table trade_logs
    modify pct_chg decimal(10, 3) null comment '涨跌幅';

alter table trade_logs
    modify change_amount decimal(10, 3) null comment '涨跌额';

alter table trade_logs
    modify vwap decimal(10, 3) null comment 'VWAP';

alter table trade_logs
    add order_type varchar(20) default 'daily_order' null comment '订单类型: daily_order/limit_order/market_order' after status;

alter table trade_logs
    add limit_price decimal(10, 3) null comment '限价单价格' after order_type;

alter table trade_logs
    add cutoff_time datetime null comment '限价单截止时间' after limit_price;

alter table trade_logs
    add actual_price decimal(10, 3) null comment '实际成交价' after cutoff_time;

create index idx_cutoff_time
    on trade_logs (cutoff_time);

create index idx_order_type
    on trade_logs (order_type);

alter table trades
    modify trade_date datetime not null comment '交易时间（精确到分钟）';

alter table trades
    modify price decimal(10, 3) null comment '成交价';

alter table trades
    modify price_type enum ('open', 'close', 'vwap', 'actual') not null comment '价格类型';

alter table trades
    add order_type varchar(20) default 'daily_order' null comment '订单类型: daily_order/limit_order/market_order';

alter table trades
    add limit_price decimal(10, 3) null comment '限价单价格';

alter table trades
    add cutoff_time datetime null comment '限价单截止时间';

alter table trades
    add actual_price decimal(10, 3) null comment '实际成交价（与 VWAP 分离）';

create index idx_cutoff_time
    on trades (cutoff_time);

create index idx_order_type
    on trades (order_type);

