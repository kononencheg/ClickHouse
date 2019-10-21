
-- Журнал событий
--
-- Основная таблица для "нарезки" вьюх для конкретного решения. Хранит
-- событие и атрибуты источника на момент проведения событий.
CREATE TABLE IF NOT EXISTS __event_log
(
    event_domain      String,
    event             String,
    event_source      String,

    event_time        DateTime,
    event_date        Date,
    event_id          UUID,

    event_tags        Array(String),

    event_double_1    Float64,
    event_double_2    Float64,
    event_double_3    Float64,
    event_double_4    Float64,
    event_double_5    Float64,
    event_double_6    Float64,
    event_double_7    Float64,
    event_double_8    Float64,
    event_double_9    Float64,
    event_double_10   Float64,
    event_double_11   Float64,
    event_double_12   Float64,
    event_double_13   Float64,
    event_double_14   Float64,
    event_double_15   Float64,
    event_double_16   Float64,
    event_double_17   Float64,
    event_double_18   Float64,
    event_double_19   Float64,
    event_double_20   Float64,

    event_time_1      DateTime,
    event_time_2      DateTime,
    event_time_3      DateTime,
    event_time_4      DateTime,
    event_time_5      DateTime,
    event_time_6      DateTime,
    event_time_7      DateTime,
    event_time_8      DateTime,
    event_time_9      DateTime,
    event_time_10     DateTime,

    event_string_1    String,
    event_string_2    String,
    event_string_3    String,
    event_string_4    String,
    event_string_5    String,
    event_string_6    String,
    event_string_7    String,
    event_string_8    String,
    event_string_9    String,
    event_string_10   String,
    event_string_11   String,
    event_string_12   String,
    event_string_13   String,
    event_string_14   String,
    event_string_15   String,
    event_string_16   String,
    event_string_17   String,
    event_string_18   String,
    event_string_19   String,
    event_string_20   String,

    event_facade_time DateTime,
    event_accept_time DateTime,
    event_insert_time DateTime MATERIALIZED now(),

    source_open_time  DateTime,
    source_open_date  Date,

    source_tags       Array(String),

    source_double_1   Float64,
    source_double_2   Float64,
    source_double_3   Float64,
    source_double_4   Float64,
    source_double_5   Float64,
    source_double_6   Float64,
    source_double_7   Float64,
    source_double_8   Float64,
    source_double_9   Float64,
    source_double_10  Float64,
    source_double_11  Float64,
    source_double_12  Float64,
    source_double_13  Float64,
    source_double_14  Float64,
    source_double_15  Float64,
    source_double_16  Float64,
    source_double_17  Float64,
    source_double_18  Float64,
    source_double_19  Float64,
    source_double_20  Float64,

    source_time_1     DateTime,
    source_time_2     DateTime,
    source_time_3     DateTime,
    source_time_4     DateTime,
    source_time_5     DateTime,
    source_time_6     DateTime,
    source_time_7     DateTime,
    source_time_8     DateTime,
    source_time_9     DateTime,
    source_time_10    DateTime,

    source_string_1   String,
    source_string_2   String,
    source_string_3   String,
    source_string_4   String,
    source_string_5   String,
    source_string_6   String,
    source_string_7   String,
    source_string_8   String,
    source_string_9   String,
    source_string_10  String,
    source_string_11  String,
    source_string_12  String,
    source_string_13  String,
    source_string_14  String,
    source_string_15  String,
    source_string_16  String,
    source_string_17  String,
    source_string_18  String,
    source_string_19  String,
    source_string_20  String
) ENGINE = ReplacingMergeTree(event_accept_time)
      PARTITION BY toYYYYMM(event_date)
      ORDER BY (event_domain, event, event_date, event_source, event_time, event_id)
      SAMPLE BY event_id;

-- Буфер входящих собтий
--
-- Хранит список событий для дальнейшей вставки в __event_log. Необходима для изалечения
-- списка источников для поиска их состояния, минимального и максимального времени для
-- ограничения реконструкции истории состояния источника.
--
-- См.  `__event_input_source_history`
CREATE TABLE IF NOT EXISTS __event_input_buffer
(
    event_domain      String,
    event             String,
    event_source      String,

    event_time        DateTime,
    event_date        Date DEFAULT toDate(event_time),

    event_id          UUID,

    event_tags        Array(String),

    event_double_1    Float64,
    event_double_2    Float64,
    event_double_3    Float64,
    event_double_4    Float64,
    event_double_5    Float64,
    event_double_6    Float64,
    event_double_7    Float64,
    event_double_8    Float64,
    event_double_9    Float64,
    event_double_10   Float64,
    event_double_11   Float64,
    event_double_12   Float64,
    event_double_13   Float64,
    event_double_14   Float64,
    event_double_15   Float64,
    event_double_16   Float64,
    event_double_17   Float64,
    event_double_18   Float64,
    event_double_19   Float64,
    event_double_20   Float64,

    event_time_1      DateTime,
    event_time_2      DateTime,
    event_time_3      DateTime,
    event_time_4      DateTime,
    event_time_5      DateTime,
    event_time_6      DateTime,
    event_time_7      DateTime,
    event_time_8      DateTime,
    event_time_9      DateTime,
    event_time_10     DateTime,

    event_string_1    String,
    event_string_2    String,
    event_string_3    String,
    event_string_4    String,
    event_string_5    String,
    event_string_6    String,
    event_string_7    String,
    event_string_8    String,
    event_string_9    String,
    event_string_10   String,
    event_string_11   String,
    event_string_12   String,
    event_string_13   String,
    event_string_14   String,
    event_string_15   String,
    event_string_16   String,
    event_string_17   String,
    event_string_18   String,
    event_string_19   String,
    event_string_20   String,

    event_facade_time DateTime,
    event_accept_time DateTime DEFAULT now()
) ENGINE = Memory();

-- Вход для изменения состояний источника
--
-- Не хранит данные. Необходим для передачи данные в материальзованные
-- представления.
CREATE TABLE IF NOT EXISTS __source_update_input
(
    source_domain      String,
    source             String,

    source_update_time DateTime,
    source_update_date Date DEFAULT toDate(source_update_time),

    source_tags        Array(String),

    source_double_1    Float64,
    source_double_2    Float64,
    source_double_3    Float64,
    source_double_4    Float64,
    source_double_5    Float64,
    source_double_6    Float64,
    source_double_7    Float64,
    source_double_8    Float64,
    source_double_9    Float64,
    source_double_10   Float64,
    source_double_11   Float64,
    source_double_12   Float64,
    source_double_13   Float64,
    source_double_14   Float64,
    source_double_15   Float64,
    source_double_16   Float64,
    source_double_17   Float64,
    source_double_18   Float64,
    source_double_19   Float64,
    source_double_20   Float64,

    source_time_1      DateTime,
    source_time_2      DateTime,
    source_time_3      DateTime,
    source_time_4      DateTime,
    source_time_5      DateTime,
    source_time_6      DateTime,
    source_time_7      DateTime,
    source_time_8      DateTime,
    source_time_9      DateTime,
    source_time_10     DateTime,

    source_string_1    String,
    source_string_2    String,
    source_string_3    String,
    source_string_4    String,
    source_string_5    String,
    source_string_6    String,
    source_string_7    String,
    source_string_8    String,
    source_string_9    String,
    source_string_10   String,
    source_string_11   String,
    source_string_12   String,
    source_string_13   String,
    source_string_14   String,
    source_string_15   String,
    source_string_16   String,
    source_string_17   String,
    source_string_18   String,
    source_string_19   String,
    source_string_20   String,

    source_facade_time DateTime
) ENGINE = Null();


-- Журнал изменения состояний источника
--
-- Хранит только изменяемые поля за все время. Необходим для возможности
-- восстановления истории состояний источника.
CREATE MATERIALIZED VIEW IF NOT EXISTS __source_update_log
    ENGINE = ReplacingMergeTree(source_facade_time)
        PARTITION BY toYYYYMM(source_update_date)
        ORDER BY (source_domain, source_update_date, source, source_update_time)
AS
    WITH toStartOfWeek(now()) AS current_period,
         current_period - toIntervalWeek(1) AS actual_period_since,
         current_period + toIntervalWeek(2) AS actual_period_until
    SELECT *
      FROM __source_update_input
     WHERE source_facade_time != 0
       AND source_update_time BETWEEN actual_period_since AND actual_period_until;


-- Кэш актуального состояния источников
--
-- Под актуальным состоянием понимается скользящее окно интервала
-- в три периода (недели) текущая, предыдущая и следующая.
--
-- Хранит набор промежуточных пре-агрегаций для восстановления истории
-- для вставки в таблицу событий.
--
-- См. `__source_actual_snapshot`
-- См. `__source_immutable_state`
CREATE MATERIALIZED VIEW IF NOT EXISTS __source_mutable_state_cache
    ENGINE = AggregatingMergeTree()
        PARTITION BY (source_facade_time = 0)
        ORDER BY (source_domain, source, source_update_time, source_facade_time)
AS
WITH toStartOfWeek(now()) AS current_period,
     current_period - toIntervalWeek(1) AS actual_period_since,
     current_period + toIntervalWeek(2) AS actual_period_until

SELECT source_domain,
       source,

       source_update_time,

       minState(source_update_time)                      AS source_open_time,

       argMaxState(source_tags, source_update_time)      AS source_tags,

       argMaxState(source_double_1, source_update_time)  AS source_double_1,
       argMaxState(source_double_2, source_update_time)  AS source_double_2,
       argMaxState(source_double_3, source_update_time)  AS source_double_3,
       argMaxState(source_double_4, source_update_time)  AS source_double_4,
       argMaxState(source_double_5, source_update_time)  AS source_double_5,
       argMaxState(source_double_6, source_update_time)  AS source_double_6,
       argMaxState(source_double_7, source_update_time)  AS source_double_7,
       argMaxState(source_double_8, source_update_time)  AS source_double_8,
       argMaxState(source_double_9, source_update_time)  AS source_double_9,
       argMaxState(source_double_10, source_update_time) AS source_double_10,
       argMaxState(source_double_11, source_update_time) AS source_double_11,
       argMaxState(source_double_12, source_update_time) AS source_double_12,
       argMaxState(source_double_13, source_update_time) AS source_double_13,
       argMaxState(source_double_14, source_update_time) AS source_double_14,
       argMaxState(source_double_15, source_update_time) AS source_double_15,
       argMaxState(source_double_16, source_update_time) AS source_double_16,
       argMaxState(source_double_17, source_update_time) AS source_double_17,
       argMaxState(source_double_18, source_update_time) AS source_double_18,
       argMaxState(source_double_19, source_update_time) AS source_double_19,
       argMaxState(source_double_20, source_update_time) AS source_double_20,

       argMaxState(source_time_1,  source_update_time) AS source_time_1,
       argMaxState(source_time_2,  source_update_time) AS source_time_2,
       argMaxState(source_time_3,  source_update_time) AS source_time_3,
       argMaxState(source_time_4,  source_update_time) AS source_time_4,
       argMaxState(source_time_5,  source_update_time) AS source_time_5,
       argMaxState(source_time_6,  source_update_time) AS source_time_6,
       argMaxState(source_time_7,  source_update_time) AS source_time_7,
       argMaxState(source_time_8,  source_update_time) AS source_time_8,
       argMaxState(source_time_9,  source_update_time) AS source_time_9,
       argMaxState(source_time_10, source_update_time) AS source_time_10,

       argMaxState(source_string_1, source_update_time)  AS source_string_1,
       argMaxState(source_string_2, source_update_time)  AS source_string_2,
       argMaxState(source_string_3, source_update_time)  AS source_string_3,
       argMaxState(source_string_4, source_update_time)  AS source_string_4,
       argMaxState(source_string_5, source_update_time)  AS source_string_5,
       argMaxState(source_string_6, source_update_time)  AS source_string_6,
       argMaxState(source_string_7, source_update_time)  AS source_string_7,
       argMaxState(source_string_8, source_update_time)  AS source_string_8,
       argMaxState(source_string_9, source_update_time)  AS source_string_9,
       argMaxState(source_string_10, source_update_time) AS source_string_10,
       argMaxState(source_string_11, source_update_time) AS source_string_11,
       argMaxState(source_string_12, source_update_time) AS source_string_12,
       argMaxState(source_string_13, source_update_time) AS source_string_13,
       argMaxState(source_string_14, source_update_time) AS source_string_14,
       argMaxState(source_string_15, source_update_time) AS source_string_15,
       argMaxState(source_string_16, source_update_time) AS source_string_16,
       argMaxState(source_string_17, source_update_time) AS source_string_17,
       argMaxState(source_string_18, source_update_time) AS source_string_18,
       argMaxState(source_string_19, source_update_time) AS source_string_19,
       argMaxState(source_string_20, source_update_time) AS source_string_20,

       source_facade_time

FROM __source_update_input
WHERE source_update_time BETWEEN actual_period_since AND actual_period_until
GROUP BY source_domain, source, source_update_time, source_facade_time;


-- Неизменное состояние источников событий
--
-- Все неактульные записи из прошлого считаются неизменяемые поэтому их можно
-- "схлопнуть" для уменьшения данныв в `__source_mutable_state`.
CREATE VIEW IF NOT EXISTS __source_immutable_state_insertion
AS
WITH toStartOfWeek(now()) - toIntervalWeek(1) AS immutable_period_until
SELECT source_domain,
       source,

       minMerge(source_open_time)    AS source_update_time,
       toDate(source_update_time)    AS source_update_date,

       argMaxMerge(source_tags)      AS source_tags,

       argMaxMerge(source_double_1)  AS source_doubleg_1,
       argMaxMerge(source_double_2)  AS source_doubleg_2,
       argMaxMerge(source_double_3)  AS source_doubleg_3,
       argMaxMerge(source_double_4)  AS source_doubleg_4,
       argMaxMerge(source_double_5)  AS source_doubleg_5,
       argMaxMerge(source_double_6)  AS source_doubleg_6,
       argMaxMerge(source_double_7)  AS source_doubleg_7,
       argMaxMerge(source_double_8)  AS source_doubleg_8,
       argMaxMerge(source_double_9)  AS source_doubleg_9,
       argMaxMerge(source_double_10) AS source_doubleg_10,
       argMaxMerge(source_double_11) AS source_doubleg_11,
       argMaxMerge(source_double_12) AS source_doubleg_12,
       argMaxMerge(source_double_13) AS source_doubleg_13,
       argMaxMerge(source_double_14) AS source_doubleg_14,
       argMaxMerge(source_double_15) AS source_doubleg_15,
       argMaxMerge(source_double_16) AS source_doubleg_16,
       argMaxMerge(source_double_17) AS source_doubleg_17,
       argMaxMerge(source_double_18) AS source_doubleg_18,
       argMaxMerge(source_double_19) AS source_doubleg_19,
       argMaxMerge(source_double_20) AS source_doubleg_20,

       argMaxMerge(source_time_1) AS source_time_1,
       argMaxMerge(source_time_2) AS source_time_2,
       argMaxMerge(source_time_3) AS source_time_3,
       argMaxMerge(source_time_4) AS source_time_4,
       argMaxMerge(source_time_5) AS source_time_5,
       argMaxMerge(source_time_6) AS source_time_6,
       argMaxMerge(source_time_7) AS source_time_7,
       argMaxMerge(source_time_8) AS source_time_8,
       argMaxMerge(source_time_9) AS source_time_9,
       argMaxMerge(source_time_10) AS source_time_10,

       argMaxMerge(source_string_1) AS source_string_1,
       argMaxMerge(source_string_2) AS source_string_2,
       argMaxMerge(source_string_3) AS source_string_3,
       argMaxMerge(source_string_4) AS source_string_4,
       argMaxMerge(source_string_5) AS source_string_5,
       argMaxMerge(source_string_6) AS source_string_6,
       argMaxMerge(source_string_7) AS source_string_7,
       argMaxMerge(source_string_8) AS source_string_8,
       argMaxMerge(source_string_9) AS source_string_9,
       argMaxMerge(source_string_10) AS source_string_10,
       argMaxMerge(source_string_11) AS source_string_11,
       argMaxMerge(source_string_12) AS source_string_12,
       argMaxMerge(source_string_13) AS source_string_13,
       argMaxMerge(source_string_14) AS source_string_14,
       argMaxMerge(source_string_15) AS source_string_15,
       argMaxMerge(source_string_16) AS source_string_16,
       argMaxMerge(source_string_17) AS source_string_17,
       argMaxMerge(source_string_18) AS source_string_18,
       argMaxMerge(source_string_19) AS source_string_19,
       argMaxMerge(source_string_20) AS source_string_20,

       0 AS user_facade_time

FROM __source_mutable_state_cache
WHERE __source_mutable_state_cache.source_update_time < immutable_period_until
GROUP BY source_domain, source;


-- Пре-агрегированный кеш изменений состояния специально для вставки текущего
-- набора событий из `__event_input_buffer`
--
-- Возвразает записи между первым и последним моментом премени набора событий и
-- только для соответсвующих источников.
CREATE VIEW IF NOT EXISTS __source_mutable_state_cache_for_input
AS
    WITH
            (SELECT max(event_time) FROM __event_input_buffer) AS max_event_time,
            (SELECT min(event_time) FROM __event_input_buffer) AS min_event_time

    SELECT source_domain,
           source,

           source_update_time < min_event_time
                ? min_event_time
                : source_update_time          AS source_update_time,

           minMergeState(source_open_time)    AS source_open_time,

           argMaxMergeState(source_tags)      AS source_tags,

           argMaxMergeState(source_double_1)  AS source_double_1,
           argMaxMergeState(source_double_2)  AS source_double_2,
           argMaxMergeState(source_double_3)  AS source_double_3,
           argMaxMergeState(source_double_4)  AS source_double_4,
           argMaxMergeState(source_double_5)  AS source_double_5,
           argMaxMergeState(source_double_6)  AS source_double_6,
           argMaxMergeState(source_double_7)  AS source_double_7,
           argMaxMergeState(source_double_8)  AS source_double_8,
           argMaxMergeState(source_double_9)  AS source_double_9,
           argMaxMergeState(source_double_10) AS source_double_10,
           argMaxMergeState(source_double_11) AS source_double_11,
           argMaxMergeState(source_double_12) AS source_double_12,
           argMaxMergeState(source_double_13) AS source_double_13,
           argMaxMergeState(source_double_14) AS source_double_14,
           argMaxMergeState(source_double_15) AS source_double_15,
           argMaxMergeState(source_double_16) AS source_double_16,
           argMaxMergeState(source_double_17) AS source_double_17,
           argMaxMergeState(source_double_18) AS source_double_18,
           argMaxMergeState(source_double_19) AS source_double_19,
           argMaxMergeState(source_double_20) AS source_double_20,

           argMaxMergeState(source_time_1)    AS source_time_1,
           argMaxMergeState(source_time_2)    AS source_time_2,
           argMaxMergeState(source_time_3)    AS source_time_3,
           argMaxMergeState(source_time_4)    AS source_time_4,
           argMaxMergeState(source_time_5)    AS source_time_5,
           argMaxMergeState(source_time_6)    AS source_time_6,
           argMaxMergeState(source_time_7)    AS source_time_7,
           argMaxMergeState(source_time_8)    AS source_time_8,
           argMaxMergeState(source_time_9)    AS source_time_9,
           argMaxMergeState(source_time_10)   AS source_time_10,

           argMaxMergeState(source_string_1)  AS source_string_1,
           argMaxMergeState(source_string_2)  AS source_string_2,
           argMaxMergeState(source_string_3)  AS source_string_3,
           argMaxMergeState(source_string_4)  AS source_string_4,
           argMaxMergeState(source_string_5)  AS source_string_5,
           argMaxMergeState(source_string_6)  AS source_string_6,
           argMaxMergeState(source_string_7)  AS source_string_7,
           argMaxMergeState(source_string_8)  AS source_string_8,
           argMaxMergeState(source_string_9)  AS source_string_9,
           argMaxMergeState(source_string_10) AS source_string_10,
           argMaxMergeState(source_string_11) AS source_string_11,
           argMaxMergeState(source_string_12) AS source_string_12,
           argMaxMergeState(source_string_13) AS source_string_13,
           argMaxMergeState(source_string_14) AS source_string_14,
           argMaxMergeState(source_string_15) AS source_string_15,
           argMaxMergeState(source_string_16) AS source_string_16,
           argMaxMergeState(source_string_17) AS source_string_17,
           argMaxMergeState(source_string_18) AS source_string_18,
           argMaxMergeState(source_string_19) AS source_string_19,
           argMaxMergeState(source_string_20) AS source_string_20

    FROM __source_mutable_state_cache
    WHERE (source_domain, source) IN (SELECT DISTINCT event_domain, event_source FROM __event_input_buffer)
      AND source_update_time < max_event_time
    GROUP BY source_domain, source, source_update_time
    ORDER BY source_domain, source, source_update_time;


-- Кусок восстановленной
CREATE VIEW IF NOT EXISTS __source_state_history_for_input
AS
SELECT source_domain                       AS event_domain,
       source                              AS event_source,
       source_update_time                  AS event_time,

       runningAccumulate(source_open_time) AS source_open_time,
       toDate(source_open_time)            AS source_open_date,

       runningAccumulate(source_tags)       AS source_tags,

       runningAccumulate(source_double_1)  AS source_doubleg_1,
       runningAccumulate(source_double_2)  AS source_doubleg_2,
       runningAccumulate(source_double_3)  AS source_doubleg_3,
       runningAccumulate(source_double_4)  AS source_doubleg_4,
       runningAccumulate(source_double_5)  AS source_doubleg_5,
       runningAccumulate(source_double_6)  AS source_doubleg_6,
       runningAccumulate(source_double_7)  AS source_doubleg_7,
       runningAccumulate(source_double_8)  AS source_doubleg_8,
       runningAccumulate(source_double_9)  AS source_doubleg_9,
       runningAccumulate(source_double_10) AS source_doubleg_10,
       runningAccumulate(source_double_11) AS source_doubleg_11,
       runningAccumulate(source_double_12) AS source_doubleg_12,
       runningAccumulate(source_double_13) AS source_doubleg_13,
       runningAccumulate(source_double_14) AS source_doubleg_14,
       runningAccumulate(source_double_15) AS source_doubleg_15,
       runningAccumulate(source_double_16) AS source_doubleg_16,
       runningAccumulate(source_double_17) AS source_doubleg_17,
       runningAccumulate(source_double_18) AS source_doubleg_18,
       runningAccumulate(source_double_19) AS source_doubleg_19,
       runningAccumulate(source_double_20) AS source_doubleg_20,

       runningAccumulate(source_time_1)    AS source_time_1,
       runningAccumulate(source_time_2)    AS source_time_2,
       runningAccumulate(source_time_3)    AS source_time_3,
       runningAccumulate(source_time_4)    AS source_time_4,
       runningAccumulate(source_time_5)    AS source_time_5,
       runningAccumulate(source_time_6)    AS source_time_6,
       runningAccumulate(source_time_7)    AS source_time_7,
       runningAccumulate(source_time_8)    AS source_time_8,
       runningAccumulate(source_time_9)    AS source_time_9,
       runningAccumulate(source_time_10)   AS source_time_10,

       runningAccumulate(source_string_1)  AS source_string_1,
       runningAccumulate(source_string_2)  AS source_string_2,
       runningAccumulate(source_string_3)  AS source_string_3,
       runningAccumulate(source_string_4)  AS source_string_4,
       runningAccumulate(source_string_5)  AS source_string_5,
       runningAccumulate(source_string_6)  AS source_string_6,
       runningAccumulate(source_string_7)  AS source_string_7,
       runningAccumulate(source_string_8)  AS source_string_8,
       runningAccumulate(source_string_9)  AS source_string_9,
       runningAccumulate(source_string_10) AS source_string_10,
       runningAccumulate(source_string_11) AS source_string_11,
       runningAccumulate(source_string_12) AS source_string_12,
       runningAccumulate(source_string_13) AS source_string_13,
       runningAccumulate(source_string_14) AS source_string_14,
       runningAccumulate(source_string_15) AS source_string_15,
       runningAccumulate(source_string_16) AS source_string_16,
       runningAccumulate(source_string_17) AS source_string_17,
       runningAccumulate(source_string_18) AS source_string_18,
       runningAccumulate(source_string_19) AS source_string_19,
       runningAccumulate(source_string_20) AS source_string_20

FROM __source_mutable_state_cache_for_input;


CREATE VIEW IF NOT EXISTS __event_log_insertion
AS
    WITH toStartOfWeek(now()) AS current_period,
            current_period - toIntervalWeek(1) AS actual_period_since,
            current_period + toIntervalWeek(2) AS actual_period_until
    SELECT * FROM __event_input_buffer
    ASOF LEFT JOIN __source_state_history_for_input
        USING (event_domain, event_source, event_time)

    WHERE event_time BETWEEN actual_period_since AND actual_period_until;


-- Insert values
INSERT INTO __event_log SELECT * FROM __event_log_insertion;
TRUNCATE TABLE __event_input_buffer;

-- Snapshot immutable state
INSERT INTO __source_update_input SELECT * FROM __source_immutable_state_insertion;
ALTER TABLE __source_mutable_state_cache DELETE
WHERE source_update_time < toStartOfWeek(now()) - toIntervalWeek(1)
  AND source_facade_time != 0;