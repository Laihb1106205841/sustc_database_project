-- 创建数据库
-- CREATE DATABASE sustc WITH ENCODING = 'UTF8' LC_COLLATE = 'C' TEMPLATE = template0;

-- 创建角色并授予登陆的权限，默认拥有public的部分权限，只能查看数据库名词和表名，其他不能查阅
CREATE ROLE bilibili WITH
  LOGIN
  NOSUPERUSER
  ENCRYPTED PASSWORD '123456';
-- 添加注释
COMMENT ON ROLE bilibili IS '操作用户';

GRANT create on database sustc to bilibili;

CREATE OR REPLACE FUNCTION DISABLE_DROP_TABLE()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
BEGIN
    if tg_tag = 'DROP TABLE'  THEN
        RAISE EXCEPTION 'Command % is disabled.', tg_tag;
    END if;
END;
$$;
CREATE EVENT TRIGGER DISABLE_DROP_TABLE on  ddl_command_start  EXECUTE FUNCTION DISABLE_DROP_TABLE();

CREATE EXTENSION IF NOT EXISTS "uuid-ossp"; --开启UUID算法
CREATE EXTENSION pgcrypto;

------------------------------------------------------------------------


------------------------USER-----------------------------------
create type user_identity as enum('USER','SUPERUSER');
create type user_sex as enum('MALE','FEMALE','UNKNOWN');
--sheet for User in Bili
-- drop table b_user;
CREATE TABLE IF NOT EXISTS b_user
(
    mid          bigint unique              not null            PRIMARY KEY,
    name         varchar(50)                not null , --checked
    sex          user_sex                   not null , --checked
    birthday     char(10), --checked 2023-5-38 13:90:34
    level        smallint    default 0, --only 2byte,128
    sign         varchar(100),
--     followingMid varchar(20), user_user
--     followerMid  varchar(20),
    identity     user_identity,
    password     varchar(35)                not null , --max 17-18
    qq           varchar(35)     unique null , --qq 1106205841 10
    wechat       varchar(35)     unique null , --max:15
    create_time  timestamp,
    coin         int         default 0
--12
);
-- drop table history_user;
CREATE TABLE IF NOT EXISTS history_user
(
    mid          bigint             not null           ,
    name         varchar(50)         not null , --checked
    sex          user_sex                   not null , --checked
    birthday     char(10), --checked 2023-5-38 13:90:34
    level        smallint    default 1, --only 2byte,128
    sign         varchar(100),
--     followingMid varchar(20), user_user
--     followerMid  varchar(20),
    identity     user_identity,
    password     varchar(20)                not null , --max 17-18
    qq           varchar(20)    , --qq 1106205841 10
    wechat       varchar(20)     , --max:15
    create_time  timestamp,
    delete_time  timestamp,
    coin         int         default 0

--12
);
-- index
CREATE INDEX IF NOT EXISTS pk_mid ON b_user (mid);
CREATE INDEX IF NOT EXISTS uk_qq ON b_user(qq);
CREATE INDEX IF NOT EXISTS uk_wechat ON b_user(wechat);

comment on column b_user.mid is '用户id';
comment on column b_user.name is '用户昵称';
comment on column b_user.sex is '用户性别';
comment on column b_user.birthday is '用户生日';
comment on column b_user.level is '用户等级';
comment on column b_user.sign is '用户签名';
comment on column b_user.identity is '用户分类：超级用户/普通用户';
comment on column b_user.password is '用户密码';
comment on column b_user.qq is '用户qq';
comment on column b_user.wechat is '用户微信';
comment on column b_user.create_time is '用户创建时间';
comment on column b_user.coin is '用户硬币';

-- drop table user_follow;
--------------------USER_USER----------------------------------------
-- a follow sheet that mark all the follower_mid and follower_mid
CREATE TABLE IF NOT EXISTS user_follow(--check
    follower_mid    bigint not null ,
    following_mid   bigint not null
);
--index

-- CREATE INDEX IF NOT EXISTS idx_follower_mid ON user_follow (follower_mid);

comment on column user_follow.follower_mid is '粉丝、跟随者';
comment on column user_follow.following_mid is '被关注者，主播';
CREATE INDEX IF NOT EXISTS idx_follower ON user_follow (follower_mid);
CREATE INDEX IF NOT EXISTS idx_following ON user_follow (following_mid);

-- drop table b_danmu cascade ;
--------------------DAN_MU----------------------------------------
--like
CREATE TABLE IF NOT EXISTS b_danmu
(
  danmu_id       serial,
  bv             char(12),
  sender_mid     bigint not null ,
  time           float,  --check
  content        text,
  post_time      timestamp

);
-- drop table history_danmu;
CREATE TABLE IF NOT EXISTS history_danmu
(
  danmu_id       bigint PRIMARY KEY ,
  bv             char(12),
  sender_mid     bigint not null ,
  time           float,  --check
  content        text,
  post_time      timestamp

);
GRANT USAGE, SELECT ON SEQUENCE b_danmu_danmu_id_seq TO bilibili;

--index
CREATE INDEX IF NOT EXISTS idx_sender_mid ON b_danmu (sender_mid);
CREATE INDEX IF NOT EXISTS idx_bv ON b_danmu(bv);

comment on column b_danmu.danmu_id is '弹幕id，主键';
comment on column b_danmu.bv is '弹幕所在的视频bv';
comment on column b_danmu.sender_mid is '发送者';
comment on column b_danmu.time is '发送的时间，用decimal表示';
comment on column b_danmu.content is '发送内容';
comment on column b_danmu.post_time is '发送时间，timestamp';


-- drop table danmu_like;
--------------------DAN_MU_USER_like----------------------------------
CREATE TABLE IF NOT EXISTS danmu_like(
    danmu_id    bigint,
    bv          varchar(20) ,
    user_mid_like bigint  not null

);
CREATE INDEX IF NOT EXISTS idx_bv ON danmu_like(bv);

-- drop table b_video;
--------------------VIDEO----------------------------------------
CREATE TABLE IF NOT EXISTS b_video(
    bv           char(12) PRIMARY KEY ,
    title        varchar(80),
    owner_mid    bigint,
    owner_name   varchar(20),
    create_time  timestamp,
    commit_time  timestamp,
    review_time  timestamp,
    public_time  timestamp,

    duration     float,
    description  text,

    reviewer     bigint,
    update_time  timestamp,
    is_posted    smallint default 0,
    is_review    smallint default 0,--sf,
    is_public    smallint default 0-------------0,不行，1，行！
);
CREATE INDEX IF NOT EXISTS uk_owner_mid ON b_video (owner_mid);

comment on column b_video.commit_time is '提交视频的时间';
comment on column b_video.review_time is '审核视频的时间';
comment on column b_video.public_time is '发布视频的时间';
comment on column b_video.update_time is '更新视频的时间，最新的一期';

CREATE TABLE IF NOT EXISTS history_video(
    bv           char(12)  ,
    title        varchar(80),
    owner_mid    bigint,
    owner_name   varchar(20),

    commit_time  timestamp,
    review_time  timestamp,
    public_time  timestamp,

    duration     int,
    description  text,

    reviewer     bigint,
    update_time  timestamp,
    is_review    smallint, --sf,
    is_posted    smallint default 0,
    is_public    smallint default 0-------------0,不行，1，行！
);

--------------------VIDEO_USER----------------------------------------

-- CREATE TABLE IF NOT EXISTS video_user(); --second solution: one for all
-- drop table user_give_coin_video;
-- drop table user_watch_video;
-- drop table user_like_video;
-- drop table user_collect_video;
--1 collect
CREATE TABLE IF NOT EXISTS user_collect_video(
    user_mid    bigint not null  ,
    bv          char(12) not null
);
CREATE INDEX IF NOT EXISTS idx_collect_bv ON user_collect_video(bv);
CREATE INDEX IF NOT EXISTS idx_collect_mid ON user_collect_video(user_mid);
-- drop index user_collect_video.uk_bv;
-- CREATE INDEX IF NOT EXISTS uk_bv ON user_collect_video (bv);
comment on table user_collect_video is '收藏视频';

--2 like
CREATE TABLE IF NOT EXISTS user_like_video(
    user_mid    bigint not null,
    bv          char(12) not null --references b_video (bv) on delete cascade
);
CREATE INDEX IF NOT EXISTS idx_like_bv ON user_like_video (bv);
CREATE INDEX IF NOT EXISTS idx_like_mid ON user_like_video (user_mid);
create index if not exists un_like on user_like_video(bv,user_mid);
comment on table user_like_video is '喜欢视频';

--3 coin
CREATE TABLE IF NOT EXISTS user_give_Coin_video(
    user_mid    bigint not null ,--references b_user (mid) on delete cascade,
    bv          char(12) not null--references b_video (bv) on delete cascade
);
CREATE INDEX IF NOT EXISTS idx_coin_bv ON user_give_Coin_video (bv);
CREATE INDEX IF NOT EXISTS idx_coin_mid ON user_give_Coin_video (user_mid);
CREATE INDEX IF NOT EXISTS un_coin ON user_give_Coin_video (bv,user_mid);
comment on table user_give_Coin_video is '投币视频';

-- drop table user_watch_video;
CREATE TABLE IF NOT EXISTS user_watch_video(
    user_mid   bigint not null, --references b_user (mid) on delete cascade,
    bv         char(12) not null, --references b_video (bv) on delete cascade,
    view_time  float
);
CREATE INDEX IF NOT EXISTS idx_watch_bv on user_watch_video (bv);
CREATE INDEX IF NOT EXISTS idx_watch_mid on user_watch_video (user_mid);
CREATE INDEX IF NOT EXISTS un_watch on user_watch_video (bv,user_mid);

----------------------------------GRANT------------------------------

-- 授予读写权限
GRANT SELECT on sustc.public.b_user to bilibili;
GRANT INSERT on sustc.public.b_user to bilibili;
GRANT update on sustc.public.b_user to bilibili;
GRANT delete on sustc.public.b_user to bilibili;

GRANT SELECT on sustc.public.b_video to bilibili;
GRANT INSERT on sustc.public.b_video to bilibili;
GRANT update on sustc.public.b_video to bilibili;
GRANT delete on sustc.public.b_video to bilibili;

GRANT SELECT on sustc.public.b_danmu to bilibili;
GRANT INSERT on sustc.public.b_danmu to bilibili;
GRANT update on sustc.public.b_danmu to bilibili;
GRANT delete on sustc.public.b_danmu to bilibili;

GRANT SELECT on sustc.public.user_like_video to bilibili;
GRANT INSERT on sustc.public.user_like_video to bilibili;
GRANT update on sustc.public.user_like_video to bilibili;
GRANT delete on sustc.public.user_like_video to bilibili;

GRANT SELECT on sustc.public.user_collect_video to bilibili;
GRANT INSERT on sustc.public.user_collect_video to bilibili;
GRANT UPDATE on sustc.public.user_collect_video to bilibili;
GRANT delete on sustc.public.user_collect_video to bilibili;

GRANT SELECT on sustc.public.user_follow to bilibili;
GRANT INSERT on sustc.public.user_follow to bilibili;
GRANT UPDATE on sustc.public.user_follow to bilibili;
GRANT delete on sustc.public.user_follow to bilibili;

GRANT all    on schema public to bilibili;



GRANT SELECT on sustc.public.history_video to bilibili;
GRANT INSERT on sustc.public.history_video to bilibili;
GRANT UPDATE on sustc.public.history_video to bilibili;
GRANT SELECT on sustc.public.user_watch_video to bilibili;
GRANT INSERT on sustc.public.user_watch_video to bilibili;
GRANT UPDATE on sustc.public.user_watch_video to bilibili;
GRANT delete on sustc.public.user_watch_video to bilibili;

GRANT SELECT on sustc.public.history_danmu to bilibili;
GRANT INSERT on sustc.public.history_danmu to bilibili;
GRANT UPDATE on sustc.public.history_danmu to bilibili;

GRANT SELECT on sustc.public.user_give_coin_video to bilibili;
GRANT INSERT on sustc.public.user_give_coin_video to bilibili;
GRANT update on sustc.public.user_give_coin_video to bilibili;
GRANT delete on sustc.public.user_give_coin_video to bilibili;

GRANT SELECT on sustc.public.history_user to bilibili;
GRANT INSERT on sustc.public.history_user to bilibili;
GRANT UPDATE on sustc.public.history_user to bilibili;

GRANT SELECT on sustc.public.danmu_like to bilibili;
GRANT INSERT on sustc.public.danmu_like to bilibili;
GRANT update on sustc.public.danmu_like to bilibili;
GRANT delete on sustc.public.danmu_like to bilibili;
GRANT CONNECT ON DATABASE sustc TO bilibili;

-- GRANT ALL ON public TO bilibili;
GRANT ALL ON ALL TABLES IN SCHEMA PUBLIC to bilibili; --赋予demo_role所有表的SELECT权限
-- select * from information_schema.table_privileges where grantee='bilibili';
-- GRANT ALL ON demo TO demo_role; --赋给用户所有权限
-- GRANT SELECT ON demo TO PUBLIC; --将SELECT权限赋给所有用户

-- select * from pg_indexes ;

-- 授予选择权限
GRANT SELECT ON TABLE pg_tables TO bilibili;
-- 授予更新权限
GRANT UPDATE ON TABLE pg_tables TO bilibili;
-- 授予插入权限
GRANT insert ON TABLE pg_tables TO bilibili;

-- 授予执行权限
GRANT EXECUTE ON all functions in schema public TO bilibili;

-- 授予截断权限
GRANT TRUNCATE ON  b_user to bilibili;

GRANT ALL ON all tables in schema public to bilibili;



CREATE OR REPLACE PROCEDURE insert_user_watch_video(
    p_user_mid bigint,
    p_bv CHAR(80),
    p_view_time float
)
AS $$
BEGIN
    INSERT INTO user_watch_video (user_mid, bv, view_time)
    VALUES (p_user_mid, p_bv, p_view_time);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE insert_user_collect_video(
    p_user_mid INT,
    p_bv VARCHAR(255)
)
AS $$
BEGIN
    INSERT INTO user_collect_video (user_mid, bv)
    VALUES (p_user_mid, p_bv);
END;
$$ LANGUAGE plpgsql;

-- 创建存储过程
CREATE OR REPLACE FUNCTION get_danmu_by_bv_and_time(
    bv_param char(12),
    start_time_param float,
    end_time_param float
) RETURNS TABLE (x b_danmu) AS
$$
BEGIN
    RETURN QUERY
    SELECT x.danmu_id
    FROM (
        SELECT DISTINCT ON (content) danmu_id, content, time
        FROM b_danmu
        WHERE bv = bv_param AND time BETWEEN start_time_param AND end_time_param
        ORDER BY content, time
    ) x
    ORDER BY x.time;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION delete_user_watch_video()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM user_watch_video
    WHERE bv = OLD.bv;

    DELETE from user_collect_video
    where bv = old.bv;

    DELETE from user_give_coin_video
    where bv = old.bv;

    DELETE from user_like_video
    where bv = old.bv;

    DELETE from b_danmu
    where bv = old.bv;

    DELETE from danmu_like
    where bv = old.bv;
    return old;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER after_b_video_delete
AFTER DELETE ON b_video
FOR EACH ROW
EXECUTE FUNCTION delete_user_watch_video();


CREATE OR REPLACE FUNCTION insert_and_update_coins(
  user_mid_param bigint,
  bv_param char(12)
)
RETURNS VOID AS $$
BEGIN
  -- 开始事务
  BEGIN
    -- 插入新行到 user_give_coin_video 表
    INSERT INTO user_give_coin_video (user_mid, bv)
    VALUES (user_mid_param, bv_param);

    -- 更新 b_user 表
    UPDATE b_user
    SET coin = coin - 1
    WHERE mid = user_mid_param;

    -- 提交事务
    COMMIT;
  EXCEPTION
    -- 如果发生错误，回滚事务
    WHEN OTHERS THEN
      ROLLBACK;
  END;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_or_delete_user_like_video(
  bv_param VARCHAR,
  mid_param BIGINT
)
RETURNS BOOLEAN AS $$
DECLARE
  is_deleted BOOLEAN;
BEGIN
  -- 开始事务
  BEGIN
    -- 检查是否存在记录
    IF EXISTS (SELECT 1 FROM user_like_video WHERE bv = bv_param AND user_mid = mid_param) THEN
      -- 如果存在，则删除记录并设置 is_deleted 为 TRUE
      DELETE FROM user_like_video WHERE bv = bv_param AND user_mid = mid_param;
      is_deleted := false;
    ELSE
      -- 如果不存在，则插入新记录并设置 is_deleted 为 FALSE
      INSERT INTO user_like_video (bv, user_mid) VALUES (bv_param, mid_param);
      is_deleted := true;
    END IF;

    -- 提交事务
    COMMIT;
  EXCEPTION
    -- 如果发生错误，回滚事务并设置 is_deleted 为 NULL
    WHEN OTHERS THEN
      ROLLBACK;
      is_deleted := NULL;
  END;

  -- 返回 is_deleted
  RETURN is_deleted;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_or_delete_user_collect_video(
  bv_param VARCHAR,
  mid_param BIGINT
)
RETURNS BOOLEAN AS $$
DECLARE
  is_deleted BOOLEAN;
BEGIN
  -- 开始事务
  BEGIN
    -- 检查是否存在记录
    IF EXISTS (SELECT 1 FROM user_collect_video WHERE bv = bv_param AND user_mid = mid_param) THEN
      -- 如果存在，则删除记录并设置 is_deleted 为 TRUE
      DELETE FROM user_collect_video WHERE bv = bv_param AND user_mid = mid_param;
      is_deleted := false;
    ELSE
      -- 如果不存在，则插入新记录并设置 is_deleted 为 FALSE
      INSERT INTO user_collect_video (bv, user_mid) VALUES (bv_param, mid_param);
      is_deleted := true;
    END IF;

    -- 提交事务
    COMMIT;
  EXCEPTION
    -- 如果发生错误，回滚事务并设置 is_deleted 为 NULL
    WHEN OTHERS THEN
      ROLLBACK;
      is_deleted := NULL;
  END;

  -- 返回 is_deleted
  RETURN is_deleted;
END;
$$ LANGUAGE plpgsql;

Create trigger del_b_user before delete on b_user
For each row
begin
 Insert into history_user(mid, name, sex, birthday, level, sign, identity, password, qq, wechat, create_time, delete_time, coin)
 Values(mid, name, sex, birthday, level, sign, identity, password, qq, wechat, create_time, delete_time, coin);
End;





-- 创建一个函数
CREATE OR REPLACE FUNCTION log_deleted_b_user()
RETURNS TRIGGER AS $$
BEGIN
    -- 将被删除的内容插入到 history_b_user 表中
    INSERT INTO history_user (mid, name, sex, birthday, level, sign,
                              identity, password, qq, wechat, create_time, delete_time, coin)
    VALUES (OLD.mid, OLD.name, OLD.sex,old.birthday,old.level,old.sign,
            old.identity,old.password,old.qq,old.wechat,old.create_time,current_timestamp,old.coin);

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- 创建触发器
CREATE TRIGGER trg_delete_b_user
BEFORE DELETE ON b_user
FOR EACH ROW
EXECUTE FUNCTION log_deleted_b_user();


-- 创建一个函数，用于回滚操作
CREATE OR REPLACE FUNCTION rollback_on_delete_failure()
RETURNS TRIGGER AS $$
BEGIN
    -- 开始一个事务
    BEGIN
        savepoint s;
        -- 如果是删除操作触发了这个触发器，回滚这次删除
        IF TG_OP = 'DELETE' THEN
            RAISE NOTICE 'Delete operation failed! Rolling back...';
            RAISE EXCEPTION 'Rolling back due to delete operation failure';

        END IF;
        -- 其他操作的情况下同样进行回滚
        -- 可以根据需要添加其他条件和逻辑

        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'An error occurred! Rolling back...';
            RAISE EXCEPTION 'Rolling back due to transaction failure';
--             rollback ;
    END;
END;
$$ LANGUAGE plpgsql;

-- 创建触发器
CREATE TRIGGER trg_rollback_on_delete_failure
AFTER DELETE ON b_user
FOR EACH STATEMENT
EXECUTE FUNCTION rollback_on_delete_failure();



-- 创建一个保存点回滚的触发器函数
CREATE OR REPLACE FUNCTION rollback_on_failure()
RETURNS TRIGGER AS $$
DECLARE
BEGIN

    EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'An error occurred! Rolling back...';
    RAISE EXCEPTION 'Rolling back due to transaction failure';

END;
$$ LANGUAGE plpgsql;

-- 创建触发器，将触发器函数与 DELETE 操作关联
CREATE TRIGGER b_user_delete
after delete ON b_user
FOR EACH ROW
EXECUTE FUNCTION rollback_on_failure();
CREATE TRIGGER b_user_update
after update ON b_user
FOR EACH ROW
EXECUTE FUNCTION rollback_on_failure();