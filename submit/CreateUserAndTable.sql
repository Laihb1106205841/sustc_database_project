-- 创建数据库
CREATE DATABASE sustc WITH ENCODING = 'UTF8' LC_COLLATE = 'C' TEMPLATE = template0;

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
-- alter table if exists user_follow drop constraint user_follow_pkey;
-- alter table if exists user_collect_video drop constraint user_collect_video_bv_fkey;
-- alter table if exists user_collect_video drop constraint user_collect_video_user_mid_fkey;
-- alter table if exists user_like_video drop constraint user_like_video_bv_fkey;
-- alter table if exists user_like_video drop constraint user_like_video_user_mid_fkey;
-- alter table if exists user_give_Coin_video drop constraint user_give_coin_video_bv_fkey;
-- alter table if exists user_give_Coin_video drop constraint user_give_coin_video_user_mid_fkey;


-- alter table if exists user_follow drop constraint user_follow_pkey;
-- alter table if exists user_collect_video drop constraint user_collect_video_bv_fkey;
-- alter table if exists user_collect_video drop constraint user_collect_video_user_mid_fkey;
-- alter table if exists user_like_video drop constraint user_like_video_bv_fkey;
-- alter table if exists user_like_video drop constraint user_like_video_user_mid_fkey;
-- alter table if exists user_give_Coin_video drop constraint user_give_coin_video_bv_fkey;
-- alter table if exists user_give_Coin_video drop constraint user_give_coin_video_user_mid_fkey;


-- SELECT * from b_video where title like '%子%';
-- 删除事件触发器
-- drop function DISABLE_DROP_TABLE();

-- 删除函数

--
-- drop table b_user cascade ;
-- drop table b_video cascade ;
-- drop table b_danmu cascade ;
-- drop table user_watch_video cascade ;
-- drop table user_give_Coin_video cascade ;
-- drop table user_like_video;
-- drop table user_collect_video;
-- drop table user_follow;
-- drop table danmu_like;
-- drop type user_identity cascade ;
-- drop type user_sex cascade ;
-- drop index pk_mid;
-- drop index uk_bv;
-- drop index uk_owner_mid;
-- drop index idx_bv;
-- drop index idx_follower_mid;
-- drop index idx_sender_mid;

--rule:
--user mid is bigint
--BV is varchar(20)
--更新、上传、审核视频 timestamp
--视频时长、观看时长  decimal

-- drop type user_identity cascade ;
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
    password     varchar(20)                not null , --max 17-18
    qq           varchar(20)     unique null , --qq 1106205841 10
    wechat       varchar(28)     unique null , --max:15
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

-- drop table b_danmu;
--------------------DAN_MU----------------------------------------
--like
CREATE TABLE IF NOT EXISTS b_danmu
(
  danmu_id       bigint PRIMARY KEY ,
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
-- drop index user_collect_video.uk_bv;
-- CREATE INDEX IF NOT EXISTS uk_bv ON user_collect_video (bv);
comment on table user_collect_video is '收藏视频';

--2 like
CREATE TABLE IF NOT EXISTS user_like_video(
    user_mid    bigint not null,
    bv          char(12) not null --references b_video (bv) on delete cascade
);
CREATE INDEX IF NOT EXISTS uk_bv ON user_like_video (bv);
comment on table user_like_video is '喜欢视频';

--3 coin
CREATE TABLE IF NOT EXISTS user_give_Coin_video(
    user_mid    bigint not null ,--references b_user (mid) on delete cascade,
    bv          char(12) not null--references b_video (bv) on delete cascade
);
CREATE INDEX IF NOT EXISTS uk_bv ON user_give_Coin_video (bv);
comment on table user_give_Coin_video is '投币视频';

-- drop table user_watch_video;
CREATE TABLE IF NOT EXISTS user_watch_video(
    user_mid   bigint not null, --references b_user (mid) on delete cascade,
    bv         char(12) not null, --references b_video (bv) on delete cascade,
    view_time  float
);
CREATE INDEX IF NOT EXISTS idx_bv on user_watch_video (bv);

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

GRANT SELECT on sustc.public.history_user to bilibili;
GRANT INSERT on sustc.public.history_user to bilibili;
GRANT UPDATE on sustc.public.history_user to bilibili;

GRANT SELECT on sustc.public.history_video to bilibili;
GRANT INSERT on sustc.public.history_video to bilibili;
GRANT UPDATE on sustc.public.history_video to bilibili;

GRANT SELECT on sustc.public.history_danmu to bilibili;
GRANT INSERT on sustc.public.history_danmu to bilibili;
GRANT UPDATE on sustc.public.history_danmu to bilibili;

GRANT SELECT on sustc.public.user_watch_video to bilibili;
GRANT INSERT on sustc.public.user_watch_video to bilibili;
GRANT UPDATE on sustc.public.user_watch_video to bilibili;
GRANT delete on sustc.public.user_watch_video to bilibili;

GRANT SELECT on sustc.public.user_give_coin_video to bilibili;
GRANT INSERT on sustc.public.user_give_coin_video to bilibili;
GRANT update on sustc.public.user_give_coin_video to bilibili;
GRANT delete on sustc.public.user_give_coin_video to bilibili;

GRANT SELECT on sustc.public.danmu_like to bilibili;
GRANT INSERT on sustc.public.danmu_like to bilibili;
GRANT update on sustc.public.danmu_like to bilibili;
GRANT delete on sustc.public.danmu_like to bilibili;

-- 授予执行权限
GRANT EXECUTE ON all functions in schema public TO bilibili;

-- 授予截断权限
-- 请替换 'your_table' 和 'your_user' 为实际的表名和用户名
GRANT TRUNCATE ON all tables in schema public to bilibili;

-- 授予选择权限
GRANT SELECT ON TABLE pg_tables TO bilibili;