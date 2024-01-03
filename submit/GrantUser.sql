
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

