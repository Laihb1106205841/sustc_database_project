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