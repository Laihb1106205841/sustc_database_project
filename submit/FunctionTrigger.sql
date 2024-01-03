
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




-- 创建存储过程
CREATE OR REPLACE FUNCTION get_b_table_data(pg1 INT, pg2 int)
RETURNS TABLE(bv CHAR(12)) AS
$$
BEGIN
    -- 在此处执行你的查询
    RETURN QUERY (



WITH RankedData AS (
    SELECT
        v.video_id,
        COALESCE(w.total_views, 0) AS total_views,
        COALESCE(l.total_likes, 0) AS total_likes,
        COALESCE(c.total_coin,0) as total_coin,
        COALESCE(f.total_fav,0) as total_fav,
        coalesce(d.total_danmu,0) as total_danmu,
        coalesce(x.watch_ratio,0) as total_wat,
        CASE
            WHEN w.total_views > 0 THEN CAST(l.total_likes AS FLOAT) / w.total_views
            ELSE 0
        END AS like_ratio,
        CASE

            WHEN w.total_views > 0 THEN CAST(c.total_coin AS FLOAT) / w.total_views
            ELSE 0
        END AS coin_ratio,
        CASE
            WHEN w.total_views > 0 THEN CAST(f.total_fav AS FLOAT) / w.total_views
            ELSE 0
        END AS fav_ratio,
        CASE
            WHEN w.total_views > 0 THEN CAST(d.total_danmu AS FLOAT) / w.total_views
            ELSE 0
        END AS danmu_ratio,
--         CASE
--             WHEN w.total_views > 0 THEN CAST(wa.total_watch AS FLOAT) / w.total_views
--             ELSE 0
--         END AS watch_ratio,
        ROW_NUMBER() OVER (ORDER BY
                            CASE WHEN w.total_views > 0 THEN
                                 coalesce(CAST(total_likes as float) / w.total_views ,0)
                                +coalesce(CAST(c.total_coin AS FLOAT) / w.total_views,0)
                                +coalesce(CAST(f.total_fav AS FLOAT) / w.total_views ,0)
                                +CASE
                                    WHEN d.total_danmu > 0 THEN
                                        (CAST(d.total_danmu AS FLOAT) / w.total_views)
                                ELSE 0
                                END +
                                +coalesce(x.watch_ratio,0)
                                ELSE 0 END DESC,
                            total_likes DESC,
                            total_views DESC) AS row_num
    FROM
        (SELECT DISTINCT bv AS video_id FROM user_watch_video) v
    LEFT JOIN
        (SELECT bv, COUNT(DISTINCT user_mid) AS total_views FROM user_watch_video GROUP BY bv) w ON v.video_id = w.bv
    LEFT JOIN
        (SELECT bv, COUNT(DISTINCT user_mid) AS total_likes FROM user_like_video GROUP BY bv) l ON v.video_id = l.bv
    left join
        (select bv,count(distinct  user_mid) as total_coin from user_give_coin_video group by bv) c on v.video_id = c.bv
   left join
        (select bv,count(distinct  user_mid) as total_fav from user_collect_video group by bv) f on v.video_id = f.bv
    left join
        (select bv,count(  danmu_id) as total_danmu from b_danmu group by bv) d on v.video_id = d.bv
    left join (SELECT
            v.bv,
            v.duration,
            AVG(uw.view_time) AS avg_watch_duration,
                COUNT(uw.user_mid) AS total_watchers,
        CASE
            WHEN COUNT(uw.user_mid) > 0 THEN CAST( AVG(uw.view_time)  AS FLOAT) / v.duration
            ELSE 0
        END AS watch_ratio
    FROM
        user_watch_video uw
    JOIN
        b_video v ON uw.bv = v.bv
    GROUP BY
        v.bv, uw.bv, v.duration) x on x.bv=d.bv
)
SELECT
    row_num,
    video_id,
    total_views,
    total_likes,
    like_ratio,
    total_coin,
    coin_ratio,
    total_fav,
    fav_ratio,
    total_danmu,
    danmu_ratio,
    total_wat

FROM
    RankedData
WHERE
    row_num BETWEEN pg1 AND pg2
ORDER BY
    row_num
    );
END;
$$
LANGUAGE plpgsql;