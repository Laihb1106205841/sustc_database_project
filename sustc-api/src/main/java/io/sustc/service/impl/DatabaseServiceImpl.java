package io.sustc.service.impl;

import io.sustc.DatabaseConnection.SQLDataSource;
import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.asm.Type;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;



// nearly done
/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    // maybe I can first try MQ?

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

//        private SQLDataSource dataSource;

    @Getter
    private int ThreadNum = 16;
    public DatabaseServiceImpl(){
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        log.info("Available Processors:"+availableProcessors);
        if(availableProcessors * 2 >= ThreadNum){ThreadNum = availableProcessors * 2;}

//        dataSource = new SQLDataSource(ThreadNum);
    }
    private static final int SEQUENCEDEALER = 5000;
    private static final int SEQUENCELIKEBY = 40;
    private static final int INSERTVIDEOLIKE=5;



    @Override
    public List<Integer> getGroupMembers() {
        return List.of(12211612);
//        throw new UnsupportedOperationException("TODO: 12211612");
//        done
    }

    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) {
        log.info("Start to import data");
        log.info("Danmu Num:"+danmuRecords.size());
        log.info("Video Num"+videoRecords.size());
        log.info("User Num"+userRecords.size());


        int ThreadNum = getThreadNum();


        ExecutorService executorService = Executors.newFixedThreadPool(ThreadNum);
        List<Future<?>> futures = new ArrayList<>();

        log.info("ThreadPool Number:"+ThreadNum);
        int proceedWatched = 6;
        Future<?> VideoRecord = executorService.submit(() -> InsertVideoRecord(videoRecords));
        futures.add(VideoRecord);


        for(int i=0;i<proceedWatched;i++){
            int finalI = i;
            Future<?> WatchRecord = executorService.submit
                    (() -> InsertVideoWatched(videoRecords.subList(videoRecords.size() * finalI /proceedWatched,videoRecords.size()* (finalI+1)/proceedWatched)));
            futures.add(WatchRecord);
        }

        Future<?> DanmuRecord = executorService.submit(() -> InsertDanmuRecords(danmuRecords));
        futures.add(DanmuRecord);


//        Future<?> UserFollow = executorService.submit(() -> InsertUserFollow(userRecords));
        Future<?> UserRecord = executorService.submit(() -> InsertBiliUser(userRecords));
        futures.add(UserRecord);

//        Future<?> VideoCoin = executorService.submit(() -> InsertVideoCoin(videoRecords));
//        Future<?> VideoLike = executorService.submit(() -> InsertVideoLike(videoRecords));

//        Future<?> VideoCollect = executorService.submit(() -> InsertVideoCollect(videoRecords));
//        Future<?> VideoWatched = executorService.submit(() -> InsertVideoWatched(videoRecords));

//        Future<?> VideoLike = executorService.submit(() -> InsertVideoLike(videoRecords));
//        Future<?> VideoCoin = executorService.submit(() -> InsertVideoCoin(videoRecords));
//        Future<?> VideoCollect = executorService.submit(() -> InsertVideoCollect(videoRecords));

        int proceedCoinVideo = 2;
        for(int i=0;i<proceedCoinVideo;i++){
            int finalI = i;
            Future<?> CoinVideo = executorService.submit
                    (() -> InsertVideoCoin(videoRecords.subList(videoRecords.size() * finalI /proceedCoinVideo,videoRecords.size()* (finalI+1)/proceedCoinVideo)));
            futures.add(CoinVideo);
        }

        int proceedUserFollow = 8;
        for(int i=0;i<proceedUserFollow;i++){
            int finalI = i;
            Future<?> UserFollow = executorService.submit
                    (() -> InsertUserFollow(userRecords.subList
                            (userRecords.size() * finalI /proceedUserFollow,
                                    userRecords.size()* (finalI+1)/proceedUserFollow)));
            futures.add(UserFollow);
        }

        int proceedVideoLike = 4;
        for(int i=0;i<proceedVideoLike;i++){
            int finalI = i;
            Future<?> VideoLike = executorService.submit
                    (() -> InsertVideoLike(videoRecords.subList(videoRecords.size() * finalI /proceedVideoLike,videoRecords.size()* (finalI+1)/proceedVideoLike)));
            futures.add(VideoLike);
        }
        Future<?> DanmuLike = executorService.submit(() -> InsertDanmuLikedBy(danmuRecords));
        futures.add(DanmuLike);



        int proceedCollected = 4;
        for(int i=0;i<proceedCollected;i++){
            int finalI = i;
            Future<?> CollectRecord = executorService.submit
                    (() -> InsertVideoCollect(videoRecords.subList(videoRecords.size() * finalI /proceedCollected,videoRecords.size()* (finalI+1)/proceedCollected)));
            futures.add(CollectRecord);
        }


        // 等待所有任务执行完毕
        for (Future<?> future : futures) {
            try {
                future.get(); // 等待每个任务完成
            } catch (InterruptedException | ExecutionException e) {
                // 处理异常
                e.printStackTrace();
            }
        }

        log.info("All Thread Publish");

//        List<?> small = ;
//        List<?> back  = ;
        //TODO: gb import your data!!!

//        throw new RuntimeException();
    }


    ///////////////////////////////前方高能///////////////////////////
    // 插入弹幕记录
    private void InsertDanmuRecords( List<DanmuRecord> danmuRecords)  {
        log.info("Start Inserting Danmu");

        String InsertDanmu =
                "INSERT INTO b_danmu(danmu_id, bv, sender_mid, time, content, post_time) " +
                        "values (       ?,      ?   ,   ?,       ? ,      ?,      ?)";
        try(Connection con= dataSource.getConnection();
  PreparedStatement stmt=con.prepareStatement(InsertDanmu)
        ) {
//            con getConnection

            long i = 1;
            for(DanmuRecord danmuRecording : danmuRecords){
                stmt.setLong(1,i);
                i+=1;
                stmt.setString(2,danmuRecording.getBv());
                stmt.setLong(3,danmuRecording.getMid());
                stmt.setFloat(4,danmuRecording.getTime());
                stmt.setString(5,danmuRecording.getContent());
                stmt.setTimestamp(6,danmuRecording.getPostTime());

                stmt.addBatch();

                if(i% SEQUENCEDEALER == 0){
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            stmt.executeBatch();
            stmt.clearBatch();
            stmt.close();
            log.info("Inser Danmu Success");
            con.close();

        }catch (SQLException e){
            log.info("No data from danmu");
//            throw new Runt、imeException(e);
        }


    }
    private void InsertDanmuLikedBy( List<DanmuRecord> danmuRecords)  {
        log.info("start Inserting Like_Danmu");
        String InsertDanmuLiked =
                "INSERT INTO danmu_like (danmu_id, bv, user_mid_like) " +
                        "values (?,?,?)";
        try (Connection con= dataSource.getConnection();
         PreparedStatement stmt= con.prepareStatement(InsertDanmuLiked);
        ) {


            long i = 1;

            for(DanmuRecord danmuRecording : danmuRecords){
                for(long like:danmuRecording.getLikedBy()){
                    stmt.setLong(1,i);
                    stmt.setString(2,danmuRecording.getBv());
                    stmt.setLong(3,like);
                    stmt.addBatch();

                }
                i+=1;
                if(i % SEQUENCELIKEBY == 0){
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            stmt.executeBatch();
            stmt.clearBatch();
            stmt.close();
            con.close();
            log.info("Insert USER_like_Danmu success");
        } catch (SQLException e) {
            log.info("批量插入 USER_like_Danmu 可能没有数据");
//            throw new RuntimeException(e);
        }
    }

    ////////////////////////////VIDEO///////////////////////////

    private void InsertVideoRecord(List<VideoRecord> videoRecords){
        log.info("start to insert video");
        String InsertVideo =
                "INSERT INTO b_video" +
                        "(bv, title, owner_mid, owner_name, " +
                        "commit_time, review_time, public_time, duration," +
                        " description, reviewer, update_time,create_time,is_public,is_posted,is_review) " +
                        "values " +
                        "(?,?,?,?" +
                        ",?,?,?,?" +
                        ",?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,1,1,1)";
        try(Connection con= dataSource.getConnection();
         PreparedStatement stmt=con.prepareStatement(InsertVideo)
        ) {
//            con

            long i = 1;
            for(VideoRecord videoRecording : videoRecords){
                stmt.setString(1,videoRecording.getBv());
                stmt.setString(2,videoRecording.getTitle());
                stmt.setLong(3,videoRecording.getOwnerMid());
                stmt.setString(4,videoRecording.getOwnerName());

                stmt.setTimestamp(5,videoRecording.getCommitTime());
                stmt.setTimestamp(6,videoRecording.getReviewTime());
                stmt.setTimestamp(7,videoRecording.getPublicTime());
                stmt.setFloat(8,videoRecording.getDuration());

                stmt.setString(9,videoRecording.getDescription());
                stmt.setLong(10,videoRecording.getReviewer());
                i+=1;
                stmt.addBatch();
                if(i % 100 == 0){
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            stmt.executeBatch();
            stmt.clearBatch();
            stmt.close();
            con.close();
            log.info("insert video success");
        }catch (SQLException e){
            log.info("批量插入 Video 可能没有数据");
            return;

//            throw new RuntimeException(e);
        }
    }
    private void InsertVideoLike(List<VideoRecord> videoRecords){
        log.info("Start inserting Like_Video");
        String InsertDanmuLiked =
                "INSERT INTO user_like_video (user_mid, bv) " +
                        "values (?,?)";
        try (
                Connection con= dataSource.getConnection() ;
                PreparedStatement stmt = con.prepareStatement(InsertDanmuLiked)
        ) {


            long i = 1;

            for(VideoRecord videoRecording :videoRecords){
                for(long like: videoRecording.getLike()){
                    stmt.setLong(1,like);
                    stmt.setString(2, videoRecording.getBv());
                    stmt.addBatch();
                }
                i+=1;
                if(i % INSERTVIDEOLIKE == 0){
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            stmt.executeBatch();
            stmt.clearBatch();
            stmt.close();
            con.close();
            log.info("Insert Video_like success");
        } catch (SQLException e) {
//            throw new RuntimeException(e);
            log.info("批量插入 USER_like_Video 可能没有数据");
            return;
        }
    }

    private static final int COINPER = 10;
    private void InsertVideoCoin(List<VideoRecord> videoRecords){
        log.info("Start Inserting Video_Coin");
        String InsertVideoCoin =
                "INSERT INTO user_give_coin_video (user_mid, bv) " +
                        "values (?,?)";
        try (Connection con= dataSource.getConnection();
             PreparedStatement stmt = con.prepareStatement(InsertVideoCoin)
        ) {


            long i = 1;

            for(VideoRecord videoRecording :videoRecords){
                for(long Coin: videoRecording.getCoin()){
                    stmt.setLong(1,Coin);
                    stmt.setString(2, videoRecording.getBv());
                    stmt.addBatch();
                }
                i+=1;
                if(i % COINPER== 0){
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            stmt.executeBatch();
            stmt.clearBatch();
            stmt.close();
            con.close();
            log.info("Insert Coin_Video success");
        } catch (SQLException e) {
//            throw new RuntimeException(e);
            log.info("批量插入 Coin_Video 可能没有数据");
        }
    }
    private void InsertVideoCollect(List<VideoRecord> videoRecords){
        log.info("Start Inserting Video_Collecting");
        String InsertUserCollect =
                "INSERT INTO user_collect_video (user_mid, bv) " +
                        "values (?,?)";
//        String InsertUserCollect = "CALL insert_user_collect_video(?, ?);";

        try (Connection con= dataSource.getConnection();
        PreparedStatement stmt =con.prepareStatement(InsertUserCollect)
        ) {
            con.setAutoCommit(false);

            long i = 1;

            for(VideoRecord videoRecording :videoRecords){
                for(long Favourite: videoRecording.getFavorite()){
                    stmt.setLong(1,Favourite);
                    stmt.setString(2, videoRecording.getBv());
                    stmt.addBatch();

                    i+=1;
                }

                if(i % COINPER== 0){
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            stmt.executeBatch();
            stmt.clearBatch();
            stmt.close();
            con.close();
            log.info("Insert Collect_Video success");
        } catch (SQLException e) {
//            throw new RuntimeException(e);
            log.info("批量插入 Collect_Video 可能没有数据");
        }
    }

    private static final int WATCHPERBATCH=1;
    private void InsertVideoWatched(List<VideoRecord> videoRecords){
        log.info("Start inserting Video_Watched");
//        String UserWatchVideo =
//                "INSERT INTO user_watch_video (user_mid, bv,view_time) " +
//                        "values (?,?,?)";

        String UserWatchVideo = "CALL insert_user_watch_video(?, ?, ?)";
        try (Connection con= dataSource.getConnection();
        PreparedStatement stmt = con.prepareStatement(UserWatchVideo);
        ) {



            long m =0;
            for(VideoRecord videoRecording :videoRecords){
                for(int i=0;i<videoRecording.getViewerMids().length;i++){
                    stmt.setLong(1,videoRecording.getViewerMids()[i]);
                    stmt.setString(2, videoRecording.getBv());
                    stmt.setFloat(3,videoRecording.getViewTime()[i]);
                    stmt.addBatch();
                }
                m+=1;
                if(m % WATCHPERBATCH == 0){
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            stmt.executeBatch();
            stmt.clearBatch();
            stmt.close();
            con.close();
            log.info("Insert USER_Watch_Video Success");
        } catch (SQLException e) {
//            throw new RuntimeException(e);
            log.info("批量插入 USER_Watch_Video 可能没有数据");
        }
    }

    private static final int USERINSERT=300;
    private void InsertBiliUser(List<UserRecord> userRecords){
        log.info("Start Inserting User");
        String InsertUser =
                "INSERT INTO b_user" +
                        "(mid, name, sex, birthday, " +
                        "level, sign, identity, password, " +
                        "qq, wechat, create_time, coin) values " +
                        "(?,?,CAST(? AS user_sex),?," +
                        "?,?,CAST(? AS user_identity),?," +
                        "?,?,CURRENT_TIMESTAMP,?)";
        try(Connection con= dataSource.getConnection();
            PreparedStatement stmt = con.prepareStatement(InsertUser)
        ) {
//            con

            long i = 1;
            for(UserRecord userRecord : userRecords){
                stmt.setLong(1,userRecord.getMid());
                stmt.setString(2,userRecord.getName());

                if(String.valueOf(userRecord.getSex()).equals("男") ){
                    stmt.setString(3,"MALE");
                }else if(String.valueOf(userRecord.getSex()).equals("女")){
                    stmt.setString(3,"FEMALE");
                }else
                {stmt.setString(3,"UNKNOWN");}

                if(Objects.equals(userRecord.getBirthday(), "")){
                    stmt.setNull(4, Type.CHAR);}
                else {stmt.setString(4,userRecord.getBirthday());}

                stmt.setShort(5,userRecord.getLevel());

                if(Objects.equals(userRecord.getSign(), "")){
                    stmt.setNull(6,Type.CHAR);
                }else {
                    stmt.setString(6,userRecord.getSign());
                }

                stmt.setString(7, String.valueOf(userRecord.getIdentity()));
                stmt.setString(8,userRecord.getPassword());

                if(Objects.equals(userRecord.getQq(), "")){
                    stmt.setNull(9,Type.CHAR);
                }else {
                    stmt.setString(9,userRecord.getQq());
                }

                if(Objects.equals(userRecord.getWechat(), "")){
                    stmt.setNull(10,Type.CHAR);
                }else {
                    stmt.setString(10,userRecord.getWechat());
                }

                stmt.setInt(11,userRecord.getCoin());
                stmt.addBatch();
                i+=1;
                if(i % USERINSERT == 0){
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            stmt.executeBatch();
            stmt.clearBatch();
            stmt.close();
            con.close();
            log.info("Inserting USER success");

        }catch (SQLException e){
//            throw new RuntimeException(e);
            log.info("批量插入 USER 可能没有数据");
        }
    }

    private static final int FOLLOW = 10;
    private void InsertUserFollow(List<UserRecord> userRecords){
        log.info("Start inserting User_Following");
        String InsertUserFollows ="INSERT into user_follow " +
                "(follower_mid, following_mid)" +
                " values (?   ,   ?)";
        try (Connection con= dataSource.getConnection();
             PreparedStatement stmt = con.prepareStatement(InsertUserFollows)
        ) {


            long m =0;
            for(UserRecord userRecord :userRecords){
                if(userRecord.getFollowing().length!=0){
                    for(long following : userRecord.getFollowing()){

                        stmt.setLong(1,userRecord.getMid());
                        stmt.setLong(2,following);
                        stmt.addBatch();
                    }
                }

                m+=1;
                if(m % FOLLOW == 0){
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            stmt.executeBatch();
            stmt.clearBatch();
            stmt.close();
            con.close();
            log.info("Insert USER_Follow Success");
        } catch (SQLException e) {
//            throw new RuntimeException(e);
            log.info("批量插入 USER_Follow 可能没有数据");
        }
    }
    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void truncate() {
        // You can use the default truncate script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.

        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
