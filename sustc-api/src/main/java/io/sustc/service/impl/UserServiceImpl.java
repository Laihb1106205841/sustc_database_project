package io.sustc.service.impl;

import io.sustc.DatabaseConnection.SQLDataSource;
import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserInfoResp;
import io.sustc.pojo.OAMessage;
import io.sustc.pojo.UserInfo;
import io.sustc.service.UserService;
import io.sustc.service.ValidationCheck.UserValidationCheck;
import lombok.extern.slf4j.Slf4j;
import org.springframework.asm.Type;
import org.springframework.stereotype.Service;

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

import static io.sustc.service.ValidationCheck.UserValidationCheck.HasResultAndSet;


@Service
@Slf4j
public class UserServiceImpl implements UserService {
//    @Autowired
    private SQLDataSource dataSource;

    public UserServiceImpl(){
        dataSource = new SQLDataSource(10);
    }


    /**
     * Delete Account
     * finish
     * insert table: b_user
     * */

    static String RegisterSQL =
            "INSERT INTO " +
                    "b_user(name, sex, birthday," +
                    "sign, identity, password, qq," +
                    "wechat, create_time" +
                    ",mid) " +
                    "VALUES (?, CAST(? AS user_sex), ?, " +
                    "?, CAST(? AS user_identity), ?, ?, " +
                    "?, CURRENT_TIMESTAMP,?)";

    //level default 1
            //coin  default 0
    static String GenerateMidSQL = "SELECT max(mid)+1 from b_user";

    @Override
    public long register(RegisterUserReq req){
        try ( var con = dataSource.getSQLConnection()){
            if(!UserValidationCheck.checkBirthday(req.getBirthday())){
                log.info("birthday");
//                System.out.println(5);
                return -1;}

            PreparedStatement stm1 = con.prepareStatement(GenerateMidSQL);

            //step02:get prepared Statement
            PreparedStatement stmt = con.prepareStatement(RegisterSQL);
            stmt.setString(1,req.getName());

            if(Objects.equals(req.getSex(),"男")||Objects.equals(String.valueOf(req.getSex()),"MALE")){
                stmt.setString(2, "MALE");
            }else if(Objects.equals(req.getSex(),"女")||Objects.equals(String.valueOf(req.getSex()),"FEMALE")){
                stmt.setString(2, "FEMALE");
            }else {
                stmt.setString(2, "UNKNOWN");
            }

            if(Objects.equals(req.getBirthday(),"")||Objects.equals(req.getBirthday(),null)){
                stmt.setNull(3,Type.CHAR);
            }else {stmt.setString(3,req.getBirthday());}

            if(Objects.equals(req.getSign(),"")||Objects.equals(req.getSign(),null)){
                stmt.setNull(4,Type.CHAR);
            }else { stmt.setString(4,req.getSign());}

            stmt.setString(5,"USER");
            stmt.setString(6, req.getPassword());

            if(Objects.equals(req.getQq(),"")||Objects.equals(req.getQq(),null)){
                stmt.setNull(7,Type.CHAR);
            }else{ stmt.setString(7,req.getQq());}

            if(Objects.equals(req.getWechat(),"")||Objects.equals(req.getWechat(),null)){
                stmt.setNull(8,Type.CHAR);
            }else{ stmt.setString(8,req.getWechat());}

            ResultSet resultSet = stm1.executeQuery();

            long mid = 1;

            // 使用 next() 方法将光标移动到结果集的第一行
            if (resultSet.next()) {
                // 在这里获取数据，例如：
                mid = resultSet.getLong(1);
            }

            resultSet.next();
            long newId = Math.max(mid,1);
            stmt.setLong(9,newId);

            stmt.executeUpdate();
//            System.out.println(7);

//            long generatedMid = -1;
//            // 获取生成的键
//            ResultSet generatedKeys = stmt.getGeneratedKeys();
//            if (generatedKeys.next()) {
//               generatedMid = generatedKeys.getLong(1);
//
//            } else {
//                return -1;
//            }
//            return generatedMid;

            return newId;
        } catch (SQLException e) {
//            throw new ConnectionToDatabaseException(e);
            log.info("SQL");
            return -1;
        }
    }

//批处理
    //execute batch


    /**
     * Delete Account
     * finish
     * TODO: waijian and jilian, try to not use them
     * */

    @Override
    public boolean deleteAccount(AuthInfo auth, long mid){

        if(mid<=0){return false;}

        //OA Authentication
//        UserValidationCheck UVC = new UserValidationCheck(dataSource);
        OAMessage oaMessage = checkAuthInvalid(auth);
        if(!oaMessage.isAuthIsValid()){return false;}
        String identity = oaMessage.getIdentity();
        String iden = null;

        String select = "select identity from b_user where mid = ?";
        try (Connection con = dataSource.getSQLConnection()){
            PreparedStatement stmt = con.prepareStatement(select);
            stmt.setLong(1,mid);
            ResultSet resultSet = stmt.executeQuery();
            if(resultSet.next()){
                iden = String.valueOf(resultSet.getString("identity"));
            }else {return false;}
        }catch (SQLException e){return false;}

        if(identity.equals("SUPERUSER") && oaMessage.getMid()!=mid && iden.equals("SUPERUSER")){
            return false;
        }

        //valid
        if((identity.equals("USER") && oaMessage.getMid()==mid )
                ||(identity.equals("SUPERUSER") )){
            Connection con = null;
            String GoToHistory =
                    "INSERT INTO history_user " +
                            "(mid, name, sex, birthday, " +
                            "level, sign, identity, password," +
                            " qq, wechat, delete_time,create_time, " +
                            "coin) values (" +
                            "?,?,CAST(? AS user_sex),?," +
                            "?,?,CAST(? AS user_identity),?," +
                            "?,?,CURRENT_TIMESTAMP,?," +
                            "?)";
            String deleteDanmu ="delete from b_danmu where sender_mid = ?";
            String deleteVideo ="delete from b_video where owner_mid = ?";
            String deleteFollowing = "delete from user_follow where follower_mid = ?";
            String deleteFollower = "delete from user_follow where following_mid = ?";
            String deleteUserWatchVideo = "delete from user_watch_video where user_mid = ?";
            String deleteDanmuLike = "delete from danmu_like where user_mid_like = ?";
            String deleteCoinVideo = "delete from user_give_coin_video where user_mid =?";
            String deleteCollectVideo = "delete from user_collect_video where user_mid = ?";
            String deleteLikeVideo = "delete from user_like_video where user_mid = ?";

            String deleteAccount = "DELETE from b_user where mid = ?";

            //                con = dataSource.getSQLConnection();
//                con.setAutoCommit(false);

//                PreparedStatement stmt = con.prepareStatement(GoToHistory);
//                stmt.setLong(1,oaMessage.getMid());
//                stmt.setString(2,oaMessage.getName());
//                stmt.setString(3, String.valueOf(oaMessage.getSex()));
//                if(Objects.equals(oaMessage.getBirthday(),null)){
//                    stmt.setNull(4, org.springframework.asm.Type.CHAR);
//                }else {stmt.setString(4, oaMessage.getBirthday());}
//
//                stmt.setShort(5,oaMessage.getLevel());
//
//                if(Objects.equals(oaMessage.getSign(),null)){
//                    stmt.setNull(6, Type.CHAR);
//                }else {stmt.setString(6,oaMessage.getSign());}
//
//                stmt.setString(7, String.valueOf(oaMessage.getIdentity()));
//                stmt.setString(8,oaMessage.getPassword());
//
//                stmt.setString(9,oaMessage.getQq());
//                stmt.setString(10,oaMessage.getWechat());
//                stmt.setTimestamp(11, oaMessage.getTimestamp());
//                stmt.setInt(12,oaMessage.getCoin());
//                stmt.executeUpdate();

//                PreparedStatement stmt = null;
            ExecutorService executorService = Executors.newFixedThreadPool(8);

            executorService.submit(() -> executeDeleteOperation(deleteAccount, mid));

            executorService.submit(() -> executeDeleteOperation(deleteDanmuLike, mid));
            executorService.submit(() -> executeDeleteOperation(deleteCoinVideo, mid));
            executorService.submit(() -> executeDeleteOperation(deleteCollectVideo, mid));
            executorService.submit(() -> executeDeleteOperation(deleteLikeVideo, mid));

            executorService.submit(() -> executeDeleteOperation(deleteDanmu, mid));
            executorService.submit(() -> executeDeleteOperation(deleteVideo, mid));
            executorService.submit(() -> executeDeleteOperation(deleteFollowing, mid));
            executorService.submit(() -> executeDeleteOperation(deleteFollower, mid));
            executorService.submit(() -> executeDeleteOperation(deleteUserWatchVideo, mid));
//                stmt = con.prepareStatement(deleteAccount);
//                stmt.setLong(1,mid);
//                stmt.executeUpdate();
//                stmt = con.prepareStatement(deleteDanmu);
//                stmt.setLong(1,mid);
//                stmt.executeUpdate();
//                stmt = con.prepareStatement(deleteVideo);
//                stmt.setLong(1,mid);
//                stmt.executeUpdate();
//                stmt = con.prepareStatement(deleteFollowing);
//                stmt.setLong(1,mid);
//                stmt.executeUpdate();
//                stmt = con.prepareStatement(deleteFollower);
//                stmt.setLong(1,mid);
//                stmt.executeUpdate();
//
//
//                stmt = con.prepareStatement(deleteUserWatchVideo);
//                stmt.setLong(1,mid);
//                stmt.executeUpdate();
//                stmt = con.prepareStatement(deleteDanmuLike);
//                stmt.setLong(1,mid);
//                stmt.executeUpdate();
//                stmt = con.prepareStatement(deleteCoinVideo);
//                stmt.setLong(1,mid);
//                stmt.executeUpdate();
//                stmt = con.prepareStatement(deleteCollectVideo);
//                stmt.setLong(1,mid);
//                stmt.executeUpdate();
//                stmt = con.prepareStatement(deleteLikeVideo);
//                stmt.setLong(1,mid);
//                stmt.executeUpdate();

//                con.commit();

            //数据库，我真的好讨厌你啊！！！天天报错，哈基米！！！


//                stmt.close();
//                con.close();

//                con.setAutoCommit(false);
//                PreparedStatement stm1 = con.prepareStatement(deleteAccount);
//                stm1.setLong(1,mid);
//                stm1.executeUpdate();
//                con.commit();
            return true;


        }

        return false;
    }

    private void executeDeleteOperation(String query, long mid) {
        try (Connection con = dataSource.getSQLConnection()) {
            try (PreparedStatement stmt = con.prepareStatement(query)) {
                stmt.setLong(1, mid);
                stmt.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Follow Account
     * finish
     * */
    @Override
    public boolean follow(AuthInfo auth, long followeeMid){
        if(followeeMid<=0){return false;}
        OAMessage oaMessage = checkAuthInvalid(auth);
        if(!oaMessage.isAuthIsValid()){return false;}

        String findFollowing = "Select mid from b_user where mid = ?";
        String ifFollowing = "SELECT * from user_follow where " +
                "following_mid = ? and follower_mid = ?";
//        Connection con = null;

        boolean hasFollowingUser = false;
        boolean FollowedBefore = true;

        try (Connection con = dataSource.getSQLConnection()) {

            con.setAutoCommit(false);
            PreparedStatement stm1 = con.prepareStatement(findFollowing);
            PreparedStatement stm2 = con.prepareStatement(ifFollowing);
            stm1.setLong(1,followeeMid);

            stm2.setLong(1,followeeMid);
            stm2.setLong(2,oaMessage.getMid());

            con.commit();

            ResultSet resultSet1 = stm1.executeQuery();//do we have user in b_user
            ResultSet resultSet2 = stm2.executeQuery();//do we have follow before

            //                System.out.println(resultSet1.getLong(1));
            hasFollowingUser = resultSet1.next();//true while has
            FollowedBefore= resultSet2.next();
            stm1.close();
            stm2.close();
            con.close();
        } catch (SQLException e) {

            return false;
        }
        if(hasFollowingUser && !FollowedBefore){
            String followSQL = "INSERT into user_follow (follower_mid, following_mid) " +
                    "values (?,?)";

            try(Connection con = dataSource.getSQLConnection()) {

                PreparedStatement stm1 = con.prepareStatement(followSQL);
                stm1.setLong(1,oaMessage.getMid());
                stm1.setLong(2,followeeMid);
                stm1.executeUpdate();

                stm1.close();
                con.close();
                return true;
            } catch (SQLException e) {
                return false;
            }
        }
        if(hasFollowingUser && FollowedBefore){
            String followSQL = "delete from user_follow where follower_mid = ? and following_mid = ?";

            try(Connection con = dataSource.getSQLConnection()) {

                PreparedStatement stm1 = con.prepareStatement(followSQL);
                stm1.setLong(1,oaMessage.getMid());
                stm1.setLong(2,followeeMid);
                stm1.executeUpdate();

                stm1.close();
                con.close();
                return true;
            } catch (SQLException e) {
                return false;
            }
        }

        return false;

    }

//not done
//@Override
    public UserInfoResp getUserInfos(long mid){
        if(mid<= 0){return null;}
        UserInfoResp userInfoResp = null;

        String getUser = "SELECT mid,coin from b_user where mid = ?";
        String getUserFollowing = "SELECT following_mid from user_follow where follower_mid = ?";
        String getUserFollower = "SELECT follower_mid from user_follow where following_mid = ?";
        String getUserWatched = "SELECT bv from user_watch_video where user_mid = ?";

        Connection con = null;
        try {
            con = dataSource.getSQLConnection();
//            PreparedStatement stm = con.prepareStatement();
            //1 Thread 1 select1
            PreparedStatement stm1 = con.prepareStatement(getUser);

            stm1.setLong(1,mid);
            ResultSet resultSet = stm1.executeQuery();
            if(!resultSet.next()){
                log.info("can't find userid");
                return null;}
            long userId = resultSet.getLong("mid");
            int  coin   = resultSet.getInt("coin");
            stm1.close();

            //2 Thread 2 select2
            List<Long> result2 = new ArrayList<>();
            PreparedStatement stm2 = con.prepareStatement(getUserFollowing);
            stm2.setLong(1,mid);
            ResultSet resultSet2 = stm2.executeQuery();
//            if(!resultSet2.next()){return null;}may be there are no follow
            if(resultSet2.next()){
                do{
                    result2.add(resultSet2.getLong("following_mid"));
                }while(resultSet2.next());
            }

//            Long[] following_Mids = null;
//            if (result2 != null) {
//                following_Mids = result2.toArray(new Long[result2.size()]);
//            }
            long [] following = new long[result2.size()];

            for (int i = 0; i < result2.size(); i++) {
                following[i] = result2.get(i);
            }
            stm2.close();

            //3 Thread 3 select3
            List<Long> result3 = new ArrayList<>();
            PreparedStatement stm3 = con.prepareStatement(getUserFollower);
            stm3.setLong(1,mid);
            ResultSet resultSet3 = stm3.executeQuery();
            if(resultSet3.next()){
                do{
                    result3.add(resultSet3.getLong("follower_mid"));
                }while(resultSet3.next());
            }//may be there are no follow

//            Long[] follower_Mids = null;
//            if (result3 != null) {
//                follower_Mids = result3.toArray(new Long[result3.size()]);
//            }
            long [] follower = new long[result3.size()];
            for (int i = 0; i < result3.size(); i++) {
                follower[i] = result3.get(i);
            }
            stm3.close();

            //4 Thread 4 select4
            List<String> result4 = new ArrayList<>();
            PreparedStatement stm4 = con.prepareStatement(getUserWatched);
            stm4.setLong(1,mid);
            ResultSet resultSet4 = stm4.executeQuery();
            if(resultSet4.next()){
                do{
                    result4.add(resultSet4.getString("bv"));
                }while (resultSet4.next());
            }

            String[] UserWatched = null;
            if (result4 != null) {
                UserWatched = result4.toArray(new String[result4.size()]);
            }
            stm4.close();

            String getUserLiked = "SELECT bv from user_like_video where user_mid = ? ";

            PreparedStatement stUL = con.prepareStatement(getUserLiked);
            stUL.setLong(1, mid);

            ResultSet resultSetUL = stUL.executeQuery();
            List<String> resultUL = new ArrayList<>();
            if(resultSetUL.next()){
                do{
                    resultUL.add(resultSetUL.getString("bv"));
                }while (resultSetUL.next());
            }

            String[] UserLike = resultUL.toArray(new String[resultUL.size()]);
            stUL.close();


            String getUserCollected = "SELECT bv from user_collect_video where user_mid = ? ";
            PreparedStatement stUC = con.prepareStatement(getUserCollected);
            stUC.setLong(1,mid);
            ResultSet resultSetUC = stUC.executeQuery();
            List<String> resultUC = new ArrayList<>();

            if(resultSetUC.next()){
                do{
                    resultUC.add(resultSetUC.getString("bv"));
                } while (resultSetUC.next());
            }
            String[] UserCollected = resultUC.toArray(new String[resultUC.size()]);
            stUC.close();


            String getUserPosted ="SELECT bv from b_video where owner_mid = ? ";
            PreparedStatement pre = con.prepareStatement(getUserPosted);
            pre.setLong(1, mid);

            ResultSet resultSetP = pre.executeQuery();
            List<String> result5 = new ArrayList<>();

            if(resultSetP.next()){
                do {
                    result5.add(resultSetP.getString("bv"));
                } while (resultSetP.next());
            }

            String[] UserPosted= result5.toArray(new String[result5.size()]);
            pre.close();

            userInfoResp = new UserInfoResp(userId,coin,following,follower,UserWatched,UserLike,UserCollected,UserPosted);

            return userInfoResp;
        } catch (SQLException e) {
            return null;
        }
    }
    @Override
    public UserInfoResp getUserInfo(long mid) {
//        log.info("je");
        //MultiThread
        ExecutorService executorService = Executors.newFixedThreadPool(7);
        List<Future<?>> futures = new ArrayList<>();
        try {

            Future<UserInfo> userFuture = executorService.submit(() -> selectUser(mid));
            futures.add(userFuture);
            Future<long[]> followingMidsFuture = executorService.submit(() -> selectUserFollowing(mid));
            futures.add(followingMidsFuture);
            Future<long[]> followerMidsFuture = executorService.submit(() -> selectUserFollower(mid));
            futures.add(followerMidsFuture);
            Future<String[]> MidsWatchFuture = executorService.submit(() -> selectUserWatched(mid));
            futures.add(MidsWatchFuture);
            Future<String[]> VideoLikeFuture = executorService.submit(() -> selectUserLikedVideos(mid));
            futures.add(VideoLikeFuture);
            Future<String[]> VideoPosted = executorService.submit(() -> selectUserPostedVideos(mid));
            futures.add(VideoPosted);
            Future<String[]> VideoCollected = executorService.submit(() -> selectUserCollectedVideos(mid));
            futures.add(VideoCollected);


//            // 等待所有任务执行完毕
//            for (Future<?> future : futures) {
//                try {
//                    future.get(); // 等待每个任务完成
//                } catch (InterruptedException | ExecutionException e) {
//                    // 处理异常
//                    e.printStackTrace();
//                }
//            }

            log.info("01");

            // Get results from thread 1
            UserInfo user = userFuture.get();
            long userId = 0;
            int coin = 0;
            // Process results
            if (user != null) {
                userId = user.getUserId();
                coin = user.getCoin();
                // Process user details
            }else {return null;}
            log.info("01");

            // Get results from thread 2
            long[] followingMids = followingMidsFuture.get();
            log.info("02");

            // Get results from thread 3
            long[] followerMids  = followerMidsFuture.get();
            log.info("03");

            // Get results from thread 4
            String[] Watchbv = MidsWatchFuture.get();
            log.info("04");

            // Get results from thread 5
            String[] Likebv = VideoLikeFuture.get();
            log.info("05");

            // Get results from thread 6
            String[] Postedbv = VideoPosted.get();
            log.info("06");

            // Get results from thread 7
            String[] Collectedbv = VideoCollected.get();
            log.info("07");

            UserInfoResp userInfoResp = new UserInfoResp(
                    userId,coin,followingMids,followerMids,Watchbv,Likebv,Collectedbv,Postedbv
            );
            log.info("08");


            log.info("get user info");
            log.info(userInfoResp.toString());
            return userInfoResp;

        } catch (InterruptedException | ExecutionException e) {

            log.info("shit happens");
            return null;
        } finally {
            executorService.shutdown();
        }
    }


    /*
    * done for selectUser
    * */
    private UserInfo selectUser(long mid) {
        String getUser = "SELECT mid,coin from b_user where mid = ? ";
        try (Connection con = dataSource.getSQLConnection();
             PreparedStatement stm1 = con.prepareStatement(getUser)) {
            stm1.setLong(1, mid);
            ResultSet resultSet = stm1.executeQuery();
            if (!resultSet.next()) {
                return null;
            }

            long userId = resultSet.getLong("mid");
            int coin = resultSet.getInt("coin");
            con.close();
            return new UserInfo(userId, coin);
        } catch (SQLException e) {
            return null;
        }
    }

    private long[] selectUserFollowing(long mid) {
        String getUserFollowing = "SELECT following_mid from user_follow where follower_mid = ? ";
        try (Connection con = dataSource.getSQLConnection();
             PreparedStatement stm2 = con.prepareStatement(getUserFollowing)) {

            stm2.setLong(1, mid);

            ResultSet resultSet1 = stm2.executeQuery();

            List<Long> result = new ArrayList<>();

            while (resultSet1.next()) {
                result.add(resultSet1.getLong("following_mid"));
            }

            long [] a = new long[result.size()];

            for (int i = 0; i < result.size(); i++) {
                a[i] = result.get(i);
            }
//            Long[] array = result.toArray(new Long[result.size()]);
            con.close();
            return a;
        } catch (SQLException e) {
            return null;
        }
    }
    private long[] selectUserFollower(long mid) {
        String getUserFollower = "SELECT follower_mid from user_follow where following_mid = ? ";
        try (Connection con = dataSource.getSQLConnection();
             PreparedStatement stm3 = con.prepareStatement(getUserFollower)) {

            stm3.setLong(1, mid);

            ResultSet resultSet3 = stm3.executeQuery();
            List<Long> result3 = new ArrayList<>();

            if (!resultSet3.next()) {
                return null;
            }

            while (resultSet3.next()) {
                result3.add(resultSet3.getLong("follower_mid"));
            }

            long[] a = new long[result3.size()];
            for (int i = 0; i < result3.size(); i++) {
                a[i] = result3.get(i);
            }
            con.close();
            return a;
//                    result3.toArray(new Long[result3.size()]);
        } catch (SQLException e) {
            return null;
        }
    }
    private String[] selectUserWatched(long mid) {
        String getUserWatched = "SELECT bv from user_watch_video where user_mid = ? ";
        try (Connection con = dataSource.getSQLConnection();
             PreparedStatement stm4 = con.prepareStatement(getUserWatched)) {

            stm4.setLong(1, mid);

            ResultSet resultSet4 = stm4.executeQuery();
            List<String> result4 = new ArrayList<>();

            if (!resultSet4.next()) {
                return null;
            }

            while (resultSet4.next()) {
                result4.add(resultSet4.getString("bv"));
            }
            con.close();
            return result4.toArray(new String[result4.size()]);
        } catch (SQLException e) {
            return null;
        }
    }
    private String[] selectUserLikedVideos(long mid) {
        String getUserLikedVideos = "SELECT bv from user_like_video where user_mid = ? ";
        try (Connection con = dataSource.getSQLConnection();
             PreparedStatement stm = con.prepareStatement(getUserLikedVideos)) {

            stm.setLong(1, mid);

            ResultSet resultSet = stm.executeQuery();
            List<String> result = new ArrayList<>();

            if (!resultSet.next()) {
                return null;
            }

            do {
                result.add(resultSet.getString("bv"));
            } while (resultSet.next());
            con.close();
            return result.toArray(new String[result.size()]);
        } catch (SQLException e) {
            return null;
        }
    }

    private String[] selectUserPostedVideos(long mid) {
        String getUserPostedVideos = "SELECT bv from b_video where owner_mid = ? ";
        try (Connection con = dataSource.getSQLConnection();
             PreparedStatement stm = con.prepareStatement(getUserPostedVideos)) {

            stm.setLong(1, mid);

            ResultSet resultSet = stm.executeQuery();
            List<String> result = new ArrayList<>();

            if (!resultSet.next()) {
                return null;
            }

            do {
                result.add(resultSet.getString("bv"));
            } while (resultSet.next());
            con.close();
            return result.toArray(new String[result.size()]);
        } catch (SQLException e) {
            return null;
        }
    }
    private String[] selectUserCollectedVideos(long mid) {
        String getUserCollectedVideos = "SELECT bv from user_collect_video where user_mid = ? ";
        try (Connection con = dataSource.getSQLConnection();
             PreparedStatement stm = con.prepareStatement(getUserCollectedVideos)) {

            stm.setLong(1, mid);

            ResultSet resultSet = stm.executeQuery();
            List<String> result = new ArrayList<>();

//            if (!resultSet.next()) {
//                return null;
//            }

            do {
                result.add(resultSet.getString("bv"));
            } while (resultSet.next());
            con.close();
            return result.toArray(new String[result.size()]);
        } catch (SQLException e) {
            return null;
        }
    }




//    static String Auth2A = "SELECT * from b_user where ? = ? and ? = ?";

//    static String AuthA = "SELECT * from b_user where ? = ?";
//    static String Auth3A = "SELECT * from b_user where mid = ? and wechat = ? and qq = ?";

    ////////////////////NIUNIUNIUNIUNIHDJSAJFLAFJ;EVFJK DFHASFKLDASHJCLGKZ DFR//////////////////////////////////////////////////

    public OAMessage checkAuthInvalid(AuthInfo auth){


        OAMessage message = new OAMessage();
        if(auth.getMid()<=0){//don't have mid
            if(auth.getPassword()==null){//don't have password
                if(auth.getQq()!=null && auth.getWechat()!=null){ //2A
                    try(Connection con = dataSource.getSQLConnection()) {
//                        Connection con = dataSource.getSQLConnection();
                        String Auth2A = "SELECT * from b_user where qq = ? and wechat= ?  ";
                        PreparedStatement stmt = con.prepareStatement(Auth2A);
                        stmt.setString(1,auth.getQq());
                        stmt.setString(2,auth.getWechat());

                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person

                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()!=null && auth.getWechat()==null){//qq 1A
                    try(Connection con = dataSource.getSQLConnection()){
//                        Connection con = dataSource.getSQLConnection();
                        String AuthA = "SELECT * from b_user where qq = ? ";
                        PreparedStatement stmt = con.prepareStatement(AuthA);

                        stmt.setString(1,auth.getQq());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()==null && auth.getWechat()!=null){
                    try(Connection con = dataSource.getSQLConnection()) {
//                        Connection con = dataSource.getSQLConnection();
                        String AuthA = "SELECT * from b_user where wechat = ?  ";
                        PreparedStatement stmt = con.prepareStatement(AuthA); //1A
                        stmt.setString(1, auth.getWechat());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()==null && auth.getWechat()==null){  //0A
                    // nmdx,shen me dou mei you ni deng ge ji er
                    return message;
                }
            }
            else {//has password
                if(auth.getQq()!=null && auth.getWechat()!=null){ //3A
                    try(Connection con = dataSource.getSQLConnection()) {
//                        Connection con = dataSource.getSQLConnection();
                        String Auth3A = "SELECT * from b_user " +
                                "where wechat = ? and qq = ? and password = ? ";
                        PreparedStatement stmt = con.prepareStatement(Auth3A);

                        stmt.setString(1,auth.getWechat());

                        stmt.setString(2,auth.getQq());

                        stmt.setString(3,auth.getPassword());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()==null && auth.getWechat()!=null){  //2A
                    try(Connection con = dataSource.getSQLConnection()) {
//                        Connection con = dataSource.getSQLConnection();
                        String Auth2A = "SELECT * from b_user " +
                                "where wechat = ? and password =?  ";
                        PreparedStatement stmt = con.prepareStatement(Auth2A);

                        stmt.setString(1,auth.getWechat());

                        stmt.setString(2,auth.getPassword());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()!=null && auth.getWechat()==null){//2A
                    try(Connection con = dataSource.getSQLConnection()) {

                        String Auth2A = "SELECT * from b_user " +
                                "where qq = ? and password =? ";
                        PreparedStatement stmt = con.prepareStatement(Auth2A);

                        stmt.setString(1,auth.getQq());

                        stmt.setString(2,auth.getPassword());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()==null && auth.getWechat()==null){//A
                    return message;//only password
                }
            }

        }
        else {//HAVE MID
            if(auth.getPassword()==null){//don't have password
                if(auth.getQq()!=null && auth.getWechat()!=null){//3A
                    try(Connection con = dataSource.getSQLConnection()) {

                        String Auth3A = "SELECT * from b_user " +
                                "where mid = ? and wechat = ? and password =? ";
                        PreparedStatement stmt = con.prepareStatement(Auth3A);

                        stmt.setLong(1,auth.getMid());
                        stmt.setString(2,auth.getWechat());
                        stmt.setString(3,auth.getPassword());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()==null && auth.getWechat()!=null){//2A
                    try(Connection con = dataSource.getSQLConnection()) {

                        String Auth2A = "SELECT * from b_user " +
                                "where mid = ? and wechat =? ";
                        PreparedStatement stmt = con.prepareStatement(Auth2A);

                        stmt.setLong(1,auth.getMid());

                        stmt.setString(2,auth.getWechat());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()!=null && auth.getWechat()==null){//2A
                    try(Connection con = dataSource.getSQLConnection()) {

                        String Auth2A = "SELECT * from b_user " +
                                "where mid = ? and qq =? ";
                        PreparedStatement stmt = con.prepareStatement(Auth2A);

                        stmt.setLong(1,auth.getMid());

                        stmt.setString(2,auth.getQq());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()==null && auth.getWechat()==null){//A
                    return message;
                }
            }else {// have password
                if(auth.getQq()!=null && auth.getWechat()!=null){//4A
                    try(Connection con = dataSource.getSQLConnection()) {
//                        System.out.println("4A");


//                        System.out.println(dataSource.toStrin
                        String Auth4A = "SELECT * from b_user " +
                                "where mid = ? and wechat = ? and qq = ? and password = ? ";

                        PreparedStatement stmt = con.prepareStatement(Auth4A);

                        stmt.setLong(1,auth.getMid());

                        stmt.setString(2,auth.getWechat());

                        stmt.setString(3,auth.getQq());

                        stmt.setString(4,auth.getPassword());

                        ResultSet resultSet = stmt.executeQuery();
//                        if(!resultSet.next()){System.out.println("hey");}
//                        System.out.println(1);
//                        System.out.println(resultSet.toString());
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()==null && auth.getWechat()!=null){//3A
                    try (Connection con = dataSource.getSQLConnection()) {

                        String Auth3A = "SELECT * from b_user " +
                                "where mid = ? and wechat =? and password =? ";
                        PreparedStatement stmt = con.prepareStatement(Auth3A);

                        stmt.setLong(1,auth.getMid());

                        stmt.setString(2,auth.getWechat());

                        stmt.setString(3,auth.getPassword());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()!=null && auth.getWechat()==null){//3A
                    try (Connection con = dataSource.getSQLConnection()) {

                        String Auth3A = "SELECT * from b_user " +
                                "where mid = ? and qq =? and password=? ";
                        PreparedStatement stmt = con.prepareStatement(Auth3A);

                        stmt.setLong(1,auth.getMid());

                        stmt.setString(2,auth.getQq());

                        stmt.setString(3,auth.getPassword());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()==null && auth.getWechat()==null){//2A
                    try (Connection con = dataSource.getSQLConnection()) {

                        String Auth2A = "SELECT * from b_user " +
                                "where mid = ? and password =? ";
                        PreparedStatement stmt = con.prepareStatement(Auth2A);

                        stmt.setLong(1,auth.getMid());

                        stmt.setString(2,auth.getPassword());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person
                    } catch (SQLException e) {
                        return message;
                    }
                }
            }
        }
        return message;
    }
}
