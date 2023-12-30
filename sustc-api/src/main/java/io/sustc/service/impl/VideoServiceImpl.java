package io.sustc.service.impl;

import io.sustc.DatabaseConnection.SQLDataSource;
import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.pojo.OAMessage;
import io.sustc.pojo.VideoMessage;
import io.sustc.service.VideoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.asm.Type;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.sustc.service.ValidationCheck.UserValidationCheck.HasResultAndSet;

@Service
@Slf4j
public class VideoServiceImpl implements VideoService {
    private SQLDataSource dataSource;

    public VideoServiceImpl(){
        dataSource = new SQLDataSource(10);
    }
    public String postVideo(AuthInfo auth, PostVideoReq req){

        if(req.getTitle().isEmpty() || req.getTitle()==null){
            log.info("No title");
            return null;}

        if(req.getPublicTime().toLocalDateTime().isBefore(LocalDateTime.now())){
            log.info("before we start");return null;
        }
        if(req.getDuration() < 10){
            log.info("短视频！b站不会变味！");return null;
        }

        //check OA
        OAMessage oaMessage = checkAuthInvalid(auth);
        if(!oaMessage.isAuthIsValid()){
            log.info("Video post OA failed");
            return null;}

        String CHA_CHONG ="SELECT title from b_video where title =? and owner_mid =?";
        try (Connection con = dataSource.getSQLConnection();
             PreparedStatement preparedStatement = con.prepareStatement(CHA_CHONG)){
            preparedStatement.setString(1,req.getTitle());
            preparedStatement.setLong(2,oaMessage.getMid());
            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){log.info("video exist！");return null;}

            String NewBV = "SELECT LEFT(regexp_replace(translate(uuid_generate_v4()::text, '-', '')," +
                    " '[^a-zA-Z0-9]', '', 'g'), 9) AS custom_length_uuid";

            String bv = "BV1";

            PreparedStatement stmt = con.prepareStatement(NewBV);
            ResultSet resultSet1 = stmt.executeQuery();
            if(resultSet1.next()){
                bv += resultSet1.getString(1);
            }
            stmt.close();
            log.info(bv);

            log.info("start to insert video");
            String InsertVideo =
                    "INSERT INTO b_video" +
                            "(bv, title, owner_mid, owner_name, " +
                            "commit_time, review_time, public_time, duration," +
                            " description, reviewer, update_time,create_time,is_public,is_posted,is_review) " +
                            "values " +
                            "(?,?,?,?" +
                            ",CURRENT_TIMESTAMP,null,null,?" +
                            ",?,null,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,0,1,0)";

            PreparedStatement stmt2 = con.prepareStatement(InsertVideo);
            stmt2.setString(1,bv);
            stmt2.setString(2, req.getTitle());
            stmt2.setLong(3,oaMessage.getMid());
            stmt2.setString(4,oaMessage.getName());

            stmt2.setFloat(5,req.getDuration());
            if(req.getDescription().isEmpty()||req.getDescription()==null){
                stmt2.setNull(6, Type.CHAR);
            }else {stmt2.setString(6,req.getDescription());}

            log.info("Get Ready");
            stmt2.executeUpdate();


            stmt2.close();
            con.close();


        }catch (SQLException e){
            log.info("SQL?");
            return null;

        }
        return null;
    }
    public boolean deleteVideo(AuthInfo auth, String bv){
        //check OA
        OAMessage oaMessage = checkAuthInvalid(auth);
        if(!oaMessage.isAuthIsValid()){
            log.info("OA failed");
            return false;
        }

        String GetBv = "(select bv,owner_mid from b_video where bv = ?)";



        try (Connection con = dataSource.getSQLConnection();
             PreparedStatement preparedStatement = con.prepareStatement(GetBv)) {

            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                long own = resultSet.getLong(2);
                if(own!=oaMessage.getMid()){
                    log.info("Two man not the same!");return false;
                }

                String sql = "DELETE from b_video where bv = ? and owner_mid =?";
                PreparedStatement send = con.prepareStatement(sql);
                send.setString(1,bv);
                send.setLong(2,oaMessage.getMid());

                log.info("准备删除");
                send.executeUpdate();
                send.close();
                log.info("删除成功");
                //use trigger to delete the last
                // 升级：有时间了一定要来这里优化

                return true;

            }else {
                log.info("Can't find video");
                return false;
            }

        } catch (SQLException e) {
            log.info("SQL 报错");
           return false;
        }

    }
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req){
        if(req.getTitle().isEmpty() || req.getTitle()==null){
            log.info("No title");
            return false;}

        if(req.getPublicTime().toLocalDateTime().isBefore(LocalDateTime.now())){
            log.info("before we start");return false;
        }
        if(req.getDuration() < 10){
            log.info("短视频！b站不会变味！");return false;
        }

        //check OA
        OAMessage oaMessage = checkAuthInvalid(auth);
        if(!oaMessage.isAuthIsValid()){
            log.info("OA failed");
            return false;
        }

//        String FindVideo = "SELECT bv,owner_mid from b_video where bv = ?";
        try {
            Connection con = dataSource.getSQLConnection();
            String CHA_CHONG ="SELECT title,bv,owner_mid,duration,description,public_time from b_video where title =? and owner_mid =?";
            PreparedStatement preparedStatement = con.prepareStatement(CHA_CHONG);
            preparedStatement.setString(1,req.getTitle());
            preparedStatement.setLong(2,oaMessage.getMid());
            ResultSet resultSet = preparedStatement.executeQuery();
            if(!resultSet.next()){log.info("video not exist");return false;}//can not find
            if(resultSet.getRow()==2){log.info("video exist twice!");return false;}

            long user_mid = resultSet.getLong("owner_mid");
            if(user_mid!=oaMessage.getMid()){log.info("not the owner");return false;}
            float duration = resultSet.getFloat("duration");
            if(duration != req.getDuration()){log.info("duration not the same");return false;}

            String desc = resultSet.getString("description");
            String tit = resultSet.getString("title");
            Timestamp ts = resultSet.getTimestamp("public_time");
            if(tit.equals(req.getTitle()) && desc.equals(req.getDescription())
                    && duration==req.getDuration() && ts.equals(req.getPublicTime())){
                log.info("the same, I want to quit CSE!");
                return false;
            }

            String alter = "update b_video set title=?,description=?,duration=?,public_time=?,update_time=CURRENT_TIMESTAMP";

            PreparedStatement stmt =con.prepareStatement(alter);
            stmt.setString(1,req.getTitle());
            stmt.setString(2,req.getDescription());
            stmt.setFloat(3,req.getDuration());
            stmt.setTimestamp(4,req.getPublicTime());
            stmt.executeUpdate();
            return true;

        } catch (SQLException e) {
            throw new RuntimeException(e);
//            return null;
        }
        }
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum){
        if(keywords.isEmpty() || keywords.equals("")){log.info("null key");return null;}
        if(pageSize<=0){log.info("page size");return null;}
        if(pageNum<=0){log.info("page num");return null;}

        //check OA
        OAMessage oaMessage = checkAuthInvalid(auth);
        if(!oaMessage.isAuthIsValid()){
            log.info("search video OA failed");
            return null;
        }

        //start finding
        String[] keywordArray = keywords.split(" ");



        return null;

    }

    /**
     * AC 通过
     * */

    public double getAverageViewRate(String bv){
        String sql = "" +
                "SELECT AVG(total_view_time) AS average_view_time " +
                "FROM " +
                "    (" +
                "        SELECT " +
                "            bv , " +
                "            user_mid," +
                "            SUM(view_time) AS total_view_time " +
                "        FROM" +
                "            user_watch_video" +
                "        where bv = ?" +
                "        GROUP BY" +
                "            bv, user_mid" +
                "   " +
                "    ) AS user_total_view_time" +
                " group by bv" +
                "";


        try {
            var con = dataSource.getSQLConnection();
            PreparedStatement pre = con.prepareStatement(sql);
            pre.setString(1,bv);
            ResultSet resultSet = pre.executeQuery();
            if(resultSet.next()){
                double li = resultSet.getDouble(1);



                String findbv ="SELECT duration from b_video where bv = ?";
                PreparedStatement p = con.prepareStatement(findbv);
                p.setString(1,bv);
                ResultSet resultSet1 = p.executeQuery();
                if(resultSet1.next()){
                    double length = resultSet1.getDouble(1);
                    pre.close();
                    p.close();
                    con.close();

                    return li/length;
                }else {return -1;}



            }else {return -1;}


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        }

        /**
         * AC 通过
         * */
    public Set<Integer> getHotspot(String bv){

        Set<Integer> hot = new HashSet<>();
        String sql = "WITH DanmuChunks AS ( " +
                "  SELECT " +
                "    bv, " +
                "    FLOOR(time / 10) AS chunk_number, " +
                "    COUNT(*) AS danmu_count " +
                "  FROM b_danmu " +
                "  WHERE bv = ? " +
                "  GROUP BY bv, chunk_number " +
                ") " +
                "SELECT " +
                "  bv, " +
                "  chunk_number * 10 AS start_time, " +
                "  (chunk_number + 1) * 10 AS end_time, " +
                "  danmu_count " +
                "FROM DanmuChunks " +
                "WHERE danmu_count = (SELECT MAX(danmu_count) FROM DanmuChunks) " +
                "ORDER BY bv, start_time; ";
        try {
            var con = dataSource.getSQLConnection();
            var stmt = con.prepareStatement(sql);
            stmt.setString(1,bv);

            ResultSet resultSet = stmt.executeQuery();

            while (resultSet.next()){
                Integer start = (int) (resultSet.getFloat("start_time")/10);
                hot.add(start);
            }
            stmt.close();
            con.close();
            return hot;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public  boolean reviewVideo(AuthInfo auth, String bv){
        //check OA
        OAMessage oaMessage = checkAuthInvalid(auth);
        if(!oaMessage.isAuthIsValid()){
            log.info("search video OA failed");
            return false;
        }

        String GetBv = "(select bv,owner_mid,is_review from b_video where bv = ?)";
        try {
            var con = dataSource.getSQLConnection();
            PreparedStatement stm = con.prepareStatement(GetBv);
            stm.setString(1,bv);
            ResultSet resultSet = stm.executeQuery();
            if(resultSet.next()){
                long mid = resultSet.getLong("owner_mid");
                short review = resultSet.getShort("is_review");
                if(oaMessage.getIdentity().toLowerCase().equals("superuser") || oaMessage.getMid()==mid){
                    if(review==1){log.info("review = 1");return false;}

                    String alter = "update b_video set is_review =1,update_time=CURRENT_TIMESTAMP,review_time=CURRENT_TIMESTAMP where bv=?";
                    PreparedStatement p = con.prepareStatement(alter);
                    p.setString(1,bv);
                    p.executeUpdate();
                    return true;

                }else {
                    log.info("oa / super");
                    return false;
                }
            }else {log.info("no bv");
                return false;}

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public boolean coinVideo(AuthInfo auth, String bv){
        //check OA
        OAMessage oaMessage = checkAuthInvalid(auth);
        if(!oaMessage.isAuthIsValid()){
            log.info("search video OA failed");
            return false;
        }
        VideoMessage videoMessage = GetVideoMessage(bv);
        if(videoMessage == null){log.info("no v");return false;}
        if(videoMessage.getOwnerMid()==oaMessage.getMid()){log.info("himself");return false;}
        if(videoMessage.getPublicTime().equals(null) || videoMessage.getPublicTime() ==null ){return false;}
        Timestamp d = new Timestamp(System.currentTimeMillis());
        if(videoMessage.getPublicTime().after(d) && !oaMessage.getIdentity().toLowerCase().equals("superuser")){
            log.info("can not see");return false;
        }
        if(oaMessage.getCoin()<1){log.info("ni bi mei le");return false;}

        String sql = "SELECT * from user_give_coin_video where bv= ? and user_mid = ?";
        try( var con = dataSource.getSQLConnection();
             PreparedStatement stm = con.prepareStatement(sql);){
            stm.setString(1,bv);
            stm.setLong(2,oaMessage.getMid());
            ResultSet resultSet = stm.executeQuery();
            if(resultSet.next()){log.info("you have coin it");return false;}
            stm.close();

            //transaction
            String sbl = "SELECT insert_and_update_coins(?, ?);";
            PreparedStatement t = con.prepareStatement(sbl);
            t.setString(2,bv);
            t.setLong(1,oaMessage.getMid());
            t.executeUpdate();
            t.close();


            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean likeVideo(AuthInfo auth, String bv){
        //check OA
        OAMessage oaMessage = checkAuthInvalid(auth);
        if(!oaMessage.isAuthIsValid()){
            log.info("search video OA failed");
            return false;
        }
        VideoMessage videoMessage = GetVideoMessage(bv);
        if(videoMessage == null){log.info("no v");return false;}
        if(videoMessage.getOwnerMid()==oaMessage.getMid()){log.info("himself");return false;}
        if(videoMessage.getPublicTime().equals(null) || videoMessage.getPublicTime() ==null ){return false;}
        Timestamp d = new Timestamp(System.currentTimeMillis());
        if(videoMessage.getPublicTime().after(d) && !oaMessage.getIdentity().toLowerCase().equals("superuser")){
            log.info("can not see");return false;
        }


        return false;}
    public boolean collectVideo(AuthInfo auth, String bv){return false;}

    public VideoMessage GetVideoMessage(String bv){
        String GetBv = "(select * from b_video where bv = ?)";
        try( var con = dataSource.getSQLConnection();
             PreparedStatement stm = con.prepareStatement(GetBv);){
            stm.setString(1,bv);
            ResultSet resultSet = stm.executeQuery();
            if(resultSet.next()){
                String bv1 = resultSet.getString("bv");
                String title = resultSet.getString("title");
                long owner_mid = resultSet.getLong("owner_mid");
                String owner_name = resultSet.getString("owner_name");
                Timestamp create_time = resultSet.getTimestamp("create_time");
                Timestamp commit_time = resultSet.getTimestamp("commit_time");
                Timestamp review_time = resultSet.getTimestamp("review_time");
                Timestamp public_time = resultSet.getTimestamp("public_time");
                float duration = resultSet.getFloat("duration");
                String description = resultSet.getString("description");
                long reviewer = resultSet.getLong("reviewer");
                Timestamp update_time = resultSet.getTimestamp("update_time");
                short is_posted = resultSet.getShort("is_posted");
                short is_review = resultSet.getShort("is_review");
                short is_public = resultSet.getShort("is_public");

                VideoMessage vd = new VideoMessage(bv,title,owner_mid,owner_name,create_time,commit_time,review_time,public_time
                ,duration,description,reviewer,update_time,is_posted,is_review,is_public);
                return vd;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }



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
