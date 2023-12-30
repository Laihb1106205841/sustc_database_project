package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.pojo.OAMessage;
import io.sustc.service.DanmuService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static io.sustc.service.ValidationCheck.UserValidationCheck.HasResultAndSet;

@Service
@Slf4j
public class DanmuServiceImpl implements DanmuService {
    @Autowired
    private DataSource dataSource;


    // complete


    @Override
    public long sendDanmu(AuthInfo auth, String bv, String content, float time){
        if(content.isEmpty() || content == null){
            log.info("content null");
            return -1;
        }
        //check OA
        OAMessage oaMessage = checkAuthInvalid(auth);
        if(!oaMessage.isAuthIsValid()){
            log.info("OA failed");
            return -1;}

        //check bv

        String CheckBv ="select bv,is_public from b_video where bv = ? and is_public = 1";
        String CheckView ="select user_mid,view_time from user_watch_video where bv = ? and user_mid =?";
        try (Connection con = dataSource.getConnection();
             PreparedStatement preparedStatement = con.prepareStatement(CheckBv)){
            preparedStatement.setString(1,bv);
            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
//                short is_p = resultSet.getShort("is_public");
//                if (is_p==0){
//                    log.info("视频还没有发布");
//                    return -1;}
                //watch
                PreparedStatement stm2 = con.prepareStatement(CheckView);
                stm2.setString(1,bv);
                stm2.setLong(2,oaMessage.getMid());
                ResultSet resultSet1 = stm2.executeQuery();

                if(resultSet1.next()){
                    long mid = resultSet1.getLong("user_mid");
                    float watch_time = resultSet1.getFloat("view_time");
//                    if(time>watch_time){
//                        log.info("you haven't seen it yet");
//                        return -1;
//                    }

                    String GenerateId = "Select max(danmu_id)+1 from b_danmu";
                    stm2 = con.prepareStatement(GenerateId);
                   ResultSet resultSet2 = stm2.executeQuery();
                    long newId = -1;
                    if(resultSet2.next()){ newId= resultSet2.getLong(1);

// ���Բ��Բ��Բ��Բ��Բ��Բ��Բ��Բ���TESTDANMUDANMU MEMORYOVERFLOW��������2
                    String Send="INSERT INTO b_danmu" +
                            "(danmu_id, bv, sender_mid," +
                            " time, content, post_time) values " +
                            "       (?,?,?," +
                                    "?,?,CURRENT_TIMESTAMP)";
                    stm2 = con.prepareStatement(Send);
                    stm2.setLong(1,newId);
                    stm2.setString(2,bv);
                    stm2.setLong(3,oaMessage.getMid());
                    stm2.setFloat(4,time);
                    stm2.setString(5,content);
                    stm2.executeUpdate();

                    preparedStatement.close();
                    stm2.close();
                    con.close();
                    return newId;}
                    log.info("come to display");
                    return -1;
                }else {
                    log.info("haven't watch");
                    return -1;}
            }else {
                log.info("no bv");
                return -1;
            }

        }catch (RuntimeException | SQLException e){
            log.info("SQL fail");
            return -1;
        }
    }


    /**
    <h2>API Success<h2>
    <p>display Danmu
     */
    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter){
        if(timeStart-timeEnd>=0 || timeEnd<0 || timeStart<0){log.info("时间无效");return null;}

        String sql = "";
        if(filter){ sql = "select x.danmu_id from(" +
                "SELECT DISTINCT ON (content) danmu_id,content,time " +
                "FROM b_danmu " +
                "WHERE bv = ? AND time BETWEEN ? AND ? order by content,time" +
                ")x order by x.time;";}
        else {
            sql = "SELECT" +
                    "    danmu_id FROM b_danmu" +
                    "       WHERE bv = ? AND time BETWEEN ? AND ? order by  time ";}

        String FindVideo = "select bv,duration,is_public from b_video where bv =?";

        try( var con = dataSource.getConnection();
             PreparedStatement stmt = con.prepareStatement(FindVideo);
        ) {
            stmt.setString(1,bv);
            ResultSet resultSet = stmt.executeQuery();

            if(resultSet.next()){
//                log.info("GET bv"+resultSet.getString("bv"));


                double timeG = resultSet.getDouble("duration");
//                log.info("Get Duration:"+timeG);

                if(timeStart>timeG || timeEnd>timeG){
//                    log.info("超出时间");
                    return null;}
                short is_pub = resultSet.getShort("is_public");
                if(is_pub == 0){
//                    log.info("not publish");
                    return null;}

                PreparedStatement stmt2 = con.prepareStatement(sql);
//                log.info("start select");
                stmt2.setString(1,bv);
                stmt2.setFloat(2,timeStart);
                stmt2.setFloat(3,timeEnd);
                ResultSet resultSet1 = stmt2.executeQuery();

                List<Long> result = new ArrayList<>();
//                log.info("h");
                while (resultSet1.next()) {
                    result.add(resultSet1.getLong("danmu_id"));
                }
//                log.info("danmu size"+String.valueOf(result.size()));
                stmt.close();
                stmt2.close();
                con.close();
            return result;


            }else {
//                log.info("找不到bv");
            stmt.close();
            }
        } catch (SQLException e) {
            return null;
        }




        return null;
    }

//    static
    @Override
    public boolean likeDanmu(AuthInfo auth, long id){

        //check OA
        OAMessage oaMessage = checkAuthInvalid(auth);
        if(!oaMessage.isAuthIsValid()){
            log.info("OA failed");
            return false;}

        String Get_Danmu = "SELECT danmu_id,bv from b_danmu where danmu_id = ?";

        String FlikeDanmu = "SELECT danmu_id,bv from danmu_like where danmu_id = ? and user_mid_like =?";
        try(var con = dataSource.getConnection();
            PreparedStatement stmt = con.prepareStatement(Get_Danmu);) {

            stmt.setLong(1,id);
            ResultSet resultSet = stmt.executeQuery();


            if(resultSet.next()){
                String BV_ = resultSet.getString("bv");
               PreparedStatement stmt2 = con.prepareStatement(FlikeDanmu);
               stmt2.setLong(1,id);
               stmt2.setLong(2,oaMessage.getMid());
               ResultSet resultSet1 = stmt2.executeQuery();

               if(resultSet1.next()){
                   log.info("喜欢过");

                   String DisLike = "DELETE from danmu_like where danmu_id =? and bv = ? and user_mid_like =?";
                   PreparedStatement statement = con.prepareStatement(DisLike);
                   statement.setLong(1,id);
                   statement.setString(2,BV_);
                   statement.setLong(3,oaMessage.getMid());
                   statement.executeUpdate();
                   statement.close();

               }else {
                   log.info("第一次");
//                   String BV_ = resultSet1.getString("bv");
                   String DisLike = "INSERT into danmu_like(danmu_id, bv, user_mid_like) values (?,?,?)";
                   PreparedStatement statement = con.prepareStatement(DisLike);
                   statement.setLong(1,id);
                   statement.setString(2,BV_);
                   statement.setLong(3,oaMessage.getMid());
                   statement.executeUpdate();
                   statement.close();
               }
                stmt2.close();
                stmt.close();
               con.close();
               return true;


            }else {log.info("找不到弹幕");return false;}

        } catch (Exception e) {
            log.info("wrong with sql");
//            throw new RuntimeException(e);
            return false;
        }
//        return false;
    }





    public OAMessage checkAuthInvalid(AuthInfo auth){

        OAMessage message = new OAMessage();
        if(auth.getMid()<=0){//don't have mid
            if(auth.getPassword()==null){//don't have password
                if(auth.getQq()!=null && auth.getWechat()!=null){ //2A
                    try(Connection con = dataSource.getConnection()) {
//                        Connection con = dataSource.getConnection();
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
                    try(Connection con = dataSource.getConnection()){
//                        Connection con = dataSource.getConnection();
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
                    try(Connection con = dataSource.getConnection()) {
                        log.info("check wechat");
//                        Connection con = dataSource.getConnection();
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
//
            }
            else {//has password
                if(auth.getQq()!=null && auth.getWechat()!=null){ //3A
                    try(Connection con = dataSource.getConnection()) {
//                        Connection con = dataSource.getConnection();
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
                    try(Connection con = dataSource.getConnection()) {
//                        Connection con = dataSource.getConnection();
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
                    try(Connection con = dataSource.getConnection()) {

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
                    try(Connection con = dataSource.getConnection()) {

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
                    try(Connection con = dataSource.getConnection()) {

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
                    try(Connection con = dataSource.getConnection()) {

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
                    try(Connection con = dataSource.getConnection()) {
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
                    try (Connection con = dataSource.getConnection()) {

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
                    try (Connection con = dataSource.getConnection()) {

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
                    try (Connection con = dataSource.getConnection()) {

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
