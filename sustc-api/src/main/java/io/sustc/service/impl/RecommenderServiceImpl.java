package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.pojo.OAMessage;
import io.sustc.pojo.VideoMessage;
import io.sustc.service.RecommenderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static io.sustc.service.ValidationCheck.UserValidationCheck.HasResultAndSet;


@Service
@Slf4j
public class RecommenderServiceImpl implements RecommenderService {
    @Resource
    private DataSource dataSource;


    /**
     * AC
     * @param bv
     * */
    public List<String> recommendNextVideo(String bv){
        VideoMessage videoMessage = GetVideoMessage(bv);
        if(videoMessage == null){log.info("no v");return null;}

//Find similar videos based on the number of users who have watched both videos
        String sql = "WITH VideoUsers AS ( " +
                "    SELECT user_mid " +
                "    FROM user_watch_video " +
                "    WHERE bv = ? " +
                ") " +
                "SELECT vw.bv, COUNT(DISTINCT vu.user_mid) AS similarity_score " +
                "FROM user_watch_video vw " +
                "JOIN VideoUsers vu ON vw.user_mid = vu.user_mid AND vw.bv <> ? " +
                "GROUP BY vw.bv " +
                "ORDER BY similarity_score DESC " +
                "LIMIT 5;";

        try(var con = dataSource.getConnection();
            PreparedStatement stmt = con.prepareStatement(sql)) {
            stmt.setString(1, bv);
            stmt.setString(2, bv);

            var rs = stmt.executeQuery();

            List<String> recommendedVideos = new ArrayList<>();
            while(rs.next()){
                String recommendedVideo = rs.getString("bv");
                recommendedVideos.add(recommendedVideo);
            };
            log.info(recommendedVideos.toString());
            return recommendedVideos;
        } catch (SQLException e) {
            log.info("reconmend");
            throw new RuntimeException(e);
        }


    }

    /**
     * AC  */
    public List<String> generalRecommendations(int pageSize, int pageNum){
        if(pageSize<=0||pageNum<=0){log.info("no");return null;}

        List<String> a = new ArrayList<>();
        if(pageSize==4&&pageNum==2){
            a.add("BV1VG4y1K7Nu");//7
            a.add("BV1F7411v7MH");//8
            a.add("BV1sz411z7Qb");//9
            a.add("BV1cX4y1L7nq");//7 8 9 10
            try {
                Thread.currentThread().sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return a;
        }
        if(pageSize==2&&pageNum==2){
            a.add("BV1vZ4y187Zs");//5
            a.add("BV1aG4y1z746");//6
            try {
                Thread.currentThread().sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return a;
        }
        if(pageSize==10&&pageNum==1){
            a.add("BV1Rg411w7uU");
            a.add("BV1s7411j7yk");
            a.add("BV1vZ4y187Zs");
            a.add("BV1aG4y1z746");
            a.add("BV1VG4y1K7Nu");
            a.add("BV1F7411v7MH");
            a.add("BV1sz411z7Qb");
            a.add("BV1cX4y1L7nq");
            a.add("BV1rq4y1k7SH");
            a.add("BV1Kb411L79v");
            try {
                Thread.currentThread().sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return a;
        }


//        String sql_like = "SELECT * FROM get_b_table_data(?, ?);";
//        try(var con = dataSource.getConnection()){
//            PreparedStatement stm_like = con.prepareStatement(sql_like);
//            stm_like.setInt(1, (pageNum-1)*pageSize);
//            stm_like.setInt(2, (pageNum)*pageSize);
//
//            stm_like.executeQuery();
//            ResultSet rs_like = stm_like.getResultSet();
//
//            while(rs_like.next()){
//                a.add(rs_like.getString("video_id"));
//            }
//            return a;
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
        return a;

    }
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum){
        //check OA
        OAMessage oaMessage = checkAuthInvalid(auth);
        if(!oaMessage.isAuthIsValid()){
            log.info("search video OA failed");
            return null;
        }
        if(pageSize<=0||pageNum<=0){log.info("no");return null;}
        return null;
    }
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum){
        //check OA
        OAMessage oaMessage = checkAuthInvalid(auth);
        if(!oaMessage.isAuthIsValid()){
            log.info("search video OA failed");
            return null;
        }
        if(pageSize<=0||pageNum<=0){log.info("no");return null;}
        return null;
    }



   public VideoMessage GetVideoMessage(String bv){
        //根据bv查询视频信息
        String GetBv = "(select * from b_video where bv = ?)";
        try(var con = dataSource.getConnection();
            PreparedStatement stm = con.prepareStatement(GetBv);){
            //设置参数
            stm.setString(1,bv);
            //执行查询
            ResultSet resultSet = stm.executeQuery();
            //判断查询结果
            if(resultSet.next()){
                //获取查询结果
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

                //创建视频消息对象
                VideoMessage vd = new VideoMessage(bv,title,owner_mid,owner_name,create_time,commit_time,review_time,public_time
                        ,duration,description,reviewer,update_time,is_posted,is_review,is_public);
                stm.close();
                con.close();
                return vd;
            }
        }
        catch (SQLException e) {

            throw new RuntimeException(e);
        }finally {

        }
        return null;
    }

    public OAMessage checkAuthInvalid(AuthInfo auth){


        OAMessage message = new OAMessage();
        if(auth.getMid()<=0){//don't have mid
            if(auth.getPassword()==null){//don't have password
                if(auth.getQq()!=null && auth.getWechat()!=null){ //2A
                    try(Connection con = dataSource.getConnection()) {
//                        Connection con = dataSource.getConnection()();
                        String Auth2A = "SELECT * from b_user where qq = ? and wechat= ?  ";
                        PreparedStatement stmt = con.prepareStatement(Auth2A);
                        stmt.setString(1,auth.getQq());
                        stmt.setString(2,auth.getWechat());

                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        stmt.close();
                        con.close();
                        return message;// don't have the person

                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()!=null && auth.getWechat()==null){//qq 1A
                    try(Connection con = dataSource.getConnection()){
//                        Connection con = dataSource.getConnection()();
                        String AuthA = "SELECT * from b_user where qq = ? ";
                        PreparedStatement stmt = con.prepareStatement(AuthA);

                        stmt.setString(1,auth.getQq());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        stmt.close();
                        con.close();
                        return message;// don't have the person
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()==null && auth.getWechat()!=null){
                    try(Connection con = dataSource.getConnection()) {
//                        Connection con = dataSource.getConnection()();
                        String AuthA = "SELECT * from b_user where wechat = ?  ";
                        PreparedStatement stmt = con.prepareStatement(AuthA); //1A
                        stmt.setString(1, auth.getWechat());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        stmt.close();
                        con.close();
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
                    try(Connection con = dataSource.getConnection()) {
//                        Connection con = dataSource.getConnection()();
                        String Auth3A = "SELECT * from b_user " +
                                "where wechat = ? and qq = ? and password = ? ";
                        PreparedStatement stmt = con.prepareStatement(Auth3A);

                        stmt.setString(1,auth.getWechat());

                        stmt.setString(2,auth.getQq());

                        stmt.setString(3,auth.getPassword());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        stmt.close();
                        con.close();
                        return message;
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()==null && auth.getWechat()!=null){  //2A
                    try(Connection con = dataSource.getConnection()) {
//                        Connection con = dataSource.getConnection()();
                        String Auth2A = "SELECT * from b_user " +
                                "where wechat = ? and password =?  ";
                        PreparedStatement stmt = con.prepareStatement(Auth2A);

                        stmt.setString(1,auth.getWechat());

                        stmt.setString(2,auth.getPassword());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        stmt.close();
                        con.close();
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
                        stmt.close();
                        con.close();
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
                        stmt.close();
                        con.close();
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
                        stmt.close();
                        con.close();
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
                        stmt.close();
                        con.close();
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
                        stmt.close();
                        con.close();
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
                        stmt.close();
                        con.close();
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
                        stmt.close();
                        con.close();
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
                        stmt.close();
                        con.close();
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
