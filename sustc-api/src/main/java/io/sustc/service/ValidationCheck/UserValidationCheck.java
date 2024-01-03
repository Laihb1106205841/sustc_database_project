package io.sustc.service.ValidationCheck;

import io.sustc.dto.AuthInfo;
import io.sustc.pojo.OAMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Slf4j
public class UserValidationCheck {


//    private DataSource dataSource;
//
//    public UserValidationCheck(DataSource dataSource){
//        this.dataSource=dataSource;
//    }



    public static boolean checkBirthday(String birthday) {
        if(birthday == null){return true;}
        // 使用正则表达式检查格式
        String regex = "^(0?[1-9]|1[0-2])月(0?[1-9]|[12][0-9]|3[01])日$";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(birthday);

        if (matcher.matches()) {
            // 符合格式，进一步判断日期的合法性
            String[] parts = birthday.split("[月日]");
            int month = Integer.parseInt(parts[0]);
            int day = Integer.parseInt(parts[1]);

            // 判断月份和日期的合法性
            if (month >= 1 && month <= 12 && day >= 1 && day <= 31) {
                if(month==4&&day>30){return false;}
                if(month==2&&day>29){return false;}
                if(month==6&&day>30){return false;}
                if(month==9&&day>30){return false;}
                if(month==11&&day>30){return false;}
                return true; // 符合条件
            }
        }

        return false; // 不符合条件
    }

//    static

//    public OAMessage checkAuthInvalid(AuthInfo auth){
//
//
//        OAMessage message = new OAMessage();
//        if(auth.getMid()<=0){//don't have mid
//            if(auth.getPassword()==null){//don't have password
//                if(auth.getQq()!=null && auth.getWechat()!=null){ //2A
//                    try {
//                        Connection con = dataSource.getConnection();
//                        String Auth2A = "SELECT * from b_user where qq = ? and wechat= ?";
//                        PreparedStatement stmt = con.prepareStatement(Auth2A);
//                        stmt.setString(1,auth.getQq());
//                        stmt.setString(2,auth.getWechat());
//
//                        ResultSet resultSet = stmt.executeQuery();
//                        HasResultAndSet(message, resultSet);
//                        return message;
//
//                    } catch (SQLException e) {
//                        return message;
//                    }
//                }
//                if(auth.getQq()!=null && auth.getWechat()==null){//qq 1A
//                    try{
//                        Connection con = dataSource.getConnection();
//                        String AuthA = "SELECT * from b_user where qq = ? ";
//                        PreparedStatement stmt = con.prepareStatement(AuthA);
//
//                        stmt.setString(1,auth.getQq());
//                        ResultSet resultSet = stmt.executeQuery();
//                        HasResultAndSet(message, resultSet);
//                        return message;// don't have the person
//                    } catch (SQLException e) {
//                        return message;
//                    }
//                }
//                if(auth.getQq()==null && auth.getWechat()!=null){
//                    try {
//                        Connection con = dataSource.getConnection();
//                        String AuthA = "SELECT * from b_user where wechat = ? ";
//                        PreparedStatement stmt = con.prepareStatement(AuthA); //1A
//                        stmt.setString(1, auth.getWechat());
//                        ResultSet resultSet = stmt.executeQuery();
//                        HasResultAndSet(message, resultSet);
//                        return message;// don't have the person
//                    } catch (SQLException e) {
//                        return message;
//                    }
//                }
//                if(auth.getQq()==null && auth.getWechat()==null){  //0A
//                    // nmdx,shen me dou mei you ni deng ge ji er
//                    return message;
//                }
//            }
//            else {//has password
//                if(auth.getQq()!=null && auth.getWechat()!=null){ //3A
//                    try {
//                        Connection con = dataSource.getConnection();
//                        String Auth3A = "SELECT * from b_user where wechat = ? and qq = ? and password = ?";
//                        PreparedStatement stmt = con.prepareStatement(Auth3A);
//
//                        stmt.setString(1,auth.getWechat());
//
//                        stmt.setString(2,auth.getQq());
//
//                        stmt.setString(3,auth.getPassword());
//                        ResultSet resultSet = stmt.executeQuery();
//                        HasResultAndSet(message, resultSet);
//                        return message;
//                    } catch (SQLException e) {
//                        return message;
//                    }
//                }
//                if(auth.getQq()==null && auth.getWechat()!=null){  //2A
//                    try {
//                        Connection con = dataSource.getConnection();
//                        String Auth2A = "SELECT * from b_user where wechat = ? and password =? ";
//                        PreparedStatement stmt = con.prepareStatement(Auth2A);
//
//                        stmt.setString(1,auth.getWechat());
//
//                        stmt.setString(2,auth.getPassword());
//                        ResultSet resultSet = stmt.executeQuery();
//                        HasResultAndSet(message, resultSet);
//                        return message;
//                    } catch (SQLException e) {
//                        return message;
//                    }
//                }
//                if(auth.getQq()!=null && auth.getWechat()==null){//2A
//                    try {
//                        Connection con = dataSource.getConnection();
//                        String Auth2A = "SELECT * from b_user where qq = ? and password =? ";
//                        PreparedStatement stmt = con.prepareStatement(Auth2A);
//
//                        stmt.setString(1,auth.getQq());
//
//                        stmt.setString(2,auth.getPassword());
//                        ResultSet resultSet = stmt.executeQuery();
//                        HasResultAndSet(message, resultSet);
//                        return message;
//                    } catch (SQLException e) {
//                        return message;
//                    }
//                }
//                if(auth.getQq()==null && auth.getWechat()==null){//A
//                    return message;//only password
//                }
//            }
//
//        }
//        else {//HAVE MID
//            if(auth.getPassword()==null){//don't have password
//                if(auth.getQq()!=null && auth.getWechat()!=null){//3A
//                    try {
//                        Connection con = dataSource.getConnection();
//                        String Auth3A = "SELECT * from b_user where mid = ? and wechat = ? and password =? ";
//                        PreparedStatement stmt = con.prepareStatement(Auth3A);
//
//                        stmt.setLong(1,auth.getMid());
//                        stmt.setString(2,auth.getWechat());
//                        stmt.setString(3,auth.getPassword());
//                        ResultSet resultSet = stmt.executeQuery();
//                        HasResultAndSet(message, resultSet);
//                        return message;// don't have the person
//                    } catch (SQLException e) {
//                        return message;
//                    }
//                }
//                if(auth.getQq()==null && auth.getWechat()!=null){//2A
//                    try {
//                        Connection con = dataSource.getConnection();
//                        String Auth2A = "SELECT * from b_user where mid = ? and wechat =? ";
//                        PreparedStatement stmt = con.prepareStatement(Auth2A);
//
//                        stmt.setLong(1,auth.getMid());
//
//                        stmt.setString(2,auth.getWechat());
//                        ResultSet resultSet = stmt.executeQuery();
//                        HasResultAndSet(message, resultSet);
//                        return message;// don't have the person
//                    } catch (SQLException e) {
//                        return message;
//                    }
//                }
//                if(auth.getQq()!=null && auth.getWechat()==null){//2A
//                    try {
//                        Connection con = dataSource.getConnection();
//                        String Auth2A = "SELECT * from b_user where mid = ? and qq =? ";
//                        PreparedStatement stmt = con.prepareStatement(Auth2A);
//
//                        stmt.setLong(1,auth.getMid());
//
//                        stmt.setString(2,auth.getQq());
//                        ResultSet resultSet = stmt.executeQuery();
//                        HasResultAndSet(message, resultSet);
//                        return message;// don't have the person
//                    } catch (SQLException e) {
//                        return message;
//                    }
//                }
//                if(auth.getQq()==null && auth.getWechat()==null){//A
//                    return message;
//                }
//            }else {// have password
//                if(auth.getQq()!=null && auth.getWechat()!=null){//4A
//                    try {
////                        System.out.println("4A");
//                        Connection con = dataSource.getConnection();
//                        String Auth4A = "SELECT * from b_user where mid = ? and wechat = ? and qq = ? and password = ?";
////                        System.out.println(dataSource.toString());
//                        PreparedStatement stmt = con.prepareStatement(Auth4A);
//
//                        stmt.setLong(1,auth.getMid());
//
//                        stmt.setString(2,auth.getWechat());
//
//                        stmt.setString(3,auth.getQq());
//
//                        stmt.setString(4,auth.getPassword());
//
//                        ResultSet resultSet = stmt.executeQuery();
////                        if(!resultSet.next()){System.out.println("hey");}
////                        System.out.println(1);
////                        System.out.println(resultSet.toString());
//                        HasResultAndSet(message, resultSet);
//                        return message;// don't have the person
//                    } catch (SQLException e) {
//                        return message;
//                    }
//                }
//                if(auth.getQq()==null && auth.getWechat()!=null){//3A
//                    try {
//                        Connection con = dataSource.getConnection();
//                        String Auth3A = "SELECT * from b_user where mid = ? and wechat =? and password =?";
//                        PreparedStatement stmt = con.prepareStatement(Auth3A);
//
//                        stmt.setLong(1,auth.getMid());
//
//                        stmt.setString(2,auth.getWechat());
//
//                        stmt.setString(3,auth.getPassword());
//                        ResultSet resultSet = stmt.executeQuery();
//                        HasResultAndSet(message, resultSet);
//                        return message;// don't have the person
//                    } catch (SQLException e) {
//                        return message;
//                    }
//                }
//                if(auth.getQq()!=null && auth.getWechat()==null){//3A
//                    try {
//                        Connection con = dataSource.getConnection();
//                        String Auth3A = "SELECT * from b_user where mid = ? and qq =? and password=?";
//                        PreparedStatement stmt = con.prepareStatement(Auth3A);
//
//                        stmt.setLong(1,auth.getMid());
//
//                        stmt.setString(2,auth.getQq());
//
//                        stmt.setString(3,auth.getPassword());
//                        ResultSet resultSet = stmt.executeQuery();
//                        HasResultAndSet(message, resultSet);
//                        return message;// don't have the person
//                    } catch (SQLException e) {
//                        return message;
//                    }
//                }
//                if(auth.getQq()==null && auth.getWechat()==null){//2A
//                    try {
//                        Connection con = dataSource.getConnection();
//                        String Auth2A = "SELECT * from b_user where mid = ? and password =? ";
//                        PreparedStatement stmt = con.prepareStatement(Auth2A);
//
//                        stmt.setLong(1,auth.getMid());
//
//                        stmt.setString(2,auth.getPassword());
//                        ResultSet resultSet = stmt.executeQuery();
//                        HasResultAndSet(message, resultSet);
//                        return message;// don't have the person
//                    } catch (SQLException e) {
//                        return message;
//                    }
//                }
//            }
//        }
//        return message;
//    }
    public static OAMessage HasResultAndSet(OAMessage message, ResultSet resultSet){
        try {
//            if(resultSet.getRow()==1){
////                    System.out.println(resultSet.getLong("mid"));
//                message.setValid(true);
//                message.setMid(resultSet.getLong("mid"));
//                message.setIdentity(
//                        String.valueOf(resultSet.getString("identity")));
//                return message;
//            }
//            if(resultSet.next()){
//
//                // only one person  OR
//                //more person!
//
//            }
            if (resultSet.next()) {
                if (resultSet.isFirst()) {
                    message.setAuthIsValid(true);
                    message.setMid(resultSet.getLong("mid"));
                    message.setIdentity(String.valueOf(resultSet.getString("identity")));
                    message.setBirthday(String.valueOf(resultSet.getString("birthday")));

                    message.setCoin(resultSet.getInt("coin"));
                    message.setLevel(resultSet.getShort("level"));
                    message.setPassword(String.valueOf(resultSet.getString("password")));
                    message.setQq(String.valueOf(resultSet.getString("qq")));

                    message.setWechat(String.valueOf(resultSet.getString("wechat")));
                    message.setSex(String.valueOf(resultSet.getString("sex")));
                    message.setSign(String.valueOf(resultSet.getString("sign")));
                    message.setTimestamp(resultSet.getTimestamp("create_time"));
                    message.setName(String.valueOf(resultSet.getString("name")));

                } else {
                    // more than one person logic
                    message.setAuthIsValid(false);
                }
            }
        } catch (SQLException e) {
            return message;
        }return message;
    }


}
