package io.sustc.pojo;

import lombok.Getter;

import java.sql.Timestamp;

@Getter
public class OAMessage {

    private boolean AuthIsValid;

    private String identity;

    private long mid;

    private String sex;

    public void setSex(String sex) {
        this.sex = sex;
    }

    public void setBirthday(String birthday) {
        this.birthday = birthday;
    }

    public void setLevel(short level) {
        this.level = level;
    }

    public void setSign(String sign) {
        this.sign = sign;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setQq(String qq) {
        this.qq = qq;
    }

    public void setWechat(String wechat) {
        this.wechat = wechat;
    }

    public void setCoin(int coin) {
        this.coin = coin;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    private String birthday;
    private short level;
    private String sign;
    private String password;
    private String qq;
    private String wechat;
    private int coin;
    private Timestamp timestamp;

    public void setName(String name) {
        this.name = name;
    }

    private String name;

    public void setAuthIsValid(boolean authIsValid) {
        AuthIsValid = authIsValid;
    }

    public void setMid(long mid) {
        this.mid = mid;
    }



    public void setIdentity(String identity) {
        this.identity = identity;
    }



    public OAMessage(){
        mid=0;
        AuthIsValid =false;
        identity=null;
        name=null;
        timestamp=null;
        birthday=null;
        sign=null;
        qq=null;
        wechat=null;
    }


}
