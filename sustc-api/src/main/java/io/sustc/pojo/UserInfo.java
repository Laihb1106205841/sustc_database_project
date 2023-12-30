package io.sustc.pojo;

import lombok.Getter;

@Getter
public class UserInfo {
    private final long userId;
    private final int coin;

    public UserInfo(long userId, int coin) {
        this.userId = userId;
        this.coin = coin;
    }

}

