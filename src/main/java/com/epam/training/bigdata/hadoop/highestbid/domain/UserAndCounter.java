package com.epam.training.bigdata.hadoop.highestbid.domain;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/

public class UserAndCounter {

    private final String userId;
    private final long count;

    public static UserAndCounter create(String userId, long count) {
        return new UserAndCounter(userId, count);
    }

    public UserAndCounter(String userId, long count) {
        this.userId = userId;
        this.count = count;
    }

    public String getUserId() {
        return userId;
    }

    public long getCount() {
        return count;
    }

}
