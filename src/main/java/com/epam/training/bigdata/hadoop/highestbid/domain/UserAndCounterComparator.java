package com.epam.training.bigdata.hadoop.highestbid.domain;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/20/2016
*/

import java.util.Comparator;

public class UserAndCounterComparator implements Comparator<UserAndCounter> {
    @Override
    public int compare(UserAndCounter o1, UserAndCounter o2) {
        return Long.compare(o2.getCount(), o1.getCount());
    }
}
