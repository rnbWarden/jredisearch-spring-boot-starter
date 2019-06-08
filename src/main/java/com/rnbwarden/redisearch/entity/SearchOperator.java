package com.rnbwarden.redisearch.entity;

public enum SearchOperator {

    UNION("|"),
    INTERSECTION(" ")
    //DISJUNCT,
    //DISJUNCT_UNION // the inverse of a UNION
    ;

    private final String joinString;

    SearchOperator(String joinString) {

        this.joinString = joinString;
    }

    public String getJoinString() {

        return joinString;
    }
}
