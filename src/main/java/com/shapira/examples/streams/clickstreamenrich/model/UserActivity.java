package com.shapira.examples.streams.clickstreamenrich.model;

/**
 * Created by gwen on 1/28/17.
 */
public class UserActivity {
    int userId;
    String userName;
    String zipcode;
    String[] interests;
    String searchTerm; //用户的搜索词
    String page; //用户点击匹配的页面

    public UserActivity(int userId, String userName, String zipcode, String[] interests, String searchTerm, String page) {
        this.userId = userId;
        this.userName = userName;
        this.zipcode = zipcode;
        this.interests = interests;
        this.searchTerm = searchTerm;
        this.page = page;
    }

    public UserActivity updateSearch(String searchTerm) {
        this.searchTerm = searchTerm;
        return this;
    }
}
