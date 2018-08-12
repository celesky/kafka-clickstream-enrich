package com.shapira.examples.streams.clickstreamenrich;

/**
 * Created by gwen on 1/28/17.
 */
public class Constants {

    public static final String BROKER = "47.106.140.44:9092";
    // 用户信息更新事件流
    public static final String USER_PROFILE_TOPIC = "clicks.user.profile";
    //
    public static final String PAGE_VIEW_TOPIC = "clicks.pages.views";
    // 网站搜索事件流 {userId,terms}
    public static final String SEARCH_TOPIC = "clicks.search";

    public static final String USER_ACTIVITY_TOPIC = "clicks.user.activity";

}
