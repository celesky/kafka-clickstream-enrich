package com.shapira.examples.streams.clickstreamenrich;

import com.shapira.examples.streams.clickstreamenrich.model.PageView;
import com.shapira.examples.streams.clickstreamenrich.model.Search;
import com.shapira.examples.streams.clickstreamenrich.model.UserActivity;
import com.shapira.examples.streams.clickstreamenrich.model.UserProfile;
import com.shapira.examples.streams.clickstreamenrich.serde.JsonDeserializer;
import com.shapira.examples.streams.clickstreamenrich.serde.JsonSerializer;
import com.shapira.examples.streams.clickstreamenrich.serde.WrapperSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class ClickstreamEnrichment {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "clicks");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER);
        // Since each step in the stream will involve different objects, we can't use default Serde

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // work-around for an issue around timing of creating internal topics
        // this was resolved in 0.10.2.0 and above
        // don't use in large production apps - this increases network load
        // props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 500);

        KStreamBuilder builder = new KStreamBuilder();
        // 为点击事件和搜索事件创建流对象
        KStream<Integer, PageView> views = builder.stream(Serdes.Integer(), new PageViewSerde(), Constants.PAGE_VIEW_TOPIC);
        // 给用户信息定义一个 KTable. KTable 是本地缓存，可以通过变更流来对其进行更新。
        KTable<Integer, UserProfile> profiles = builder.table(Serdes.Integer(), new ProfileSerde(), Constants.USER_PROFILE_TOPIC, "profile-store");
        KStream<Integer, Search> searches = builder.stream(Serdes.Integer(), new SearchSerde(), Constants.SEARCH_TOPIC);

        // 点击事件流与信息表连接起来 ， 将用户信息填充到点击事件里
        KStream<Integer, UserActivity> viewsWithProfile = views.leftJoin(profiles,
                    (page, profile) -> {
                        if (profile != null)
                            return new UserActivity(profile.getUserID(), profile.getUserName(), profile.getZipcode(), profile.getInterests(), "", page.getPage());
                        else
                           return new UserActivity(-1, "", "", null, "", page.getPage());
                     });

        // 接下来要将点击信息和用户的搜索事件连接起来。这也是一个左连接操作，不过现在连接的是两个流，而不是流和表。
        KStream<Integer, UserActivity> userActivityKStream = viewsWithProfile.leftJoin(searches,
                (userActivity, search) -> {
                    if (search != null)
                        userActivity.updateSearch(search.getSearchTerms());
                    else
                        userActivity.updateSearch("");
                    return userActivity;
                },
                // 我们要把具有相关性的 搜索事件和点击事件连接起来。
                // 具有相关性的点击事件应该发生在搜索之后的一小段时间内。所以这里定义了一个一秒钟的连接时间窗口。
                // 在搜索之后的 一秒钟内发生的点击事件才被认为是具有相关性的， 而且搜索关键词也会被放进包含了点击信息和用户信息 的活动记录里，
                // 这样有助于对搜索和搜索结果进行全面的分析。
                JoinWindows.of(1000), Serdes.Integer(), new UserActivitySerde(), new SearchSerde());

        userActivityKStream.to(Serdes.Integer(), new UserActivitySerde(), Constants.USER_ACTIVITY_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder, props);

        streams.cleanUp();

        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        //Thread.sleep(60000L);

        //treams.close();


    }

    static public final class PageViewSerde extends WrapperSerde<PageView> {
        public PageViewSerde() {
            super(new JsonSerializer<PageView>(), new JsonDeserializer<PageView>(PageView.class));
        }
    }

    static public final class ProfileSerde extends WrapperSerde<UserProfile> {
        public ProfileSerde() {
            super(new JsonSerializer<UserProfile>(), new JsonDeserializer<UserProfile>(UserProfile.class));
        }
    }

    static public final class SearchSerde extends WrapperSerde<Search> {
        public SearchSerde() {
            super(new JsonSerializer<Search>(), new JsonDeserializer<Search>(Search.class));
        }
    }

    static public final class UserActivitySerde extends WrapperSerde<UserActivity> {
        public UserActivitySerde() {
            super(new JsonSerializer<UserActivity>(), new JsonDeserializer<UserActivity>(UserActivity.class));
        }
    }
}
