package dev.shreyas.flink.twitter.analysis.pojo;

import com.google.gson.Gson;
import lombok.Data;

import java.io.Serializable;

/**
 * @author shreyas b
 * @created 20/04/2020 - 1:24 AM
 * @project flink-twitter-analysis
 **/

@Data
public class TweetPojo implements Serializable {
    String id;
    String text;
    String source;
    TwitterUser twitterUser;
    boolean truncated;
    boolean favorited;
    boolean retweeted;
    String lang;
    long timestampMs;

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
