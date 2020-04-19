/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.shreyas.flink.twitter.analysis;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import dev.shreyas.flink.twitter.analysis.pojo.TweetPojo;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.lang.reflect.Parameter;
import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */

@Slf4j
public class StreamingJob {
	private static Gson gson = new GsonBuilder()
			.setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
			.create();
	private static ParameterTool parameterTool ;
	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env 	=	StreamExecutionEnvironment.getExecutionEnvironment();
		parameterTool 							=	ParameterTool.fromArgs(args);

		String consumerKey						=	checkProperty("consumerKey");
		String pattern							=	checkProperty("pattern");
		String consumerSecret					=	checkProperty("consumerSecret");
		String token							= 	checkProperty("token");
		String tokenSecret						=	checkProperty("tokenSecret");

		System.out.println(
				String.format("Received the following inputs\n consumerKey\t\t: %s \n consumerSecret\t\t: %s \n token\t\t\t: %s \n tokenSecret\t\t: %s \n Pattern to search\t: %s",consumerKey,consumerSecret,token,tokenSecret,pattern));

		Properties twitterProperties 			= 	new Properties();
		twitterProperties.put(TwitterSource.CONSUMER_KEY, consumerKey);
		twitterProperties.put(TwitterSource.CONSUMER_SECRET,consumerSecret);
		twitterProperties.put(TwitterSource.TOKEN, token);
		twitterProperties.put(TwitterSource.TOKEN_SECRET, tokenSecret);

		env.addSource(new TwitterSource(twitterProperties))
				.flatMap(new MapTwitterStringToTwitterPojo())
				.filter(new RemoveRetweetsNNonEngFilter())
				.filter( new SearchForTweets(checkProperty("pattern")))
				.print();

		// execute program
		env.execute("Flink Streaming Twitter Analysis");
	}

	private static  String checkProperty(String param){
		if (!parameterTool.has(param)){
			throw new RuntimeException("Please pass parameter: "+param);
		}
		return parameterTool.get(param);
	}


	private static class MapTwitterStringToTwitterPojo implements FlatMapFunction<String, TweetPojo>{
		@Override
		public void flatMap(String s, Collector<TweetPojo> collector) throws Exception {
			try {
				TweetPojo tweetPojo =	gson.fromJson(s,TweetPojo.class);
				collector.collect(tweetPojo);
			}catch (Exception e){
				log.error(e.getMessage(),e);
			}
		}
	}


	private static class RemoveRetweetsNNonEngFilter implements FilterFunction<TweetPojo>{
		@Override
		public boolean filter(TweetPojo tweetPojo) {
			try {
				return !tweetPojo.isRetweeted() && tweetPojo.getLang()!=null &&tweetPojo.getLang().equals("en");
			}catch (Exception e){
				log.error(e.getMessage(),e);
			}
			return false;
		}
	}
	private static class SearchForTweets implements FilterFunction<TweetPojo>{
		String pattern;
		SearchForTweets(String pattern){
			this.pattern = pattern;
		}

		@Override
		public boolean filter(TweetPojo tweetPojo) throws Exception {
			try {
				return tweetPojo.getText().contains(pattern) || tweetPojo.getText().matches(pattern);
			}catch (Exception e){
				log.error(e.getMessage(),e);
			}
			return false;
		}
	}

}
