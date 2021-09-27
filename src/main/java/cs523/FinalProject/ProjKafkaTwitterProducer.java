package cs523.FinalProject;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProjKafkaTwitterProducer {
	
	public static void main(String[] args) throws Exception {
		final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>( );
		
		String consumerKey = "flsfOlKjS9KR4xWwM8pztn4Um"; 
		String consumerSecret = "eAnP1TF9CF0obix4XndikAbAYPBmTEwW358ueQeY6Mkav5NYZp"; 
		String accessToken = "968452682775257088-kNLEtvuGX5MYGxo7oV99IyQtW0gDQ99"; 
		String accessTokenSecret = "QT8MvNaS4rDQwuM2b1gpRkEz7GEO2Vxrn4cY9p8i3ybgu"; 
		String topicName = "TwitterData"; 

		String[] keyWords = {"Justin Tucker", "Rams", "Chiefs", "Jets"}; 

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
				.setOAuthConsumerKey(consumerKey)
				.setOAuthConsumerSecret(consumerSecret)
				.setOAuthAccessToken(accessToken)
				.setOAuthAccessTokenSecret(accessTokenSecret);

		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("Got scrub_geo event userId:" + userId + "upToStatusId:" + upToStatusId);
			}

			@Override
			public void onStallWarning(StallWarning warning) {
				System.out.println("Got stall warning:" + warning);
			}

			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}
		};
		twitterStream.addListener(listener);

		// Filter keywords
		FilterQuery query = new FilterQuery().track(keyWords);
		twitterStream.filter(query);

		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:2181");                         
		props.put("bootstrap.servers", "localhost:2181");                                
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		@SuppressWarnings("resource")
		Producer<String, String> producer = new KafkaProducer<String, String>(props);                
		int j = 0;

		while (true) {
			Status ret = queue.poll();

			if (ret == null) {
				Thread.sleep(100);
				// i++;
			} else {
					System.out.println("Tweet:" + ret);
					String source = ret.getSource();
					String msg = new String (ret.getCreatedAt() + ", " + 
							  ret.getUser().getName() + ", " + 
							  ret.getUser().getScreenName() + ", " +                          
							  ret.getUser().getFollowersCount()+ ", " + 
							  ret.getUser().getFriendsCount() + ", " + 
							  ret.getUser().getFavouritesCount() + ", " + 
							  getLocation(ret.getUser().getLocation())  + ", " + 
							  ret.getRetweetCount() + ", " + 
							  ret.getFavoriteCount() + ", " + 
							  ret.getLang() + ", " + 
							  source.substring((source.indexOf('>',5) + 1), source.indexOf('<',5))
							  );
					producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(j++), msg));    

			}
			
				
		}
		// producer.close();
		// Thread.sleep(500);
		// twitterStream.shutdown();
	}
	
	private static String getLocation(String loc){
		
		if (loc == null) 
			return "null" ;
		else return loc.split(",")[0];
	}
	
}