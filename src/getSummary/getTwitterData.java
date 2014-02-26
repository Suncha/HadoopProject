package getSummary;

import twitter4j.conf.ConfigurationBuilder;
import twitter4j.GeoLocation;
import twitter4j.MediaEntity;
import twitter4j.Place;
import twitter4j.StallWarning;
import twitter4j.TwitterFactory;
import twitter4j.Twitter;
import twitter4j.TwitterStreamFactory;
import twitter4j.TwitterStream;
import twitter4j.StatusListener;
import twitter4j.StatusDeletionNotice;
import twitter4j.Status;
import twitter4j.FilterQuery;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.UserMentionEntity;
import twitter4j.json.DataObjectFactory;
import twitter4j.HashtagEntity;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.*;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

import java.util.regex.*;

public class getTwitterData {
	private static class StatusComparator implements java.util.Comparator {
		public int compare(Object o1, Object o2) {
			Status s1 = (Status) o1;
			Status s2 = (Status) o2;
			User u1 = (User) s1.getUser();
			User u2 = (User) s2.getUser();
			if (u2 == null)
				return 1;
			if (u1 == null)
				return -1;
			return u1.getFollowersCount() - u2.getFollowersCount();
		}
	}
	
	private static final String ColumnFamily_HashTag = "HashTag";
	private static final String ColumnFamily_Top10Summary = "Top10Summary";
	private static final String ColumnFamily_UserInfo = "UserInfo";
	private static final String TABLENAME = "twitterTable";

    public static final long MAX_FILE_SIZE = (1 * 1024 * 1024 * 1024);

    private static myStatusListener lsn = null;
    
    private static long lastPutTime = 0;
    
    private static HashMap<String,Integer> trackers = new HashMap<String,Integer>();
    private static HashMap<String,String> summary = new HashMap<String,String>();  
    private static ArrayList<String> trackers_username = new ArrayList<String>();
    private static ArrayList<String> trackers_profileLocation = new ArrayList<String>();
    private static ArrayList<Long> trackers_tweetId = new ArrayList<Long>();
    private static ArrayList<String> trackers_content = new ArrayList<String>();
    private static ArrayList<String> trackers_Hashtag = new ArrayList<String>();
    
    public static class myStatusListener implements StatusListener {
        FileSystem fs;
        Path path;
        Configuration conf;
        Configuration hbaseConf = HBaseConfiguration.create();
        HTable table;
        SequenceFile.Writer writer;
        String dir;
        String db_date;
        
		public myStatusListener(String dir) throws Exception {
            this.dir = dir;
            conf = new Configuration();
            hbaseConf.addResource(new Path("/usr/local/hbase/conf/hbase-site.xml"));
            table = new HTable(hbaseConf, TABLENAME.getBytes());
        }
		private SequenceFile.Writer createNewWriter() throws Exception {
		        String pathStr = dir + "/" + db_date + ".xml";
		    //fileNum++;
		    path = new Path(pathStr);
		    fs = FileSystem.get(URI.create(pathStr), conf);
		    SequenceFile.Writer nwr = SequenceFile.createWriter(fs, conf, path,
		                LongWritable.class, Text.class);
		    return nwr;
	  	}
        public void onStatus(Status status) {
        	User user = status.getUser();
        	String username = status.getUser().getScreenName();
            String profileLocation = user.getLocation();
            long tweetId = status.getId(); 
            String content = status.getText();
            String hashtag_tmp = null;
            Pattern pattern_content = Pattern.compile("^\\w.*");
    	    Matcher matcher_content = pattern_content.matcher(content);
    	    
    		try {
            	HashtagEntity[] hashtagEntities = status.getHashtagEntities();
            	for (HashtagEntity hashtag : hashtagEntities) {
            		Pattern pattern = Pattern.compile("^\\w.*");
            	    Matcher matcher = pattern.matcher(hashtag.getText());
            	    if (matcher.matches()) {
            	    	
            	    	// add hashtag to UserInfo
            	    	if(hashtag_tmp!=null){
                			hashtag_tmp = hashtag.getText()+ ","+hashtag_tmp;
                		}
                		else{
                			hashtag_tmp = hashtag.getText();
                		}
            	    	// hashmap count for 1 minute
                		if(trackers.containsKey(hashtag.getText())){  
                             int num = trackers.get(hashtag.getText());  
                             trackers.put(hashtag.getText(),num+1);             
                        }  
    	                else{  
    	            	    trackers.put(hashtag.getText(),1);  
    	                }
            	    	
            	    }
    			}

            } catch (Exception ex) {
            	System.err.println(ex);
            }

    	    
    	    if (status.getUser().getLang() != null && matcher_content.matches()
            		&& hashtag_tmp != null  && status.getUser().getLang().equalsIgnoreCase("en")){
            		trackers_Hashtag.add(hashtag_tmp);
        			trackers_username.add(username);
        			trackers_profileLocation.add(profileLocation);
        			trackers_tweetId.add(tweetId);
        			trackers_content.add(content);
        			
        			if ((System.currentTimeMillis() - lastPutTime) > 60000) {
        				//System.out.println("System.currentTimeMillis()"+System.currentTimeMillis());
        				//System.out.println("lastPutTime"+lastPutTime);
        				putHbase();
        				updateSummary();
                    	cleanMap();
                    	initMap();
                    	lastPutTime = System.currentTimeMillis();
                    }
                	
            }
        }
        
        private void putHbase() {
        	long timestamp = System.currentTimeMillis();
        	Calendar cal = Calendar.getInstance();
            String calStr = String.format("%04d", (cal.get(Calendar.YEAR)))
            		+":"+ String.format("%02d", cal.get(Calendar.MONTH) + 1)
            		+":"+ String.format("%02d", cal.get(Calendar.DAY_OF_MONTH))
            		+":"+ String.format("%02d", cal.get(Calendar.HOUR_OF_DAY))
            		+":"+ String.format("%02d", cal.get(Calendar.MINUTE));
           // 		+":"+ String.format("%02d", cal.get(Calendar.SECOND));
            System.out.println("Time: "+calStr);
            
            // get HashTag Count and Add it to Hbase
        	Object[] key =   trackers.keySet().toArray();
        	Arrays.sort(key); 
        	for (int i = 0; i < key.length; i++) {  
	            String hashtag = key[i].toString();
                String hashtagcount = trackers.get(key[i]).toString();
                String rowKey = hashtag;
                try {
                	Get get = new Get(rowKey.getBytes());
                    Result rs = table.get(get);
        			Put put = new Put(rowKey.getBytes());
        			if(!rs.isEmpty()){
                	//if (rs.containsColumn(ColumnFamily_HashTag.getBytes(), "count".getBytes())){
	        		   	for(KeyValue kv : rs.raw()){
	                		String str = new String(kv.getFamily(), "UTF-8");
	        		   		int count = Integer.parseInt(new String(kv.getValue(), "UTF-8"))+ Integer.parseInt(hashtagcount);
	         	            put.add(ColumnFamily_HashTag.getBytes(), "count".getBytes(), Integer.toString(count).getBytes());
	            			//put.add(ColumnFamily_HashTag.getBytes(), "count".getBytes(), "1".getBytes());
	                	}
        			}
                	else{
         	            put.add(ColumnFamily_HashTag.getBytes(), "count".getBytes(), hashtagcount.getBytes());
                	}
                	try {
     	             	table.put(put);
     	             } catch (Exception ex) {
     	             	System.err.println(ex);
     	             }    
                } catch (Exception ex) {
 	            	System.err.println(ex);
 	            }       
 	        }    
            
            int size = trackers_username.size()-1;
            for (int i = 0; i < size; i++) {  
	            String hashtag = trackers_Hashtag.get(i);
                String username = trackers_username.get(i);
                String profileLocation = trackers_profileLocation.get(i);
                String tweetId = trackers_tweetId.get(i).toString();
                String content = trackers_content.get(i);
    			
                String rowKey_username = username+"/"+calStr;
 	            Put put = new Put(rowKey_username.getBytes());
 	            put.add(ColumnFamily_UserInfo.getBytes(), "name".getBytes(), username.getBytes());
 	            put.add(ColumnFamily_UserInfo.getBytes(), "profileLocation".getBytes(), profileLocation.getBytes());
 	            put.add(ColumnFamily_UserInfo.getBytes(), "tweetId".getBytes(), tweetId.getBytes());
 	            put.add(ColumnFamily_UserInfo.getBytes(), "content".getBytes(), content.getBytes());
 	            put.add(ColumnFamily_UserInfo.getBytes(), "hashtag".getBytes(), hashtag.getBytes());
	            
	            try {
 	             	table.put(put);
 	             } catch (Exception ex) {
 	             	System.err.println(ex);
 	             }            
 	        }
            
        }
        private void updateSummary() {
        	 /**
             * Find top 10 hash-tags
             */
        	long timestamp = System.currentTimeMillis();
        	Calendar cal = Calendar.getInstance();
            String calStr = String.format("%04d", (cal.get(Calendar.YEAR)))
            		+":"+ String.format("%02d", cal.get(Calendar.MONTH) + 1)
            		+":"+ String.format("%02d", cal.get(Calendar.DAY_OF_MONTH))
            		+":"+ String.format("%02d", cal.get(Calendar.HOUR_OF_DAY))
            		+":"+ String.format("%02d", cal.get(Calendar.MINUTE));
           // 		+":"+ String.format("%02d", cal.get(Calendar.SECOND));
            System.out.println("Time: "+calStr);
        	System.out.println("updateSummary ");
        	HashMap<String,Integer> hashmap = new HashMap<String,Integer>(); 
            try {
            	 Scan s = new Scan();
                 ResultScanner ss = table.getScanner(s);
                 for(Result r:ss){
                	 for(KeyValue kv : r.raw()){
                    	 String str = new String(kv.getFamily(), "UTF-8");
                    	 if (str.equals(ColumnFamily_HashTag)){
                    		 hashmap.put(new String(kv.getRow(), "UTF-8"),Integer.parseInt(new String(kv.getValue(), "UTF-8")));
                    	 }
                     }
                 }
            } catch (Exception ex) {
	            	System.err.println(ex);
	            } 
           
           Map<String, Integer> sortedMapDesc = sortByComparator(hashmap, false);
           Object[] key =   sortedMapDesc.keySet().toArray();
           
           // top 10 hash-tags
           int toprank = 10; 
           for (int i = 0; i < key.length; i++) {
        	   if(i<toprank){
	        	   System.out.println("toprank"+i);
	          	    //System.out.println(key[i]+"= "+trackers.get(key[i]));
	               String hashtag = key[i].toString();
	               //String hashtagcount = hashmap.get(key[i]).toString();
	               //String rowKey = hashtag+"/"+calStr;
	               //Put put = new Put(rowKey.getBytes());
	               ArrayList<String> postList = new ArrayList<String>();
	               
	               try {
		               Scan s = new Scan();
		               ResultScanner ss = table.getScanner(s);
		               for(Result r:ss){
		            	   
		                for(KeyValue kv : r.raw()){
		                	 String fam = new String(kv.getFamily(), "UTF-8");
		                  	 String col = new String(kv.getQualifier(), "UTF-8");
		                  	 String row = new String(kv.getRow(), "UTF-8");
		                  	 if (fam.equals(ColumnFamily_UserInfo)&& col.equals("hashtag")){
		                  		String tags = new String(kv.getValue(), "UTF-8");
			                  	String tag[] = tags.split(",");
			                  	for(int k=0; k<tag.length; k++){
			                  		if (tag[k].equals(hashtag)){
			                  		   //System.out.println("row "+row);
			                  			Get get = new Get(row.getBytes());
			                            Result rs = table.get(get);
			                            for(KeyValue kv2 : rs.raw()){
			                            	 String col2 = new String(kv2.getQualifier(), "UTF-8");
			                            	 if(col2.equals("content")){
					                          	String str = new String(kv2.getValue(), "UTF-8");
					                          	//System.out.println("string "+str);
					                          	postList.add(str);
			                            	 }
			    	                		//put.add(ColumnFamily_HashTag.getBytes(), "count".getBytes(), "1".getBytes());
			    	                	}		
			                  		}
	
			                  	}
		        //          		 hashmap.put(new String(kv.getRow(), "UTF-8"),Integer.parseInt(new String(kv.getValue(), "UTF-8")));
		                  	 }
		                  }
		               }
		          
		            if(!postList.isEmpty()){
	            	   HashMap<String,Double> summary = new HashMap<String,Double>();
	            	   HashMap<String,Integer> summary_Int = new HashMap<String,Integer>();
	            		 getOriginalPosts posts = new getOriginalPosts();
		            	 posts.getOriginalPosts(postList);
		            	 if(!posts.summaryList.isEmpty()){
		            		 summary = posts.summaryList;
		     	             summary_Int = converttoInt(summary);   		 
		                     Map<String, Integer> sortedMapDesc_summary = sortByComparator(summary_Int, false);
		                     Object[] key_summary =   sortedMapDesc_summary.keySet().toArray();
		                     String fistsummary_content =null;
		                     String secondsummary_content =null;
		                     String summary_content =null;
		                     for (int f = 0; f < key_summary.length; f++) {
		                  	   if(f < 2){
		                  		   if(f == 0){
		                  			   summary_content = key_summary[i].toString();
		                  		   }
		                  		   else{
		                  			 summary_content = summary_content+" "+key_summary[i].toString();
		                  		     //System.out.println("summary_content "+summary_content);
		         	               }
		          	           }
		                    }
		                     String rowKey_summary = hashtag+"/"+calStr;
		                     Put put = new Put(rowKey_summary.getBytes());
		                     put.add(ColumnFamily_Top10Summary.getBytes(), "summary".getBytes(), summary_content.getBytes());
		                     try {
		 	 	             	table.put(put);
		 	 	             } catch (Exception ex) {
		 	 	             	System.err.println(ex);
		 	 	             }     
	                   	}
		            }
		            
	               }
	               catch (Exception ex) {
		            	System.err.println(ex);
		            }
	               
	               //Put put = new Put(rowKey_username.getBytes());
	 	           //put.add(ColumnFamily_UserInfo.getBytes(), "name".getBytes(), username.getBytes());
	               //put.add(ColumnFamily_Top10HashTag.getBytes(), "count".getBytes(), hashtagcount.getBytes());
	               //try {
	               // 	table.put(put);
	               // } catch (Exception ex) {
	              //  	System.err.println(ex);
	              //  }  
        	   }
	         }
           
     	//find top 10 hashtags
        	//for each hashtags
        	//	find contents that contains the hashtag
        	//	in each content 
        	//		remove stop-words
        	//		sentence split
        	//		ts/isf score and topic score
        	//choose the best sentence
        	//update to hbase
        }
        private static HashMap<String,Integer> converttoInt(HashMap<String,Double> summary){
        	HashMap<String,Integer> temp = new HashMap<String,Integer>();
        	Object[] key_summary =   summary.keySet().toArray();
            for (int f = 0; f < key_summary.length; f++) {
            	Double k = summary.get(key_summary[f])*1000;
            	temp.put(key_summary[f].toString(),k.intValue());
            }
        	return temp;
        			
        }
        private static Map<String, Integer> sortByComparator(Map<String, Integer> unsortMap, final boolean order)
        {

            List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(unsortMap.entrySet());

            // Sorting the list based on values
            Collections.sort(list, new Comparator<Entry<String, Integer>>()
            {
                public int compare(Entry<String, Integer> o1,
                        Entry<String, Integer> o2)
                {
                    if (order)
                    {
                        return o1.getValue().compareTo(o2.getValue());
                    }
                    else
                    {
                        return o2.getValue().compareTo(o1.getValue());

                    }
                }
            });

            // Maintaining insertion order with the help of LinkedList
            Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
            for (Entry<String, Integer> entry : list)
            {
                sortedMap.put(entry.getKey(), entry.getValue());
            }

            return sortedMap;
        }
        private void initMap() {
        	trackers_Hashtag = new ArrayList<String>();
            trackers_username = new ArrayList<String>();
            trackers_profileLocation = new ArrayList<String>();
            trackers_tweetId = new ArrayList<Long>();
            trackers_content = new ArrayList<String>();
            trackers = new HashMap<String,Integer>();  
        }
        
        private void cleanMap() {
        	trackers_Hashtag.clear();
        	trackers_username.clear();
			trackers_profileLocation.clear();
			trackers_tweetId.clear();
			trackers_content.clear();
			trackers.clear();
        }
        
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice){
        }

        public void onTrackLimitationNotice(int numLimitedStatuses) {
        }

        public void onScrubGeo(long userId, long upToStatusId) {
        }

        public void onException(Exception ex) {
        }

 		@Override
		public void onStallWarning(StallWarning arg0) {
			// TODO Auto-generated method stub
			
		}
    }

    public static void main(String[] args) throws Exception {

    	ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
          .setOAuthConsumerKey("hzxy5ZDEjd4HmlUChSZ61A")
         .setOAuthConsumerSecret("R79oaJ3P2nrIRkH4sxFJMouYPTJFKYj2fUmcCyg")
          .setOAuthAccessToken("1355534078-GZHjkui8p1b5oDREnu3WkGvSccuqXbIpYs6z3iz")
          .setOAuthAccessTokenSecret("EQk2YTwAtU95r68I9k35QWqDZ7w79xytghFoKnA");
        
        TwitterStream ts = new TwitterStreamFactory(cb.build()).getInstance();

        lsn = new myStatusListener("/home/hduser/database");
        ts.addListener(lsn);
        ts.sample();
        
    }
}
