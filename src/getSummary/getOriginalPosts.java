package getSummary;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class getOriginalPosts {
		public static HashMap<String,Double> summaryList = new HashMap<String,Double>();  
		private static ArrayList<String> summaryListStemmed = new ArrayList<String>();
		private static ArrayList<Double> summaryListScore = new ArrayList<Double>();
//		private static double[] final_summaryListScore = new ArrayList<Double>();
		 
	   // public static Charset ENCODING = StandardCharsets.UTF_8;
	    final static String summary=null;
	    public void  getOriginalPosts(List<String> args) throws IOException {

	        String STOP_WORD = "/home/hduser/workspace/HbaseProject/data/StopWords.txt";
	        double[] final_summaryListScore = new double[args.size()];
	        //double[][] final_jaccard_score = new double[args.length][];
	        //List<String> outputlines = new ArrayList();
	        //List<String> totalsentence = new ArrayList();
	        List<String> originalList = new ArrayList<String>();
	        originalList = args;
	        for (int i = 0; i < originalList.size(); i++) {          // recursively index them
	        		//log("args.size()"+i);
	                String post = originalList.get(i);
	            
	                //String sentenceList = null;
	                //List<Double> termweightscore = new ArrayList<Double>(); // tf-isf score
	                //List<Double> jaccardSim_score = new ArrayList<Double>();
	                try{
	                    Tokenization token = new Tokenization();
	                    token.Tokenization(post, STOP_WORD);
	                    //sentenceList = token.sentenceList;
	                    summaryListStemmed.add(token.postStopandStemed);

		            } catch (Exception e) {
		                System.out.println("error : " + e);
		            }
		        }      
	        try{
	        		SentenceFeatures sentencefeatures = new SentenceFeatures();            
                    sentencefeatures.TermWeightScore(summaryListStemmed);
                    summaryListScore = sentencefeatures.termweight_scoreList;
	                    //jaccardSim_score = sentencefeatures.jaccard_scoreList;
	                    
                    final_summaryListScore = convertScores(summaryListScore);
                    for (int s = 0; s<originalList.size(); s++){
                    //	log(summaryListStemmed.get(s));
                    //	log(Double.toString(final_summaryListScore[s]));
                    	summaryList.put(originalList.get(s), final_summaryListScore[s]);
                    }
	    	
	        } catch (Exception e) {
                System.out.println("error tttt: " + e);
            }
	      
	   }
	     
	    public static String[] tokensFromString(String query) throws IOException {
	                String words[] = query.split(" ");
	        return words;
	    }    
	    public static double[] convertScores(ArrayList<Double> file) throws IOException {
	        double[] scores = new double[file.size()];
	        double[] realscore = new double[file.size()];
	        double maxscore = 0;
	        int num = 0;
	        Iterator<Double> iterator_scores = file.iterator();
	        while( iterator_scores.hasNext( ) ) {
	            Double score = iterator_scores.next( );
	            if (score >= maxscore){
	                maxscore = score;
	            }
	            scores[num] = score;
	            num++;
	        }
	        for (int i=0; i<scores.length; i++){
	            realscore[i] = scores[i] / maxscore;
	        }
	        return realscore;
	    }
	  
	    
	    private static void log(Object aMsg){
	        System.out.println(String.valueOf(aMsg));
	    }
	    public void error(String msg)
	       {
	           System.out.println("\n\n"+msg+"\n\n");
	           Runtime cur = Runtime.getRuntime();
	           cur.exit(1);
	       }
	}
