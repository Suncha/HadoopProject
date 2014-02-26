package getSummary;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;

import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import java.nio.charset.Charset;

import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.Version;

public class Tokenization {
    
    //public Charset ENCODING = StandardCharsets.UTF_8;
    public Set<String> stopWords = null;
    public String postSentence = null;
    public String postOriginal = null;
    public String postStopandStemed = null;
    
    //public List<String> splitSentences_original = null;
    public List<String> sentenceList = new ArrayList<String>();
    public List<String> titleWordsLIst = new ArrayList<String>();
    public List<String> originalsentenceList = new ArrayList<String>();
    public String docString = null;
    public String title = null;
    public String topic = null;
    public int num = 0;

    public void Tokenization(String post, String stopwords) throws IOException{
    	//log("Tokenization"+post+stopwords);
        stopWords = loadStopWords(stopwords);
        postSentence = post;
        postOriginal = post;
        //splitSentences_original = loadSplitSentence2(filename);
        boolean firstsen_check = true;
        String titleStemmed = null;
        postStopandStemed= removeStopWordsAndStem(postSentence);
        //log("postOriginal"+postOriginal);
        //log("postStopandStemed"+postStopandStemed);
        //topic = removeStopWordsAndStem(readOneSentence(topicwords)); // topic sentence stemming 
        
//      String titlewordlist[] = tokensFromString(titleStemmed);
//      for(int i=0; i<titlewordlist.length; i++){
//          titleWordsLIst.add(titlewordlist[i]);
//      }

  }

    public String[] tokensFromString(String input) throws IOException {
        String words[] = input.split(" ");
        return words;
    }    

   
  
  public Set<String> loadStopWords(String aFileName) throws IOException{
    String line;
    FileInputStream fis = new FileInputStream(aFileName);
    InputStreamReader in = new InputStreamReader(fis, "UTF-8"); 
    
    BufferedReader reader = new BufferedReader(in);
    String readLine;
    Set<String> stopWords = new HashSet<String>();

    while ((readLine = reader.readLine()) != null) {
    	stopWords.add(readLine.toLowerCase()); 
    	}
    return stopWords;
  }
  
  public String removeStopWordsAndStem(String input) throws IOException {
   
        TokenStream tokenStream = new StandardTokenizer(
                Version.LUCENE_36, new StringReader(input));

        tokenStream = new StopFilter(true, tokenStream, stopWords);
        tokenStream = new PorterStemFilter(tokenStream);

        StringBuilder sb = new StringBuilder();
        
        TermAttribute termAttr = tokenStream.getAttribute(TermAttribute.class);
        int check = 0;
        while (tokenStream.incrementToken()) {
            check = 1;
            if (sb.length() > 0){
                sb.append(" ");
            }
            sb.append(termAttr.term());
        }
        if (check == 0){
            String abc = "None";
            sb.append(abc);
        }
        return sb.toString();
    }
  private static void log(Object aMsg){
      System.out.println(String.valueOf(aMsg));
  }
} 