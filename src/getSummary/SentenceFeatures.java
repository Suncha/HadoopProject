package getSummary;

import java.io.IOException;
import java.nio.charset.Charset;
//import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;


public class SentenceFeatures {
    
    public static IndexWriter indexWriter;
    public static Directory directory;
    public static IndexWriterConfig config;
    public static Analyzer analyzer;
    public static IndexReader reader;
    public static IndexSearcher searcher;
    //final static Charset ENCODING = StandardCharsets.UTF_8;
    public ArrayList<Double> termweight_scoreList = new ArrayList<Double>();
    public ArrayList<Double> coord_scoreList = new ArrayList<Double>();
    public ArrayList<Double> lengthNorm_scoreList = new ArrayList<Double>();
    public ArrayList<Double> jaccard_scoreList = new ArrayList<Double>();
    public ArrayList<Double> topic_scoreList = new ArrayList<Double>();
//    public List<Double> sentenceposition_scoreList = new ArrayList<Double>();
    
    public List<Double> SentenceLengthScore(List<String> sentenceList) throws IOException {
        List<Double> sentencelength_scoreList = new ArrayList<Double>();
        double currentlength = 0;
        Iterator iterator_sentenceList = sentenceList.iterator();
        
        while( iterator_sentenceList.hasNext( ) ) {
            String sentence = (String) iterator_sentenceList.next( );
            String sentence_words[] = tokensFromString(sentence);
            currentlength = sentence_words.length;
            sentencelength_scoreList.add(currentlength);
        }

        return sentencelength_scoreList;
    }
    
     
    public static void sentenceindexvalues(String post) throws IOException {
           Document doc = new Document();
           doc.add(new Field("Post", post, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
           indexWriter.addDocument(doc);
         } 
    public static void sentenceindexvalues_backup(String sentence, String document, String title) throws IOException {
           Document doc = new Document();
           doc.add(new Field("Title", title, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
           doc.add(new Field("Sentence", sentence, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
           doc.add(new Field("Document", document, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
           indexWriter.addDocument(doc);
         } 

    public static void getIndexWriter(boolean create) throws IOException {
                 directory = new RAMDirectory();
                 analyzer = new StandardAnalyzer(Version.LUCENE_36);
                 config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
                 indexWriter = new IndexWriter(directory, config);
     }    
   public void TermWeightScore(ArrayList<String> postList) throws IOException {
      //      TermFreqVector vector = reader.getTermFreqVector(docID, "ProjectTerms");
        
        Similarity sim = new DefaultSimilarity();
//        Iterator iteratorpostList = postList.iterator();
       // String title = titleSentence;
       // String topic = topicSentence;
//        String document = null;
        getIndexWriter(true);
        for(int i=0; i<postList.size(); i++){
//        	log("postList"+postList.get(i));
            sentenceindexvalues(postList.get(i));
        	
        }
        indexWriter.close();
        
        try{
            reader = IndexReader.open(directory); // read-only=true
            searcher = new IndexSearcher(reader);
            for (int i=0; i<reader.numDocs(); i++){ // the number of sentences
                    double tf_score = 0;
                    TermFreqVector tfv_post = reader.getTermFreqVector(i, "Post");
                    String[] terms_post = tfv_post.getTerms();
                    int[] freqs_post = tfv_post.getTermFrequencies(); // sentence words frequencies
                    for (int s = 0; s < terms_post.length; s++){ // while the number of sentences
                            int postFreq = reader.docFreq(new Term("Post", terms_post[s])); // sentence frequency
                            int numPost= reader.numDocs(); // the number of sentences
                            //log("Sentence = " + postFreq);
                            //log("Number of Sentence = " + numPost);
                            tf_score = tf_score + sim.tf(freqs_post[s])*Math.pow(sim.idf(postFreq, numPost),2); // score tf*isf
                    } 
                    termweight_scoreList.add(tf_score);
            }
            searcher.close();
            reader.close();
        }     
        catch (Exception pe) {
                System.out.println("IOException");
        }
        directory.close();
    }
    
   
    public String[] tokensFromString(String input) throws IOException {
                String words[] = input.split(" ");
        //        for(int i=0; i<words.length; i++) {
        //            System.out.println(words[i]);
        //        }
        return words;
    }  
    private static void log(Object aMsg){
        System.out.println(String.valueOf(aMsg));
    }
}
