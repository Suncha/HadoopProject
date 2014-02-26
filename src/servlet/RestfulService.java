package servlet;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/csc8711")
public class RestfulService {
	@GET
	@Produces(MediaType.TEXT_XML)
	public String getXMLInfo() throws IOException{
		String returnvalue = httpGet();
		return returnvalue;
	}
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String getHTMLInfo(){
		return "HTML Info";
	}
	@PUT
	//@Path("{stuentRollNo}")
	@Produces(MediaType.TEXT_PLAIN)
	public String updateInfo(@PathParam("RollNo") String RollNo){
		return "Success!!";
	}
	public static String httpGet() throws IOException {
		String urlStr_tmp = "http://localhost:50000/twitterTable/*";
		String urlStr= "http://localhost:50000/twitterTable/*/Top10Summary:summary";
		  URL url = new URL(urlStr);
		  HttpURLConnection conn =
		      (HttpURLConnection) url.openConnection();

		  if (conn.getResponseCode() != 200) {
		    throw new IOException(conn.getResponseMessage());
		  }

		  // Buffer the result into a string
		  BufferedReader rd = new BufferedReader(
		      new InputStreamReader(conn.getInputStream()));
		  StringBuilder sb = new StringBuilder();
		  String line;
		  while ((line = rd.readLine()) != null) {
		    sb.append(line);
		  }
		  rd.close();

		  conn.disconnect();
		  return sb.toString();
	}
	
}
