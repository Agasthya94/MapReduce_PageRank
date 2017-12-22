/* @Author: Agasthya Vidyanath Rao Peggerla. */ 

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.StringTokenizer;


public class Page_Rank extends Configured implements Tool {
	
	// Pattern to find the title block ( <title> .... </title> )
	private static final Pattern Title = Pattern .compile("<title>(.*?)</title>");	
	// Pattern to find the text block ( <text> .... </text> )
	private static final Pattern Text_Limit = Pattern .compile("<text(.*?)</text>");
	// Pattern to find the links ( [[link]] )
	private static final Pattern Links = Pattern .compile("\\[\\[(.*?)\\]\\]");
	
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new Page_Rank(), args);
      System .exit(res);
   }
   
   public int run( String[] args) throws  Exception {
	   
	  // Job to do the initial processing
	  Job job_initial  = Job .getInstance(getConf(), " pagerank ");
      job_initial.setJarByClass( this .getClass());
      
      //Input for initial job
      FileInputFormat.addInputPaths(job_initial, args[0]);			
      //Output of the initial job stored in  temporary file
      FileOutputFormat.setOutputPath(job_initial,  new Path("tempoutput/file0"));
      
      job_initial.setMapperClass( Map .class);									
      job_initial.setReducerClass( Reduce .class);
      job_initial.setOutputKeyClass( Text .class);
      job_initial.setOutputValueClass( Text .class);
      
      job_initial.waitForCompletion( true);     
      
      // input path for the iterator job.
      String inputpath_iteratorJob = "intermediate/files0";
      String outputpath_iteratorJob = "";
      //Running the iterator job for 10 iterations.
    	 for(int i = 0; i <10; i++){
    	  
    	  //Input ofr the iterator job
    	  inputpath_iteratorJob = "tempoutput/file" + i;
    	  // Output path for the iterator job.
    	  outputpath_iteratorJob = "tempoutput/file" + Math.abs(i+1); 
    	  
    	  Job job_iterator  = Job .getInstance(getConf(), " Iterator ");
    	  job_iterator.setJarByClass( this .getClass());

          FileInputFormat.addInputPaths(job_iterator,  inputpath_iteratorJob);
          FileOutputFormat.setOutputPath(job_iterator,  new Path(outputpath_iteratorJob));
          
          job_iterator.setMapperClass( Mapper_IteratorJob.class);
          job_iterator.setReducerClass( Reducer_IteratorJob.class);
          job_iterator.setOutputKeyClass( Text .class);
          job_iterator.setOutputValueClass( Text .class);
     	  job_iterator.waitForCompletion( true);															

    	 }
    	 
    	// Job to format the final output.
      	Job cleaner  = Job .getInstance(getConf(), " clean ");
      	cleaner.setJarByClass( this .getClass());

        FileInputFormat.addInputPaths(cleaner,  outputpath_iteratorJob);
        FileOutputFormat.setOutputPath(cleaner,  new Path(args[1]));
        
        cleaner.setMapperClass( Mapper_Cleaner .class);
        cleaner.setReducerClass( Reducer_Cleaner .class);
      
        cleaner.setOutputKeyClass( DoubleWritable .class);
        cleaner.setOutputValueClass( Text .class);
       // cleaner.waitForCompletion(true);

        return cleaner.waitForCompletion( true)  ? 0 : 1;    
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
   
     
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
         
    	  String lines = lineText.toString();
    	  if(!lines.isEmpty()){
    	  String title = new String();
    	  String links = "";
    	  String title_links = new String();
    	  //Check for the title tag in the line
    	  Matcher title_match = Title.matcher(lines);
    	  while(title_match.find()){
    		  //retrieve the content present within the title tag.
    		  title = title_match.group(1);   		  
    	  }
    	  //Check for the text tags.
    	  Matcher text_match = Text_Limit.matcher(lines);
    	  while(text_match.find()){
    		  //Retrieve the content present within the text tag.
    		  String text_line = text_match.group(1);
    		  //Check for the links.
    		  Matcher link_match = Links.matcher(text_line);
    		  while(link_match.find()){
    			//Retrieve the links.
    			  if(links == ""){
    				  links = link_match.group(1);
    			  }
    			  else{
    				  links = links + "$$$" + link_match.group(1);
    			  }
    		  }
    	  }
    	
    	  title_links = title + "#####" + links;
    	  context.write(new Text("KEY"), new Text(title_links));
    	  //Output of the mapper will be in the following format.
    	  // KEY	title#####links
         
      }
      }
   }
   
   public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
	      @Override 
	      public void reduce( Text word,  Iterable<Text > list_lines,  Context context)
	         throws IOException,  InterruptedException {
	    	
	    	//Input = KEY	title#####links
	    	List<String> lines = new ArrayList<String>();
	          for (Text line : list_lines) {
	        	  //Add all the lines to an arraylist
	        	  String s = line.toString();             
	              lines.add(s);
	          }
	          //Size of the arraylist = number of lines in the input.
	          int N = lines.size();							
	          double rank = 1.0/N;
	          
	          for(String s : lines){
	        	  //Separating the title and links
	        	  String strarr[] = s.split("#####");
	        	  String title = strarr[0];
	        	  if(strarr.length > 1){
	        	  String links = strarr[1];
	        	  String title_rank = title + "#####" + rank ;
	        	  context.write(new Text(title_rank), new Text(links));}
	        	  else{
	        		  String links ="";
		        	  String title_rank = title + "#####" + rank ;
		        	  context.write(new Text(title_rank), new Text(links)); 
	        	  }
	        	  //Output = title#####rank	links
	          }	        
	      }
	   }

   public static class Mapper_IteratorJob extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
	   
	      public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {			
	    	 
	    	  // INput is in the form of title#####pagerank	link1$$$link2....
	         String line = lineText.toString();
	         String page_rank_link[] = line.split("	");
	         //Separate page####rank and links
	         String page_rank[] = page_rank_link[0].split("#####");
	         //Separate page and rank
	         String page = page_rank[0];
	         Double pr = Double.valueOf(page_rank[1]);
	         
	         if(page_rank_link.length<=1){
	        	 context.write(new Text(page),new Text(pr.toString()));
	         }
	         else{
	        	 // Separate all the links
	        	 StringTokenizer s = new StringTokenizer(page_rank_link[1],"$$$");
	        	 String links = page_rank_link[1]; 
	        	 int size = s.countTokens();
	        	 //Calculate the rank.
	        	 String link_rank = String.valueOf(pr/size); 
	               while (s.hasMoreTokens()) {
	            	   String key = s.nextToken().toString();
	            	   context.write(new Text(key), new Text(link_rank));
	               		}
	         String value = pr + "#####" + links;
	         context.write(new Text(page), new Text(value));
	         //Output title	pagerank#####link1$$$link2$$$....
	         }	         
	      }	      
	   }
   
   public static class Reducer_IteratorJob extends Reducer<Text ,  Text ,  Text ,  Text > {
	      @Override 
	      public void reduce( Text word,  Iterable<Text > links_list,  Context context)
	         throws IOException,  InterruptedException {
	    	  //Input = title	pagerank#####link1$$$link2$$$....
	    	   double rank=0 ;
	    	   String links=new String() ;
	    	   Double df = 0.85;
	    	   
	    	  for (Text tex : links_list){
	    		  String s = tex.toString();
	    		  // s == pagerank#####link1$$$link2$$$...
	    		  if(s.contains("#####")){
	    			  //Separating the pagerank and links.
	    			  String[] rank_link = s.split("#####");		 
	    			  										
	    			  if(rank_link.length>1){
	    				  links = rank_link[1];
	    			  }	    			  						    			 	    			 
	    		  }
	    		  else{
	    			  rank += Double.valueOf(s);
	    		  }
	    	  }									 	  
	    	  if(!links.equals("")) {
	    		  //Calculating the pagerank.
	    		  rank = (1-df)+ df*(rank);//calculating page rank using and damping factor 
	    		  context.write(new Text(word.toString()+"#####"+String.valueOf(rank)), new Text(links));
	    		  //Output = title#####pagerank	link1$$$link2....
	    	  }	    	  	    	  
	      }
	   }
   
   public static class Mapper_Cleaner extends Mapper<LongWritable ,  Text ,  DoubleWritable ,  Text > {
	   
	      public void map( LongWritable offset,  Text lines,  Context context)
	        throws  IOException,  InterruptedException {
	    	  String line  = lines.toString();
	    	  // Input = title#####pagerank	link1$$$link2$$$....
	                String[] pageANDrank  = line.split("	");
	                //Separating the title####pagerank and links
	                String[] page_rank = pageANDrank[0].split("#####");
	                //Separating the title and pagerank.
	                String page = page_rank[0];
	                Double rank = Double.parseDouble(page_rank[1]);
	                rank=rank * -1;
	                //Sorting the data to output in descending order of page ranks.
	               context.write(new DoubleWritable(rank), new Text(page));	   
	               // Output = -pagerank	title	                   
	      }
	      
	   }
 
   public static class Reducer_Cleaner extends Reducer<DoubleWritable ,  Text ,  Text ,  DoubleWritable > {
	   
	   //Deletes the temporary files used by the iterator job.
	   @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	      FileSystem filesys = FileSystem.get(context.getConfiguration());
	      filesys.delete(new Path("tempoutput"), true);
	      super.cleanup(context);
	    }
	      @Override 
	      public void reduce( DoubleWritable rank,  Iterable<Text > lines,  Context context)
	         throws IOException,  InterruptedException {			 
	    	 // Input = title and page rank arranged in descending order ( -pagerank	title )
	    	 int i=0;
	    	 if (i<=100)
	    	 {
	    		 for(Text page:lines)
	    		 {
	    			 context.write(page,new DoubleWritable(Math.abs(rank.get())));
	    			 // Output = title	pagerank
	    			 i++;
	    		 }
	    	 }
	    	 }	    	  	    	  
	      }	
   }

  








