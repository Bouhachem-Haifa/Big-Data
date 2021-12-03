package tn.isima.tp1; 
  import org.apache.hadoop.io.DoubleWritable; 
  import org.apache.hadoop.io.Text; 
  import org.apache.hadoop.mapreduce.Mapper; 
  
  import java.io.IOException; 
  import java.util.StringTokenizer; 
  
  public class TokenizerMapper 
        extends Mapper<Object, Text, Text, DoubleWritable>{ 
     
    public static boolean isNumeric(String string) { 
    Double intValue; 
    
     System.out.println(String.format("Parsing string: \"%s\"", string)); 
    
     if(string == null || string.equals("")) { 
         System.out.println("String cannot be parsed, it is null or empty."); 
         return false; 
     } 
      
     try { 
         intValue = Double.parseDouble(string); 
         return true; 
     } catch (NumberFormatException e) { 
         System.out.println("Input String cannot be parsed to Integer."); 
     } 
     return false; 
 } 
     
    private Text word = new Text(); 
  
  public void map(Object key, Text value, Mapper.Context context ) throws IOException, InterruptedException { 
        String s1=value.toString().toString().trim().replaceAll("  +", "\t"); 
         
        System.out.println("s1="+s1); 
        StringTokenizer itr = new StringTokenizer(s1); 
        
        String magasin=""; 
        String v=""; 
        Double price=0.0; 
        int i=0;
          while (itr.hasMoreTokens()) { 
         v=itr.nextToken(); 
         if (i==2){ 
            magasin=v.toString();} 
         if (i>2 && isNumeric(v.toString())){ 
                price=Double.parseDouble(v.toString()); 
                context.write(new Text(magasin), new DoubleWritable(price));} 
          
System.out.println("magasin="+magasin+"price"+price); 
         i++; 
          
          
        } 
         
             
        } 
    }
