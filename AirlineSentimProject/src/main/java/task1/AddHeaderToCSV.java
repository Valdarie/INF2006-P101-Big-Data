/**
 * 2202795 Leo En Qi Valerie
 */

package task1; 

import java.io.*;

public class AddHeaderToCSV { 
 
    public static void addHeaders(String inputFilePath, String outputFilePath) { 
        // Headers 
        String headers = "_unit_id,_created_at,_golden,_id,_missed,_started_at,_tainted,_channel,_trust,_worker_id,_country,_region,_city,_ip,airline_sentiment,negativereason,airline,airline_sentiment_gold,name,negativereason_gold,retweet_count,text"; 
 
        try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath)); 
             BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilePath))) { 
 
            // Write headers to the output file 
            bw.write(headers); 
            bw.newLine(); 
 
            // Copy data from the input file to the output file 
            String line; 
            while ((line = br.readLine()) != null) { 
                bw.write(line); 
                bw.newLine(); 
            } 
 
            System.out.println("Headers added successfully to " + outputFilePath); 
 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
    } 
 
    public static void main(String[] args) { 
        if (args.length < 2) { 
            System.err.println("Usage: java AddHeaderToCSV <inputFilePath> <outputFilePath>"); 
            System.exit(1); 
        } 
 
        String inputFilePath = args[0]; 
        String outputFilePath = args[1]; 
 
        addHeaders(inputFilePath, outputFilePath); 
    } 
}