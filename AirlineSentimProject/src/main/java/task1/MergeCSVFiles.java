/**
 * 2202217 Teo Xuanting
 */

package task1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MergeCSVFiles {

    public static void main(String[] args) {
        // Check if the correct number of command-line arguments is provided
        if (args.length != 2) {
            System.err.println("Usage: java MergeCSVFiles <input_directory> <output_file>");
            System.exit(1);
        }

        try {
            // Get input directory and output file from command-line arguments
            String inputDirectoryPath = args[0];
            String outputFilePath = args[1];

            // Create File objects for input directory and output file
            File inputDirectory = new File(inputDirectoryPath);
            File outputFile = new File(outputFilePath);

            // Get a list of all CSV files in the input directory
            File[] csvFiles = inputDirectory.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));

            // Initialize a list to store the lines from all CSV files
            List<String> mergedLines = new ArrayList<>();

            // Iterate through each CSV file
            for (File csvFile : csvFiles) {
                BufferedReader reader = new BufferedReader(new FileReader(csvFile));
                String line;

                // Read each line from the current CSV file and add it to the mergedLines list
                while ((line = reader.readLine()) != null) {
                    mergedLines.add(line);
                }

                reader.close();
            }

            // Write the merged lines to the output file
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
            for (String mergedLine : mergedLines) {
                writer.write(mergedLine);
                writer.newLine();
            }

            writer.close();

            System.out.println("Merged CSV files successfully.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
