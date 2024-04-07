package task5;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// Defines T5SentiWord class to handle sentiment analysis based on a sentiment dictionary.
public class T5SentiWord {

	// Enum to represent the three basic sentiment categories.
	public enum SENTIMENT {
		NEUTRAL, NEGATIVE, POSITIVE
	};

	// Dictionary to hold the sentiment scores of words.
	private Map<String, Double> dictionary;

	// Constructor to initialize the sentiment word dictionary from a file.
	public T5SentiWord(String pathToSWN) throws IOException {
		// Initialize the dictionary HashMap to store words and their sentiment scores.
		dictionary = new HashMap<String, Double>();
		// Temporary dictionary to store detailed sentiment scores before averaging.
		HashMap<String, HashMap<Integer, Double>> tempDictionary = new HashMap<String, HashMap<Integer, Double>>();

		// Use BufferedReader to read the file at the given path line by line.
		try (BufferedReader csv = new BufferedReader(new FileReader(pathToSWN))) {
			String line;
			// Read each line until the end of the file.
			while ((line = csv.readLine()) != null) {
				// Ignore comment lines that start with "#".
				if (!line.trim().startsWith("#")) {
					// Split the line into parts using tab as delimiter.
					String[] data = line.split("\t");
					// Check for correct line format; should contain 6 parts.
					if (data.length != 6) {
						throw new IllegalArgumentException("Incorrect tabulation format in file, line: " + line);
					}

					// Calculate the synset score by subtracting the negative score from the
					// positive score.
					Double synsetScore = Double.parseDouble(data[2]) - Double.parseDouble(data[3]);
					// Split the synonym terms separated by space.
					String[] synTermsSplit = data[4].split(" ");

					// Process each synonym term.
					for (String synTermSplit : synTermsSplit) {
						// Split the synonym term from its rank.
						String[] synTermAndRank = synTermSplit.split("#");
						// Combine synonym term with part of speech tag.
						String synTerm = synTermAndRank[0] + "#" + data[0];

						// Parse the rank of the synonym term.
						int synTermRank = Integer.parseInt(synTermAndRank[1]);
						// If the term is not already in the temp dictionary, add it.
						if (!tempDictionary.containsKey(synTerm)) {
							tempDictionary.put(synTerm, new HashMap<Integer, Double>());
						}
						// Put the synset score in the map for the term.
						tempDictionary.get(synTerm).put(synTermRank, synsetScore);
					}
				}
			}

			// Average out the scores for each word across different senses.
			for (Map.Entry<String, HashMap<Integer, Double>> entry : tempDictionary.entrySet()) {
				String word = entry.getKey();
				Map<Integer, Double> synSetScoreMap = entry.getValue();

				// Calculate the weighted average of synset scores.
				double score = 0.0;
				double sum = 0.0;
				for (Map.Entry<Integer, Double> setScore : synSetScoreMap.entrySet()) {
					score += setScore.getValue() / (double) setScore.getKey();
					sum += 1.0 / (double) setScore.getKey();
				}
				score /= sum;

				// Put the final averaged score into the main dictionary.
				dictionary.put(word, score);
			}
		} catch (Exception e) {
			// Print stack trace in case of an exception.
			e.printStackTrace();
		}
	}

	// Method to extract the total sentiment score of a word across different parts
	// of speech.
	public Double extract(String word) {
		// Initialize the total score for the word.
		Double total = new Double(0);
		// Add scores from all parts of speech if present.
		if (dictionary.get(word + "#n") != null)
			total = dictionary.get(word + "#n") + total;
		if (dictionary.get(word + "#a") != null)
			total = dictionary.get(word + "#a") + total;
		if (dictionary.get(word + "#r") != null)
			total = dictionary.get(word + "#r") + total;
		if (dictionary.get(word + "#v") != null)
			total = dictionary.get(word + "#v") + total;
		return total;
	}

	// Method to analyze the overall sentiment of a text.
	public SENTIMENT analyze(String text) {
		// Split the text into words using whitespace as the delimiter.
		String[] words = text.split("\\s+");
		// Initialize a variable to keep track of the total sentiment score of the text.
		double totalScore = 0;
		// Iterate through each word in the text.
		for (String word : words) {
			// Remove any non-alphabetic characters from the word to normalize it.
			word = word.replaceAll("([^a-zA-Z\\s])", "");
			// Extract the sentiment score for the current word.
			Double score = this.extract(word);
			// If a score is found, add it to the total score.
			if (score != null) {
				totalScore += score;
			}
		}

		// Determine the overall sentiment of the text based on the total score.
		if (totalScore > 0) {
			// If the total score is positive, the sentiment is POSITIVE.
			return SENTIMENT.POSITIVE;
		} else if (totalScore < 0) {
			// If the total score is negative, the sentiment is NEGATIVE.
			return SENTIMENT.NEGATIVE;
		} else {
			// If the total score is zero, the sentiment is NEUTRAL.
			return SENTIMENT.NEUTRAL;
		}
	}
}
