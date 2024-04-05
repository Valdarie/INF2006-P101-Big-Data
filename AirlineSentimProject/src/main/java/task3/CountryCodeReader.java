package task3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class CountryCodeReader {
    private Map<String, String> countryCodeToNameMap = new TreeMap<>();

    public Map<String, String> readCountryCodes(String filePath) throws IOException {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    countryCodeToNameMap.put(parts[0], parts[1]);
                }
            }
        }
        return countryCodeToNameMap;
    }
}
