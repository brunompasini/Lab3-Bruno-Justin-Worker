package emse;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CSVParser {
    private static final String SAMPLE_CSV_FILE_PATH = "data.csv";

    public static void main(String[] args) throws IOException, CsvException {

        try (
                Reader reader = Files.newBufferedReader(Paths.get(args[0]));//SAMPLE_CSV_FILE_PATH
                CSVReader csvReader = new CSVReader(reader);
        ) {
            List<ArrayList<Integer>> amountSoldByCountryByProduct = new ArrayList<ArrayList<Integer>>();
            TransactionList transactions = new TransactionList();

            String[] nextRecord;
            csvReader.skip(1);//we skip the headers
            // Reading Records One by One in a String array

            while ((nextRecord = csvReader.readNext()) != null) {
                String product = nextRecord[2];
                String price = nextRecord[3];
                String country = nextRecord[8];
                //We build an instance of Transaction class
                transactions.addTransaction(new Transaction(product, country, Integer.parseInt(price)));
            }

            //we create a file
            try {
                File myObj = new File("data.txt");
                if (myObj.createNewFile()) {
                    System.out.println("File created: " + myObj.getName());
                } else {
                    System.out.println("File already exists.");
                }
            } catch (IOException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }
                //We write into the file
                try {
                    FileWriter myWriter = new FileWriter("data.txt");
                    String newLine = System.getProperty("line.separator");
                    for (String country : transactions.getCountries()) {
                        myWriter.write("Country : " + country+newLine);
                        myWriter.write("Average amount sold per product in " + country + " : " + transactions.averageSoldPerCountryPerProduct(country) + "$"+newLine);
                        for (String product : transactions.getProducts()) {
                            myWriter.write("Amount of " + product + " sold : " + transactions.nBSalesByCountryByProduct(country, product) + " TOTAL : " + transactions.salesByCountryByProduct(country, product) + "$"+newLine);
                        }
                        myWriter.write("=========================="+newLine);
                    }
                    myWriter.close();
                    System.out.println("Successfully wrote to the file.");
                } catch (IOException e) {
                    System.out.println("An error occurred.");
                    e.printStackTrace();
                }


               /* for (String country : transactions.getCountries()) {
                    System.out.println("Country : " + country+"\n");
                    System.out.println("Average amount sold per product in " + country + " : " + transactions.averageSoldPerCountryPerProduct(country) + "$");
                    for (String product : transactions.getProducts()) {
                        System.out.println("Amount of " + product + " sold : " + transactions.nBSalesByCountryByProduct(country, product) + " TOTAL : " + transactions.salesByCountryByProduct(country, product) + "$");
                    }
                    System.out.println("==========================\n");
                }*/
            }
        }
    }
