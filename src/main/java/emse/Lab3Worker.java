package emse;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.opencsv.CSVReader;
import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.exceptions.CsvValidationException;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class Lab3Worker {
	
	public static String queueUrl = "";
	public static String queueName = "met le nom de la queue ici stp";
	public static void main(String[] args) {
		
		queueUrl = "inbox";
		
		Region region = Region.US_EAST_1;
			
		Ec2Client ec2 = Ec2Client.builder()
                .region(region)
                .build();
		S3Client s3 = S3Client.builder()
				 .region(region)
				 .build();
		
		SqsClient sqs = SqsClient.builder()
				 .region(region)
				 .build();
		
		try {
			waitReadWrite(queueUrl, ec2, s3, sqs);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CsvValidationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	};
	
	public static void waitReadWrite(String queueUrl,
			Ec2Client ec2, S3Client s3, SqsClient sqs) throws IOException, CsvValidationException {
		
		
		ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(2) // could be changed to 2, the minimum
                .build();
		List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
        
		// first message would be bucket name
		// and second the file name
		
		Message message0 = messages.get(0);
		String fileBucketName = message0.body();
		
		Message message1 = messages.get(1);
		String fileKey = message1.body();

		
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(fileBucketName)
                .key(fileKey)
                .build();

        
        ResponseBytes<GetObjectResponse> dowloadedBytes = s3.getObjectAsBytes(getObjectRequest);
        byte[] dowloadObject = dowloadedBytes.asByteArray();
        File dowloadedFile = new File(fileKey); // file key is file name, so saving as its original name
        OutputStream outStream = new FileOutputStream(dowloadedFile);
        outStream.write(dowloadObject);
        outStream.close(); // Closing stream after reading the file is finished
    
            
        // Now reading csv
        
        Reader reader = Files.newBufferedReader(Paths.get(fileKey));
        CSVReader csvReader = new CSVReader(reader);
        csvReader.skip(1); // We don't want the title of each column
        
        String[] thisLine;
        int numberOfSales = 0;
        SaleList saleList = new SaleList();
        
        while ((thisLine = csvReader.readNext()) != null) {
        	numberOfSales = numberOfSales+1;
        	
        	
            String product = thisLine[2];
            String price = thisLine[3];
            int priceInt = Integer.parseInt(price);
            String country = thisLine[8];
            saleList.addSale(new Sale(product, country, priceInt));
            
        }
        
        File outFile = new File("sales_data.txt");
        FileWriter writeFile = new FileWriter("sales_data.txt");
        String slashN = System.getProperty("line.separator"); // \n character
        
        for (String country : saleList.getCountries()) {
        	writeFile.write("Country : " + country+slashN);
        	writeFile.write("Average amount sold per product in " + country + " : " + saleList.averageSoldPerCountryPerProduct(country) + "$"+slashN);
            for (String product : saleList.getProducts()) {
            	writeFile.write("Amount of " + product + " sold : " + saleList.nBSalesByCountryByProduct(country, product) + " TOTAL : " + saleList.salesByCountryByProduct(country, product) + "$"+slashN);
            }
        }
        writeFile.close();

        PutObjectRequest objectRequest = PutObjectRequest
        		.builder()
                .bucket(fileBucketName)
                .key(fileKey)
                .build();

        s3.putObject(objectRequest, RequestBody.fromBytes(getObjectFile(fileKey)));
        
        
        // Sending it's identifier as a message

        //String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        
        queueName = "outbox";
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        CreateQueueResponse createResult = sqs.createQueue(request);

        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
            .queueName(queueName)
            .build();

        SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
            .queueUrl(queueUrl)
            .messageBody(fileBucketName)
            .delaySeconds(5)
            .build();

        sqs.sendMessage(sendMsgRequest);
    
    	SendMessageRequest sendMsgRequest2 = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(fileKey)
                .delaySeconds(5)
                .build();

            sqs.sendMessage(sendMsgRequest2);
        	
	
	};
	
	
	// Return a byte array
    private static byte[] getObjectFile(String filePath) {

        FileInputStream fileInputStream = null;
        byte[] bytesArray = null;

        try {
            File file = new File(filePath);
            bytesArray = new byte[(int) file.length()];
            fileInputStream = new FileInputStream(file);
            fileInputStream.read(bytesArray);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return bytesArray;
    }
	
}
