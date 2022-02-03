package emse;

import java.io.File;
import java.util.List;
import java.util.Scanner;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Instance;


public class Lab2 {
	public static void main(String[] args) {
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
		//sendFileS3(s3,"mybucket03082000");
		//sendMessageQueue(sqs, "brustin-chez-bruno");
		receiveMessages(sqs);
        

//createEC2(ec2,"java_now","ami-0ed9277fb7eb570c9");
		 

	};
	
	public static void listEC2(Ec2Client ec2) {
        String nextToken = null;
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    System.out.println("Instance Id is " + instance.instanceId());
                    System.out.println("Image id is "+  instance.imageId());
                    System.out.println("Instance type is "+  instance.instanceType());
                    System.out.println("Instance state name is "+  instance.state().name());
                    System.out.println("monitoring information is "+  instance.monitoring().state());
                    System.out.println("");

            }
        }
            nextToken = response.nextToken();
        } while (nextToken != null);


};
	
	public static void createEC2(Ec2Client ec2,String name, String amiId) {
		RunInstancesRequest runRequest = RunInstancesRequest.builder().imageId(amiId).instanceType(InstanceType.T2_MICRO).maxCount(1).minCount(1).build();
        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();
        Tag tag = Tag.builder().key("Name").value(name).build();
        CreateTagsRequest tagRequest = CreateTagsRequest.builder().resources(instanceId).tags(tag).build();
        ec2.createTags(tagRequest);
        System.out.printf("Successfully started EC2 Instance %s based on AMI %s",instanceId, amiId);
	}
	
	
	public static void listS3(S3Client s3) {
        ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
        ListBucketsResponse listBucketsResponse = s3.listBuckets(listBucketsRequest);
        listBucketsResponse.buckets().stream().forEach(x -> System.out.println(x.name()));

	};
	
	public static void createS3(S3Client s3, String bucketName) {
		
		S3Waiter s3Waiter = s3.waiter();
		CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
		        .bucket(bucketName)
		        .build();
		
		s3.createBucket(bucketRequest);
		HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
		        .bucket(bucketName)
		        .build();
		
		WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
		waiterResponse.matched().response().ifPresent(System.out::println);
		System.out.println(bucketName +" is ready");
	};
	
	public static void sendFileS3(S3Client s3, String bucketName, String path) {
		File file = new File(path);
		String key = file.getName();
		PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName).key(key)
                .build();

        s3.putObject(objectRequest, RequestBody.fromFile(file));
       // s3.putObject(bucketName,"Desktop/tests3.txt", new File("/Users/bruno/Desktop/tests3.txt");
        System.out.println("fim");
	
	};
	
	public static String createQueue(SqsClient sqs, String queueName) {
		CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
				.queueName(queueName)
				.build();
		sqs.createQueue(createQueueRequest);
		
		GetQueueUrlResponse getQueueUrlResponse = sqs.getQueueUrl(GetQueueUrlRequest.builder()
																.queueName(queueName)
																.build());
        String queueUrl = getQueueUrlResponse.queueUrl();
        return queueUrl;
		
	};
	public static void sendMessageQueue(SqsClient sqs, String queueName) {
		String message = "brustin - values.csv";
		CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        CreateQueueResponse createResult = sqs.createQueue(createQueueRequest);

        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
            .queueName(queueName)
            .build();

        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

        SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
            .queueUrl(queueUrl)
            .messageBody(message)
            .delaySeconds(5)
            .build();
        sqs.sendMessage(sendMsgRequest);
	};
	
	public static void receiveMessages(SqsClient sqs) {
		ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl("https://sqs.us-east-1.amazonaws.com/964445416176/brustin-chez-bruno")
                .build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();

            // Print out the messages
        for (Message m : messages) {
            System.out.println("\n" +m.body());
        }
	};
	

    public void cleanQueue(SqsClient sqs, String queueUrl) {

    	// Commenting so we ue url directely
        //GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder().queueName(queueName).build();

        PurgeQueueRequest queueRequest = PurgeQueueRequest.builder()
                .queueUrl(queueUrl)
                .build();

        sqs.purgeQueue(queueRequest);
    }

	
	public static void deleteMessagesAndQueue(SqsClient sqs, String queueName, List<Message> messages) {
		
        
		// Getting queue url
		
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();		
		
		
        // Deleting all messages before deleting the queue
		// Iterating over all messages
        for (Message message : messages) {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
            sqs.deleteMessage(deleteMessageRequest);
        }

		
		
        // Now we delete the empty queue
        DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                .queueUrl(queueUrl)
                .build();

        sqs.deleteQueue(deleteQueueRequest);

	};
	
}

