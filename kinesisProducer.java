package aws.kinesis.sample;


import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;

import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.nio.ByteBuffer;

/**
 * 
 *
 */
public class ProducerApp_fh
{

//	static AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();

	public static void main( String[] args ) throws JsonProcessingException, JSONException {
		BasicAWSCredentials awsCredentials = new BasicAWSCredentials("xyz", "abcd");

		AmazonKinesisFirehose firehoseClient = AmazonKinesisFirehoseClient.builder()
												.withRegion(Regions.US_EAST_1)
												.withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
												.build();
		/*JSONObject messageJson = new JSONObject();
		messageJson.put("key", "EventName");

		messageJson.put("data","{name:prakash,Age:35}\n");
		*/

		//ByteBuffer smallBuffer = ByteBuffer.wrap("small".getBytes());
		String message = "{\"Name\":\"prakash1234\",\"Age\":41}\n";
		String message1 = "{\"Header\":\"EventName\"}";
		String finalmsg = message.concat(message1).concat("\n");
		//finalmsg.concat(message1);
		//finalmsg.concat("\n");
		System.out.println(finalmsg);
		Gson gson = new Gson();
		String json = gson.toJson(finalmsg);
		JSONObject messageJson = new JSONObject("{\"phonetype\":\"N95\",\"cat\":\"WP\"},{\"name\":\"prakash\"}");

		PutRecordRequest putRecordRequest = new PutRecordRequest();
		putRecordRequest.setDeliveryStreamName("test");


		//Record record = new Record().withData(ByteBuffer.wrap(messageJson.toString().getBytes()));
		Record record = new Record().withData(ByteBuffer.wrap(json.getBytes()));

		putRecordRequest.setRecord(record);
		//for(int i =0;i < 1000;i++) {
			firehoseClient.putRecord(putRecordRequest);
		//}
		//com.amazonaws.services.kinesisfirehose.model.PutRecordResult putRecordResult = firehoseClient.putRecord(putRecordRequest);

	}

}
