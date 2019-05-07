import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.stream.Collectors;



public class StreamReader{


    public static void main( String[] args ) {

        String amazonAccessKey = "AKIA3IEKS2VZJH36KHWQ";
        String amazonSecretKey = "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";
        String amazonStreamName = "fluxKinesis";
        String amazonRegionName = "eu-west-1";

        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(amazonAccessKey, amazonSecretKey);
        @SuppressWarnings("deprecation")
        AmazonKinesisClient client = new AmazonKinesisClient(awsCredentials);
        client.setRegion(RegionUtils.getRegion(amazonRegionName));
        //GetRecordsResult gggg = client.getRecords(new GetRecordsRequest().clone());
        //gggg.toString();
        //System.out.println("\nlist of records:");

        long recordNum = 0;
        final int INTERVAL = 2000;

        // Getting initial stream description from aws
        System.out.println(client.describeStream(amazonStreamName).toString());
        List<Shard> initialShardData = client.describeStream(amazonStreamName).getStreamDescription().getShards();
        System.out.println("\nlist of shards:");
        initialShardData.forEach(d->System.out.println(d.toString()));

        // Getting shardIterators (at beginning sequence number) for reach shard
        List<String> initialShardIterators = initialShardData.stream().map(s ->
                client.getShardIterator(new GetShardIteratorRequest()
                        .withStreamName(amazonStreamName)
                        .withShardId(s.getShardId())
                        .withStartingSequenceNumber(s.getSequenceNumberRange().getStartingSequenceNumber())
                        .withShardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                ).getShardIterator()
        ).collect(Collectors.toList());

        System.out.println("\nlist of ShardIterators:");
        initialShardIterators.forEach(i -> System.out.println(i));



        // WARNING!!! Assume that only have one shard. So only use that shard
        String shardIterator = initialShardIterators.get(0);


        GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
        getRecordsRequest.setShardIterator(shardIterator);
        getRecordsRequest.setLimit(25);

        GetRecordsResult recordResult = client.getRecords(getRecordsRequest);

        System.out.println("\nsize array of records: " + recordResult.getRecords().size());
        System.out.println("\nwaiting for messages....");

        recordResult.getRecords().forEach(record -> {
            try {
                String rec = new String(record.getData().array(), "UTF-8");
                System.out.println("\nKinesis record: " + record.toString());
                System.out.println("\nKinesis record en string: " + rec);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }});

        }
}
