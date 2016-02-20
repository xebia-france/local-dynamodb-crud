package com.onneby.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

/**
 * Created by ponneby on 20/02/16.
 */
public class CreateTableTest {
    @BeforeClass
    public static void startAnInMemoryServer() throws Exception {
        final String[] localArgs = {"-inMemory", "-port", "30000", "-sharedDb"};

        final DynamoDBProxyServer server = ServerRunner.createServerFromCommandLineArgs(localArgs);
        server.start();


    }

    @Test
    public void createTable() throws Exception {

        AmazonDynamoDBClient dynamoDbClient = new AmazonDynamoDBClient();
        dynamoDbClient.setEndpoint("http://localhost:30000");
        DynamoDBMapperConfig config = new DynamoDBMapperConfig(DynamoDBMapperConfig.DEFAULT, new DynamoDBMapperConfig(ConversionSchemas.V2));
        final DynamoDBMapper dynamoDBMapper = new DynamoDBMapper(dynamoDbClient, config);
        CreateTableRequest req = dynamoDBMapper.generateCreateTableRequest(User.class);
        req.setProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));

        DynamoDB dynamoDB = new DynamoDB(dynamoDbClient);

        Table newTable = dynamoDB.createTable(req);
        newTable.waitForActive();

    }

    @Test
    public void saveItem() throws Exception {
        AmazonDynamoDBClient dynamoDbClient = new AmazonDynamoDBClient();
        dynamoDbClient.setEndpoint("http://localhost:30000");
        DynamoDBMapperConfig config = new DynamoDBMapperConfig(DynamoDBMapperConfig.DEFAULT, new DynamoDBMapperConfig(ConversionSchemas.V2));
        final DynamoDBMapper dynamoDBMapper = new DynamoDBMapper(dynamoDbClient, config);
        CreateTableRequest req = dynamoDBMapper.generateCreateTableRequest(User.class);
        req.setProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));

        User user = new User();
        user.setId(UUID.randomUUID().toString());
        dynamoDBMapper.save(user);
    }

    @DynamoDBTable(tableName = "User")
    public class User {
        @DynamoDBHashKey
        private String id;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }
}
