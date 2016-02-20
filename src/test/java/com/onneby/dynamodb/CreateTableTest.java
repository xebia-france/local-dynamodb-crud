package com.onneby.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by ponneby on 20/02/16.
 */
public class CreateTableTest {
    @Before
    public void startAnInMemoryServer() throws Exception {
        final String[] localArgs = {"-inMemory", "-port", "30000"};

        final DynamoDBProxyServer server = ServerRunner.createServerFromCommandLineArgs(localArgs);
        server.start();


    }

    @Test
    public void createTable() throws Exception {

        DynamoDBMapperConfig config = new DynamoDBMapperConfig(DynamoDBMapperConfig.DEFAULT, new DynamoDBMapperConfig(ConversionSchemas.V2));
        AmazonDynamoDBClient dynamoDbClient = new AmazonDynamoDBClient();
        final DynamoDBMapper dynamoDBMapper = new DynamoDBMapper(dynamoDbClient, config);
        CreateTableRequest req = dynamoDBMapper.generateCreateTableRequest(User.class);

        DynamoDB dynamoDB = new DynamoDB(dynamoDbClient);
        Table table = dynamoDB.createTable(req);
        table.waitForActive();

    }

    @DynamoDBTable(tableName = "USER")
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
