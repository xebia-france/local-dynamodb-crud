package com.onneby.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by ponneby on 20/02/16.
 */
public class CreateTableTest {
    public static final String ID = "someUniqueId";
    private static DynamoDBMapper dynamoDBMapper;
    private static DynamoDB dynamoDB;

    @BeforeClass
    public static void startAnInMemoryServer() throws Exception {
        final String[] localArgs = {"-inMemory", "-port", "30000", "-sharedDb"};

        final DynamoDBProxyServer server = ServerRunner.createServerFromCommandLineArgs(localArgs);
        server.start();
        AmazonDynamoDBClient dynamoDbClient = new AmazonDynamoDBClient();
        dynamoDbClient.setEndpoint("http://localhost:30000");
        DynamoDBMapperConfig config = new DynamoDBMapperConfig(DynamoDBMapperConfig.DEFAULT, new DynamoDBMapperConfig(ConversionSchemas.V2));
        dynamoDBMapper = new DynamoDBMapper(dynamoDbClient, config);
        dynamoDB = new DynamoDB(dynamoDbClient);
    }

    @Before
    public void createUserTable() throws Exception {

        CreateTableRequest req = dynamoDBMapper.generateCreateTableRequest(User.class);
        req.setProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));

        Table newTable = dynamoDB.createTable(req);
        newTable.waitForActive();

        saveUser();

    }

    private void saveUser() throws Exception {
        User user = new User();
        user.setId(ID);
        dynamoDBMapper.save(user);
    }

    @Test
    public void loadUser() throws Exception {
        User user = dynamoDBMapper.load(User.class, ID);
        assertThat(user.getId()).isEqualTo(ID);
    }

}
