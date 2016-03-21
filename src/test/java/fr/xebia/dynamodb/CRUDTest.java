package fr.xebia.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.*;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by ponneby on 20/02/16.
 */
public class CRUDTest {
    public static final String ID = "someUniqueId";
    public static final String EMAIL_ADDRESS = "test@example.com";
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

        createUserTable();
    }

    private static void createUserTable() throws Exception {

        CreateTableRequest req = dynamoDBMapper.generateCreateTableRequest(User.class);
        req.setProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
        req.getGlobalSecondaryIndexes().forEach(gsi -> {
            gsi.setProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
            gsi.setProjection(new Projection().withProjectionType(ProjectionType.ALL));
        });

        Table newTable = dynamoDB.createTable(req);
        newTable.waitForActive();


    }


    private void saveUser() throws Exception {
        User user = new User();
        user.setId(ID);
        user.setEmail(EMAIL_ADDRESS);
        dynamoDBMapper.save(user);
    }

    @Test
    public void loadUser() throws Exception {
        saveUser();

        User user = dynamoDBMapper.load(User.class, ID);
        assertThat(user.getId()).isEqualTo(ID);
    }

    @Test
    public void queryByEmail() throws Exception {

        saveUser();

        String emailIndex = "email_index";
        DynamoDBQueryExpression<User> queryExpression = new DynamoDBQueryExpression<User>()
                .withIndexName(emailIndex)
                .withConsistentRead(false)
                .withKeyConditionExpression("email" + " = :email")
                .withExpressionAttributeValues(new HashMap<String, AttributeValue>() {
                    {
                        put(":email", new AttributeValue(EMAIL_ADDRESS));
                    }
                });
        PaginatedQueryList<User> result = dynamoDBMapper.query(User.class, queryExpression);
        Assertions.assertThat(result).isNotEmpty();

    }
}
