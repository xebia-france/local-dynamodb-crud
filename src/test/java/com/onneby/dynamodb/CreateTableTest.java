package com.onneby.dynamodb;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import org.junit.Test;

/**
 * Created by ponneby on 20/02/16.
 */
public class CreateTableTest {
    @Test
    public void startAnInMemoryServer() throws Exception {
        final String[] localArgs = {"-inMemory", "-port", "30000"};

        final DynamoDBProxyServer server = ServerRunner.createServerFromCommandLineArgs(localArgs);
        server.start();


    }

    
}
