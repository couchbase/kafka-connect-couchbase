package com.amazonaws.kafka.config.providers;

import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockedSecretsManagerConfigProvider extends SecretsManagerConfigProvider {
    @Override
    protected SecretsManagerClient checkOrInitSecretManagerClient() {
        SecretsManagerClient secretsClient = mock(SecretsManagerClient.class);
        when(secretsClient.getSecretValue(request("AmazonMSK_TestKafkaConfig"))).thenAnswer(
                (Answer<GetSecretValueResponse>) invocation -> response("{\"username\": \"John\", \"password\":\"Password123\"}")
        );
        when(secretsClient.getSecretValue(request("arn:aws:secretsmanager:ap-southeast-2:123456789:secret:AmazonMSK_my_service/my_secret"))).thenAnswer(
                (Answer<GetSecretValueResponse>) invocation -> response("{\"username\": \"John2\", \"password\":\"Password567\"}")
        );
        when(secretsClient.getSecretValue(request("arn:aws:secretsmanager:ap-southeast-2:123456789:secret:AmazonMSK_my_service/my_secret%3A"))).thenAnswer(
                (Answer<GetSecretValueResponse>) invocation -> response("{\"username\": \"John3\", \"password\":\"Password321\"}")
        );
        when(secretsClient.getSecretValue(request("AmazonMSK_TestTTL"))).thenAnswer(
                (Answer<GetSecretValueResponse>) invocation -> response("{\"username\": \"John\", \"password\":\"Password123\"}")
        );
        when(secretsClient.getSecretValue(request("notFound"))).thenThrow(ResourceNotFoundException.class);
        return secretsClient;
    }

    private GetSecretValueRequest request(String path) {
        return GetSecretValueRequest.builder().secretId(path).build();
    }

    private GetSecretValueResponse response(String value) {
        return GetSecretValueResponse.builder().secretString(value).build();
    }
}
