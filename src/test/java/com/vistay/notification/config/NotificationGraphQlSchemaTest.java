package com.vistay.notification.config;

import graphql.schema.idl.SchemaParser;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class NotificationGraphQlSchemaTest {
    @Test
    void managementSchemaParsesWithTheRuntimeGraphqlParser() throws IOException {
        var resource = new ClassPathResource("graphql/schema.graphqls");
        try (var reader = new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8)) {
            new SchemaParser().parse(reader);
        }
    }
}
