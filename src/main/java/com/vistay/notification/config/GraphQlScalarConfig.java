package com.vistay.notification.config;

import graphql.Scalars;
import graphql.schema.GraphQLScalarType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.graphql.execution.RuntimeWiringConfigurer;

@Configuration
public class GraphQlScalarConfig {
    @Bean
    RuntimeWiringConfigurer vistayScalars() {
        var cursor = GraphQLScalarType.newScalar(Scalars.GraphQLString)
            .name("Cursor")
            .description("Opaque non-negative decimal notification cursor")
            .build();
        var dateTime = GraphQLScalarType.newScalar(Scalars.GraphQLString)
            .name("DateTime")
            .description("RFC 3339 timestamp with offset")
            .build();
        return builder -> builder.scalar(cursor).scalar(dateTime);
    }
}
