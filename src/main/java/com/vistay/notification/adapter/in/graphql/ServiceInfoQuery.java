package com.vistay.notification.adapter.in.graphql;

import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

@Controller
public class ServiceInfoQuery {

    @QueryMapping
    public ServiceInfo notificationServiceInfo() {
        return new ServiceInfo("notification", "0.1.0");
    }

    public record ServiceInfo(String name, String version) {
    }
}

