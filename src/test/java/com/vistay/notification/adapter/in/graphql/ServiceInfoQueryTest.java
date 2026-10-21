package com.vistay.notification.adapter.in.graphql;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ServiceInfoQueryTest {

    private final ServiceInfoQuery query = new ServiceInfoQuery();

    @Test
    void exposesStableServiceIdentity() {
        var info = query.notificationServiceInfo();

        assertThat(info.name()).isEqualTo("notification");
        assertThat(info.version()).isEqualTo("0.1.0");
    }
}

