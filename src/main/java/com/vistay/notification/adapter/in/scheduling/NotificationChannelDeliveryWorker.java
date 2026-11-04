package com.vistay.notification.adapter.in.scheduling;

import com.vistay.notification.application.NotificationDeliveryRetryService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Optional polling worker; production enables it only with an approved provider adapter. */
@Component
@ConditionalOnProperty(
    name = "vistay.notifications.delivery-worker-enabled",
    havingValue = "true",
    matchIfMissing = false)
public class NotificationChannelDeliveryWorker {
    private final NotificationDeliveryRetryService deliveries;

    public NotificationChannelDeliveryWorker(NotificationDeliveryRetryService deliveries) {
        this.deliveries = deliveries;
    }

    @Scheduled(fixedDelayString = "${vistay.notifications.delivery-poll-delay-ms:1000}")
    public void poll() {
        deliveries.processPending(100);
    }
}
