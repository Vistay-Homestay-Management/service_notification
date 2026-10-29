package com.vistay.notification.application;

import com.vistay.contracts.common.v1.AccountRef;
import com.vistay.contracts.common.v1.AccountType;
import com.vistay.contracts.common.v1.DataScopeType;
import com.vistay.contracts.user.v1.GetAuthorizationSnapshotRequest;
import com.vistay.contracts.user.v1.UserAuthorizationServiceGrpc;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.grpc.client.GrpcChannelFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/** Reads the authoritative authorization snapshot owned by User Service. */
@Component
public class NotificationAuthorizationClient {
    private final UserAuthorizationServiceGrpc.UserAuthorizationServiceBlockingStub stub;
    private final Duration deadline;
    private final Duration cacheTtl;
    private final ObjectMapper json;
    private final Map<AccountKey, CachedSnapshot> cache = new ConcurrentHashMap<>();
    private final Map<AccountKey, Long> minimumVersions = new ConcurrentHashMap<>();

    public NotificationAuthorizationClient(
            GrpcChannelFactory channels,
            ObjectMapper json,
            @Value("${vistay.authorization.grpc-deadline:2s}") Duration deadline,
            @Value("${vistay.authorization.local-cache-ttl:30s}") Duration cacheTtl) {
        this.stub = UserAuthorizationServiceGrpc.newBlockingStub(channels.createChannel("user"));
        this.json = json;
        this.deadline = deadline;
        this.cacheTtl = cacheTtl;
    }

    public Snapshot require(AccountKind accountType, UUID accountId, String permission) {
        Snapshot snapshot = get(accountType, accountId);
        if (!snapshot.active() || snapshot.profileId() == null) {
            throw new AuthorizationException("PROFILE_INACTIVE", "ViStay profile is inactive");
        }
        if (snapshot.scopes(permission).isEmpty()) {
            throw new AuthorizationException("FORBIDDEN_PERMISSION", "Missing permission " + permission);
        }
        return snapshot;
    }

    public Snapshot get(AccountKind accountType, UUID accountId) {
        AccountKey key = new AccountKey(accountType, accountId);
        Instant now = Instant.now();
        long minimumVersion = minimumVersions.getOrDefault(key, 0L);
        CachedSnapshot cached = cache.get(key);
        if (cached != null && cached.expiresAt().isAfter(now)
                && cached.snapshot().authorizationVersion() >= minimumVersion) {
            return cached.snapshot();
        }

        try {
            var response = stub.withDeadlineAfter(deadline.toMillis(), TimeUnit.MILLISECONDS)
                    .getAuthorizationSnapshot(GetAuthorizationSnapshotRequest.newBuilder()
                            .setAccount(AccountRef.newBuilder()
                                    .setAccountId(accountId.toString())
                                    .setAccountType(accountType.contractValue())
                                    .build())
                            .setMinimumAuthorizationVersion(minimumVersion)
                            .build());
            Map<String, Set<Scope>> grants = new ConcurrentHashMap<>();
            response.getGrantsList().forEach(grant -> {
                Set<Scope> scopes = new LinkedHashSet<>();
                grant.getScopesList().stream()
                        .filter(value -> value != DataScopeType.DATA_SCOPE_TYPE_UNSPECIFIED)
                        .forEach(value -> scopes.add(Scope.valueOf(
                                value.name().substring("DATA_SCOPE_TYPE_".length()))));
                if (grant.getPartitionsCount() > 0) {
                    scopes.add(Scope.PARTITION);
                }
                grants.put(grant.getPermissionCode(), Set.copyOf(scopes));
            });
            Snapshot snapshot = new Snapshot(response.getActive(),
                    response.hasProfileId() ? UUID.fromString(response.getProfileId()) : null,
                    response.getAuthorizationVersion(), Map.copyOf(grants));
            cache.put(key, new CachedSnapshot(snapshot, now.plus(cacheTtl)));
            return snapshot;
        } catch (StatusRuntimeException unavailable) {
            throw new AuthorizationException("AUTHORIZATION_UNAVAILABLE",
                    "Authorization service is unavailable");
        } catch (IllegalArgumentException invalidSnapshot) {
            throw new AuthorizationException("AUTHORIZATION_INVALID",
                    "Authorization service returned an invalid snapshot");
        }
    }

    @KafkaListener(
            topics = "${vistay.topics.authorization-changed:vistay.user.authorization-changed.v1}",
            groupId = "${spring.application.name}-authorization-cache")
    public void authorizationChanged(String rawEnvelope) {
        try {
            var data = json.readTree(rawEnvelope).get("data");
            if (data == null) {
                return;
            }
            AccountKind accountType = AccountKind.valueOf(data.get("accountType").asText());
            UUID accountId = UUID.fromString(data.get("accountId").asText());
            AccountKey key = new AccountKey(accountType, accountId);
            minimumVersions.merge(key, data.get("authorizationVersion").asLong(), Math::max);
            cache.remove(key);
        } catch (Exception invalidEnvelope) {
            throw new IllegalArgumentException("Invalid authorization-changed event", invalidEnvelope);
        }
    }

    public enum AccountKind {
        CUSTOMER(AccountType.ACCOUNT_TYPE_CUSTOMER),
        ADMIN(AccountType.ACCOUNT_TYPE_ADMIN),
        HOST(AccountType.ACCOUNT_TYPE_HOST);

        private final AccountType contractValue;

        AccountKind(AccountType contractValue) {
            this.contractValue = contractValue;
        }

        AccountType contractValue() {
            return contractValue;
        }
    }

    public enum Scope { SELF, OWN, ASSIGNED, PARTITION, ALL }

    public record Snapshot(
            boolean active,
            UUID profileId,
            long authorizationVersion,
            Map<String, Set<Scope>> grants) {
        public Set<Scope> scopes(String permission) {
            return grants.getOrDefault(permission, Set.of());
        }
    }

    public static final class AuthorizationException extends RuntimeException {
        private final String code;

        public AuthorizationException(String code, String message) {
            super(message);
            this.code = code;
        }

        public String code() {
            return code;
        }
    }

    private record AccountKey(AccountKind accountType, UUID accountId) {}
    private record CachedSnapshot(Snapshot snapshot, Instant expiresAt) {}
}
