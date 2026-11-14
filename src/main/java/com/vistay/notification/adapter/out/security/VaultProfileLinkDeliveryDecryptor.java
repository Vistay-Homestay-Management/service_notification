package com.vistay.notification.adapter.out.security;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

@Component
public class VaultProfileLinkDeliveryDecryptor {
    private static final String VAULT_TOKEN_HEADER = "X-Vault-Token";

    private final RestClient vault;
    private final String configuredToken;
    private final String tokenFile;
    private final String transitMount;
    private final String deliveryKey;

    public VaultProfileLinkDeliveryDecryptor(
        @Value("${vistay.identity-protection.vault.address:http://localhost:8200}") String address,
        @Value("${vistay.identity-protection.vault.token:}") String configuredToken,
        @Value("${vistay.identity-protection.vault.token-file:}") String tokenFile,
        @Value("${vistay.identity-protection.vault.transit-mount:transit}") String transitMount,
        @Value("${vistay.identity-protection.vault.delivery-key:vistay-profile-link-delivery}") String deliveryKey
    ) {
        this.vault = RestClient.builder().baseUrl(withoutTrailingSlash(address)).build();
        this.configuredToken = configuredToken == null ? "" : configuredToken.trim();
        this.tokenFile = tokenFile == null ? "" : tokenFile.trim();
        this.transitMount = pathSegment(transitMount);
        this.deliveryKey = pathSegment(deliveryKey);
    }

    public String decrypt(String ciphertext) {
        if (ciphertext == null || !ciphertext.matches("^vault:v[0-9]+:.+")) {
            throw new DeliverySecretUnavailable("Unsupported Vault ciphertext");
        }
        try {
            var response = vault.post()
                .uri("/v1/" + transitMount + "/decrypt/" + deliveryKey)
                .header(VAULT_TOKEN_HEADER, token())
                .header(HttpHeaders.CONTENT_TYPE, "application/json")
                .body(Map.of("ciphertext", ciphertext))
                .retrieve()
                .body(Map.class);
            if (response == null || !(response.get("data") instanceof Map<?, ?> data)) {
                throw new DeliverySecretUnavailable("Vault response omitted data");
            }
            var encoded = data.get("plaintext");
            if (!(encoded instanceof String value) || value.isBlank()) {
                throw new DeliverySecretUnavailable("Vault response omitted plaintext");
            }
            return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
        } catch (RestClientException | IllegalArgumentException unavailable) {
            throw new DeliverySecretUnavailable("Vault Transit delivery decryption failed", unavailable);
        }
    }

    private String token() {
        if (!tokenFile.isBlank()) {
            try {
                var value = Files.readString(Path.of(tokenFile), StandardCharsets.UTF_8).trim();
                if (!value.isBlank()) return value;
            } catch (IOException unreadable) {
                throw new DeliverySecretUnavailable("Vault token file is unavailable", unreadable);
            }
        }
        if (!configuredToken.isBlank()) return configuredToken;
        throw new DeliverySecretUnavailable("Vault authentication is not configured");
    }

    private static String pathSegment(String value) {
        var normalized = value == null ? "" : value.trim();
        if (!normalized.matches("^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$")) {
            throw new IllegalArgumentException("Invalid Vault path segment");
        }
        return normalized;
    }

    private static String withoutTrailingSlash(String value) {
        var normalized = value == null ? "" : value.trim();
        if (normalized.isBlank()) throw new IllegalArgumentException("Vault address is required");
        return normalized.endsWith("/") ? normalized.substring(0, normalized.length() - 1) : normalized;
    }

    public static final class DeliverySecretUnavailable extends RuntimeException {
        public DeliverySecretUnavailable(String message) { super(message); }
        public DeliverySecretUnavailable(String message, Throwable cause) { super(message, cause); }
    }
}
