package com.vistay.notification.application;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

/** Owns immutable template revisions and their publication audit trail. */
@Service
public class NotificationTemplateService {
    private static final Pattern KEY = Pattern.compile("^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$");
    private static final Pattern LOCALE = Pattern.compile("^[a-z]{2}(?:-[A-Z]{2})?$");
    private static final Pattern VARIABLE = Pattern.compile("^[A-Za-z][A-Za-z0-9_.-]{0,79}$");
    private static final Pattern PLACEHOLDER = Pattern.compile("\\{\\{([A-Za-z][A-Za-z0-9_.-]{0,79})\\}\\}");

    private final JdbcClient jdbc;
    private final ObjectMapper json;

    public NotificationTemplateService(JdbcClient jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    @Transactional
    public Template publish(PublishCommand command) {
        validate(command);
        String fingerprint = fingerprint(command);
        IdempotencyRow previous = findIdempotency("template.publish", command.idempotencyKey());
        if (previous != null) {
            ensureSameRequest(previous, fingerprint);
            return loadById(resultId(previous.resultJson(), "templateId"));
        }

        TemplateHeader header = findHeader(command.templateKey(), command.channel(), command.locale());
        UUID templateId = header == null ? createHeader(command) : header.id();
        header = findHeaderById(templateId);
        int currentRevision = header.currentRevision() == null ? 0 : header.currentRevision();
        if (command.expectedVersion() != null && command.expectedVersion() != currentRevision) {
            throw new NotificationCommandException("STALE_REVISION",
                    "Template revision does not match the expected version");
        }
        int revision = currentRevision + 1;
        OffsetDateTime now = OffsetDateTime.now();

        jdbc.sql("""
                UPDATE notification_template_revisions
                SET status = 'RETIRED', retired_at = COALESCE(retired_at, now()), immutable = true
                WHERE template_id = :templateId AND status = 'PUBLISHED'
                """)
            .param("templateId", templateId)
            .update();

        jdbc.sql("""
                INSERT INTO notification_template_revisions(
                    template_id, version_number, subject_template, body_template,
                    template_variables, required_variables, variables_schema,
                    policy_revision, status, published_at, immutable, revision_fingerprint
                ) VALUES (
                    :templateId, :revision, :subject, :body,
                    CAST(:variables AS jsonb), CAST(:requiredVariables AS jsonb),
                    CAST(:variablesSchema AS jsonb), :policyRevision, 'PUBLISHED',
                    :publishedAt, true, :fingerprint
                )
                """)
            .param("templateId", templateId)
            .param("revision", revision)
            .param("subject", command.subject())
            .param("body", command.body())
            .param("variables", jsonArray(command.requiredVariables()))
            .param("requiredVariables", jsonArray(command.requiredVariables()))
            .param("variablesSchema", variablesSchema(command.requiredVariables()))
            .param("policyRevision", command.policyRevision())
            .param("publishedAt", now)
            .param("fingerprint", fingerprint)
            .update();

        jdbc.sql("""
                UPDATE notification_templates
                SET status = 'PUBLISHED', is_active = true, current_revision = :revision,
                    policy_revision = :policyRevision, published_at = :publishedAt,
                    retired_at = NULL, updated_at = now()
                WHERE id = :templateId
                """)
            .param("templateId", templateId)
            .param("revision", revision)
            .param("policyRevision", command.policyRevision())
            .param("publishedAt", now)
            .update();

        Template result = loadById(templateId);
        writeAudit("NOTIFICATION_TEMPLATE_PUBLISHED", templateId,
            command.actorAccountId(), command.idempotencyKey(), result);
        writeOutbox("vistay.notification.template-published", templateId, result,
            command.idempotencyKey());
        saveIdempotency("template.publish", command.idempotencyKey(), command.actorAccountType(),
            command.actorAccountId(), fingerprint, resultId(result, "templateId"));
        return result;
    }

    @Transactional
    public Template saveDraft(PublishCommand command) {
        validate(command);
        String fingerprint = fingerprint(command);
        IdempotencyRow previous = findIdempotency("template.draft", command.idempotencyKey());
        if (previous != null) {
            ensureSameRequest(previous, fingerprint);
            return loadById(resultId(previous.resultJson(), "templateId"));
        }
        TemplateHeader header = findHeader(command.templateKey(), command.channel(), command.locale());
        UUID templateId = header == null ? createHeader(command) : header.id();
        header = findHeaderById(templateId);
        int currentRevision = header.currentRevision() == null ? 0 : header.currentRevision();
        if (command.expectedVersion() != null && command.expectedVersion() != currentRevision) {
            throw new NotificationCommandException("STALE_REVISION",
                    "Template revision does not match the expected version");
        }
        int revision = currentRevision + 1;
        jdbc.sql("""
                INSERT INTO notification_template_revisions(
                    template_id, version_number, subject_template, body_template,
                    template_variables, required_variables, variables_schema,
                    policy_revision, status, immutable, revision_fingerprint
                ) VALUES (
                    :templateId, :revision, :subject, :body,
                    CAST(:variables AS jsonb), CAST(:requiredVariables AS jsonb),
                    CAST(:variablesSchema AS jsonb), :policyRevision, 'DRAFT', false, :fingerprint
                )
                """)
            .param("templateId", templateId)
            .param("revision", revision)
            .param("subject", command.subject())
            .param("body", command.body())
            .param("variables", jsonArray(command.requiredVariables()))
            .param("requiredVariables", jsonArray(command.requiredVariables()))
            .param("variablesSchema", variablesSchema(command.requiredVariables()))
            .param("policyRevision", command.policyRevision())
            .param("fingerprint", fingerprint)
            .update();
        jdbc.sql("""
                UPDATE notification_templates
                SET status = 'DRAFT', is_active = false, current_revision = :revision,
                    policy_revision = :policyRevision, updated_at = now()
                WHERE id = :templateId
                """)
            .param("templateId", templateId)
            .param("revision", revision)
            .param("policyRevision", command.policyRevision())
            .update();
        Template result = loadById(templateId);
        writeAudit("NOTIFICATION_TEMPLATE_DRAFT_SAVED", templateId,
            command.actorAccountId(), command.idempotencyKey(), result);
        writeOutbox("vistay.notification.template-draft-saved", templateId, result,
            command.idempotencyKey());
        saveIdempotency("template.draft", command.idempotencyKey(), command.actorAccountType(),
            command.actorAccountId(), fingerprint, resultId(result, "templateId"));
        return result;
    }

    @Transactional(readOnly = true)
    public TemplatePreview preview(PreviewCommand command) {
        if (command == null || blank(command.templateKey()) || blank(command.channel())
                || blank(command.locale())) {
            throw new NotificationCommandException("VALIDATION_ERROR", "Template preview input is invalid");
        }
        Template template = list(new TemplateQuery(
            command.templateKey(), command.channel(), null, 1)).stream().findFirst()
            .orElseThrow(() -> new NotificationCommandException("NOT_FOUND", "Notification template was not found"));
        Map<String, String> variables = command.variables() == null ? Map.of() : command.variables();
        for (String required : template.requiredVariables()) {
            if (!variables.containsKey(required) || variables.get(required) == null) {
                throw new NotificationCommandException("MISSING_TEMPLATE_VARIABLE",
                    "A required template variable is missing");
            }
        }
        return new TemplatePreview(template.templateKey(), template.channel(), template.locale(),
            template.revision(), render(template.subject(), variables), render(template.body(), variables));
    }

    @Transactional
    public Template retire(RetireCommand command) {
        if (command.templateId() == null || command.expectedVersion() < 1
                || blank(command.reason()) || blank(command.idempotencyKey())) {
            throw new NotificationCommandException("VALIDATION_ERROR", "Template retirement input is invalid");
        }
        String fingerprint = fingerprint(command.templateId().toString(),
            Integer.toString(command.expectedVersion()), command.reason());
        IdempotencyRow previous = findIdempotency("template.retire", command.idempotencyKey());
        if (previous != null) {
            ensureSameRequest(previous, fingerprint);
            return loadById(resultId(previous.resultJson(), "templateId"));
        }
        Template result = loadById(command.templateId());
        if (result.revision() != command.expectedVersion()) {
            throw new NotificationCommandException("STALE_REVISION",
                    "Template revision does not match the expected version");
        }
        jdbc.sql("""
                UPDATE notification_template_revisions
                SET status = 'RETIRED', retired_at = now(), immutable = true
                WHERE template_id = :templateId AND version_number = :revision
                """)
            .param("templateId", command.templateId())
            .param("revision", command.expectedVersion())
            .update();
        jdbc.sql("""
                UPDATE notification_templates
                SET status = 'RETIRED', is_active = false, retired_at = now(), updated_at = now()
                WHERE id = :templateId
                """)
            .param("templateId", command.templateId())
            .update();
        result = loadById(command.templateId());
        writeAudit("NOTIFICATION_TEMPLATE_RETIRED", command.templateId(),
            command.actorAccountId(), command.idempotencyKey(), result);
        writeOutbox("vistay.notification.template-retired", command.templateId(), result,
            command.idempotencyKey());
        saveIdempotency("template.retire", command.idempotencyKey(), command.actorAccountType(),
            command.actorAccountId(), fingerprint, resultId(result, "templateId"));
        return result;
    }

    @Transactional(readOnly = true)
    public List<Template> list(TemplateQuery query) {
        StringBuilder sql = new StringBuilder("""
            SELECT t.id, t.code AS template_key, t.channel, t.language_code AS locale,
                   r.status, r.version_number AS revision, r.policy_revision,
                   r.required_variables::text AS required_variables_json,
                   r.subject_template AS subject, r.body_template AS body,
                   r.immutable, r.created_at, r.published_at, r.retired_at
            FROM notification_templates t
            JOIN notification_template_revisions r ON r.template_id = t.id
                AND r.version_number = COALESCE(t.current_revision, r.version_number)
            WHERE 1 = 1
            """);
        if (!blank(query.templateKey())) {
            sql.append(" AND t.code = :templateKey");
        }
        if (!blank(query.channel())) {
            sql.append(" AND t.channel = :channel");
        }
        if (!blank(query.status())) {
            sql.append(" AND r.status = :status");
        }
        sql.append(" ORDER BY r.version_number DESC LIMIT :limit");
        var statement = jdbc.sql(sql.toString())
            .param("limit", Math.max(1, Math.min(query.limit(), 100)));
        if (!blank(query.templateKey())) {
            statement = statement.param("templateKey", query.templateKey());
        }
        if (!blank(query.channel())) {
            statement = statement.param("channel", query.channel());
        }
        if (!blank(query.status())) {
            statement = statement.param("status", query.status());
        }
        return statement.query(TemplateRow.class).list().stream().map(this::map).toList();
    }

    @Transactional(readOnly = true)
    public Optional<Template> findPublished(String templateKey, String channel, String locale) {
        return jdbc.sql("""
                SELECT t.id, t.code AS template_key, t.channel, t.language_code AS locale,
                       r.status, r.version_number AS revision, r.policy_revision,
                       r.required_variables::text AS required_variables_json,
                       r.subject_template AS subject, r.body_template AS body,
                       r.immutable, r.created_at, r.published_at, r.retired_at
                FROM notification_templates t
                JOIN notification_template_revisions r ON r.template_id = t.id
                WHERE t.code = :templateKey AND t.channel = :channel
                  AND t.language_code = :locale AND r.status = 'PUBLISHED'
                ORDER BY r.version_number DESC
                LIMIT 1
                """)
            .param("templateKey", templateKey)
            .param("channel", channel)
            .param("locale", locale)
            .query(TemplateRow.class)
            .list()
            .stream()
            .findFirst()
            .map(this::map);
    }

    @Transactional(readOnly = true)
    public Template loadById(UUID templateId) {
        TemplateRow row = jdbc.sql("""
                SELECT t.id, t.code AS template_key, t.channel, t.language_code AS locale,
                       r.status, r.version_number AS revision, r.policy_revision,
                       r.required_variables::text AS required_variables_json,
                       r.subject_template AS subject, r.body_template AS body,
                       r.immutable, r.created_at, r.published_at, r.retired_at
                FROM notification_templates t
                JOIN notification_template_revisions r ON r.template_id = t.id
                    AND r.version_number = t.current_revision
                WHERE t.id = :templateId
                """)
            .param("templateId", templateId)
            .query(TemplateRow.class)
            .list()
            .stream()
            .findFirst()
            .orElseThrow(() -> new NotificationCommandException("NOT_FOUND", "Notification template was not found"));
        return map(row);
    }

    private UUID createHeader(PublishCommand command) {
        return jdbc.sql("""
                INSERT INTO notification_templates(
                    code, channel, language_code, is_active, status, policy_revision
                ) VALUES (:templateKey, :channel, :locale, false, 'DRAFT', :policyRevision)
                ON CONFLICT (code, channel, language_code) DO NOTHING
                RETURNING id
                """)
            .param("templateKey", command.templateKey())
            .param("channel", command.channel())
            .param("locale", command.locale())
            .param("policyRevision", command.policyRevision())
            .query(UUID.class)
            .list()
            .stream()
            .findFirst()
            .orElseGet(() -> findHeader(command.templateKey(), command.channel(), command.locale()).id());
    }

    private TemplateHeader findHeader(String templateKey, String channel, String locale) {
        return jdbc.sql("""
                SELECT id, current_revision, policy_revision, status
                FROM notification_templates
                WHERE code = :templateKey AND channel = :channel AND language_code = :locale
                FOR UPDATE
                """)
            .param("templateKey", templateKey)
            .param("channel", channel)
            .param("locale", locale)
            .query(TemplateHeader.class)
            .list()
            .stream()
            .findFirst()
            .orElse(null);
    }

    private TemplateHeader findHeaderById(UUID templateId) {
        return jdbc.sql("""
                SELECT id, current_revision, policy_revision, status
                FROM notification_templates WHERE id = :templateId FOR UPDATE
                """)
            .param("templateId", templateId)
            .query(TemplateHeader.class)
            .single();
    }

    private IdempotencyRow findIdempotency(String operation, String key) {
        return jdbc.sql("""
                SELECT request_fingerprint, result
                FROM notification_command_idempotency
                WHERE operation = :operation AND idempotency_key = :idempotencyKey
                """)
            .param("operation", operation)
            .param("idempotencyKey", key)
            .query(IdempotencyRow.class)
            .list()
            .stream()
            .findFirst()
            .orElse(null);
    }

    private void saveIdempotency(
            String operation,
            String key,
            String actorType,
            UUID actorId,
            String fingerprint,
            String result) {
        jdbc.sql("""
                INSERT INTO notification_command_idempotency(
                    operation, idempotency_key, actor_account_type, actor_user_id,
                    request_fingerprint, result
                ) VALUES (:operation, :idempotencyKey, :actorType, :actorId, :fingerprint,
                    CAST(:result AS jsonb))
                ON CONFLICT (operation, idempotency_key) DO NOTHING
                """)
            .param("operation", operation)
            .param("idempotencyKey", key)
            .param("actorType", actorType)
            .param("actorId", actorId)
            .param("fingerprint", fingerprint)
            .param("result", result)
            .update();
    }

    private void writeAudit(String action, UUID templateId, UUID actorId, String idempotencyKey,
            Template result) {
        String changes = jsonObject(Map.of(
            "templateId", templateId,
            "revision", result.revision(),
            "policyRevision", result.policyRevision(),
            "idempotencyKey", idempotencyKey));
        jdbc.sql("""
                INSERT INTO audit_events(
                    actor_admin_user_id, action, aggregate_type, aggregate_id, changes
                ) VALUES (:actorId, :action, 'NotificationTemplate', :templateId,
                    CAST(:changes AS jsonb))
                """)
            .param("actorId", actorId)
            .param("action", action)
            .param("templateId", templateId)
            .param("changes", changes)
            .update();
    }

    private void writeOutbox(String eventType, UUID templateId, Template result, String causationId) {
        String payload = jsonObject(Map.of(
            "templateId", templateId,
            "templateKey", result.templateKey(),
            "channel", result.channel(),
            "locale", result.locale(),
            "status", result.status(),
            "revision", result.revision(),
            "policyRevision", result.policyRevision()));
        jdbc.sql("""
                INSERT INTO outbox_events(
                    aggregate_type, aggregate_id, aggregate_version, event_type,
                    event_version, payload, causation_id
                ) VALUES ('NotificationTemplate', :templateId, :revision, :eventType,
                    1, CAST(:payload AS jsonb), :causationId)
                """)
            .param("templateId", templateId)
            .param("revision", result.revision())
            .param("eventType", eventType)
            .param("payload", payload)
            .param("causationId", causationId)
            .update();
    }

    private void validate(PublishCommand command) {
        if (command == null || blank(command.templateKey()) || !KEY.matcher(command.templateKey()).matches()
                || blank(command.channel()) || !List.of("IN_APP", "EMAIL", "PUSH").contains(command.channel())
                || blank(command.locale()) || !LOCALE.matcher(command.locale()).matches()
                || blank(command.body()) || command.body().length() > 20_000
                || command.policyRevision() < 1 || blank(command.idempotencyKey())) {
            throw new NotificationCommandException("VALIDATION_ERROR", "Notification template input is invalid");
        }
        List<String> variables = command.requiredVariables() == null ? List.of() : command.requiredVariables();
        if (variables.stream().anyMatch(value -> value == null || !VARIABLE.matcher(value).matches())
                || variables.stream().distinct().count() != variables.size()) {
            throw new NotificationCommandException("INVALID_TEMPLATE_VARIABLES",
                    "Notification template variables are invalid");
        }
        Set<String> placeholders = new HashSet<>();
        var matcher = PLACEHOLDER.matcher(command.body());
        while (matcher.find()) {
            placeholders.add(matcher.group(1));
        }
        if (!variables.containsAll(placeholders)) {
            throw new NotificationCommandException("INVALID_TEMPLATE_VARIABLES",
                "Template body contains an undeclared variable");
        }
    }

    private String jsonArray(List<String> values) {
        try {
            return json.writeValueAsString(values == null ? List.of() : values);
        } catch (Exception serializationFailure) {
            throw new NotificationCommandException("SCHEMA_MISMATCH", "Template variables cannot be serialized");
        }
    }

    private static String render(String source, Map<String, String> variables) {
        if (source == null) {
            return null;
        }
        String rendered = source;
        for (Map.Entry<String, String> entry : variables.entrySet()) {
            rendered = rendered.replace("{{" + entry.getKey() + "}}",
                entry.getValue() == null ? "" : entry.getValue());
        }
        return rendered;
    }

    private String variablesSchema(List<String> values) {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        schema.put("required", values == null ? List.of() : values);
        return jsonObject(schema);
    }

    private String jsonObject(Object value) {
        try {
            return json.writeValueAsString(value);
        } catch (Exception serializationFailure) {
            throw new NotificationCommandException("SCHEMA_MISMATCH", "Notification metadata cannot be serialized");
        }
    }

    private String resultId(Template result, String ignored) {
        return jsonObject(Map.of("templateId", result.id()));
    }

    private UUID resultId(String result, String field) {
        try {
            return UUID.fromString(json.readTree(result).get(field).asText());
        } catch (Exception invalidResult) {
            throw new NotificationCommandException("IDEMPOTENCY_RESULT_INVALID",
                    "Stored notification command result is invalid");
        }
    }

    private void ensureSameRequest(IdempotencyRow previous, String fingerprint) {
        if (!fingerprint.equals(previous.requestFingerprint())) {
            throw new NotificationCommandException("IDEMPOTENCY_KEY_REUSED",
                    "Idempotency key was already used for another request");
        }
    }

    private static String fingerprint(PublishCommand command) {
        return DomainEventNotificationService.fingerprint(
            command.templateKey(), command.channel(), command.locale(), command.subject(),
            command.body(), String.valueOf(command.requiredVariables()),
            Integer.toString(command.policyRevision()), String.valueOf(command.expectedVersion()));
    }

    private static String fingerprint(String... values) {
        return DomainEventNotificationService.fingerprint(values);
    }

    private Template map(TemplateRow row) {
        List<String> variables = new ArrayList<>();
        try {
            var node = json.readTree(row.requiredVariablesJson() == null ? "[]" : row.requiredVariablesJson());
            node.forEach(value -> variables.add(value.asText()));
        } catch (Exception invalidVariables) {
            throw new NotificationCommandException("SCHEMA_MISMATCH", "Stored template variables are invalid");
        }
        return new Template(row.id(), row.templateKey(), row.channel(), row.locale(), row.status(),
            row.revision(), row.policyRevision(), List.copyOf(variables), row.subject(), row.body(),
            row.immutable(), row.createdAt(), row.publishedAt(), row.retiredAt());
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    public record PublishCommand(
            String templateKey,
            String channel,
            String locale,
            String subject,
            String body,
            List<String> requiredVariables,
            int policyRevision,
            Integer expectedVersion,
            String idempotencyKey,
            String actorAccountType,
            UUID actorAccountId) {}

    public record RetireCommand(
            UUID templateId,
            int expectedVersion,
            String reason,
            String idempotencyKey,
            String actorAccountType,
            UUID actorAccountId) {}

    public record PreviewCommand(
            String templateKey,
            String channel,
            String locale,
            Map<String, String> variables) {}

    public record TemplatePreview(
            String templateKey,
            String channel,
            String locale,
            int revision,
            String subject,
            String body) {}

    public record TemplateQuery(String templateKey, String channel, String status, int limit) {
        public TemplateQuery {
            if (limit < 1) {
                limit = 25;
            }
        }
    }

    public record Template(
            UUID id,
            String templateKey,
            String channel,
            String locale,
            String status,
            int revision,
            int policyRevision,
            List<String> requiredVariables,
            String subject,
            String body,
            boolean immutable,
            OffsetDateTime createdAt,
            OffsetDateTime publishedAt,
            OffsetDateTime retiredAt) {}

    private record TemplateHeader(UUID id, Integer currentRevision, int policyRevision, String status) {}

    private record TemplateRow(
            UUID id,
            String templateKey,
            String channel,
            String locale,
            String status,
            int revision,
            int policyRevision,
            String requiredVariablesJson,
            String subject,
            String body,
            boolean immutable,
            OffsetDateTime createdAt,
            OffsetDateTime publishedAt,
            OffsetDateTime retiredAt) {}

    private record IdempotencyRow(String requestFingerprint, String resultJson) {}
}
