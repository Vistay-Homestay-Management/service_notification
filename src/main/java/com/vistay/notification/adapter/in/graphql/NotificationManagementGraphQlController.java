package com.vistay.notification.adapter.in.graphql;

import com.vistay.notification.application.NotificationAuthorizationService;
import com.vistay.notification.application.NotificationPreferenceService;
import com.vistay.notification.application.NotificationTemplateService;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Controller;

/** GraphQL boundary for template publication and recipient-self preferences. */
@Controller
public class NotificationManagementGraphQlController {
    private final NotificationAuthorizationService authorization;
    private final NotificationPreferenceService preferences;
    private final NotificationTemplateService templates;

    public NotificationManagementGraphQlController(
            NotificationAuthorizationService authorization,
            NotificationPreferenceService preferences,
            NotificationTemplateService templates) {
        this.authorization = authorization;
        this.preferences = preferences;
        this.templates = templates;
    }

    @QueryMapping
    public PreferenceConnection notificationPreferences(@AuthenticationPrincipal Jwt jwt) {
        var recipient = authorization.requireOwnPreferences(jwt, false);
        return map(preferences.get(recipient.accountType(), recipient.accountId()));
    }

    @QueryMapping
    public TemplateConnection notificationTemplates(
            @Argument Map<String, Object> input,
            @AuthenticationPrincipal Jwt jwt) {
        authorization.requireOperator(jwt, "notifications:template:read");
        List<TemplateView> nodes = templates.list(new NotificationTemplateService.TemplateQuery(
            string(input, "templateKey"), string(input, "channel"), string(input, "status"),
            integer(input, "limit", 25))).stream().map(this::map).toList();
        return new TemplateConnection(nodes,
            new NotificationPageInfo(false, null, null, "ACTIVE", null, null));
    }

    @MutationMapping
    public PreferenceConnection updateNotificationPreferences(
            @Argument Map<String, Object> input,
            @AuthenticationPrincipal Jwt jwt) {
        var recipient = authorization.requireOwnPreferences(jwt, true);
        List<NotificationPreferenceService.PreferenceUpdate> updates = list(input, "updates").stream()
            .map(NotificationManagementGraphQlController::preferenceUpdate)
            .toList();
        return map(preferences.update(new NotificationPreferenceService.UpdateCommand(
            recipient.accountType(), recipient.accountId(), updates,
            requiredString(input, "idempotencyKey"), recipient.accountType(), recipient.accountId())));
    }

    @MutationMapping
    public TemplateView saveNotificationTemplateDraft(
            @Argument Map<String, Object> input,
            @AuthenticationPrincipal Jwt jwt) {
        var actor = authorization.requireOperator(jwt, "notifications:template:manage");
        return map(templates.saveDraft(templateCommand(input, actor)));
    }

    @MutationMapping
    public TemplatePreviewView previewNotificationTemplate(
            @Argument Map<String, Object> input,
            @AuthenticationPrincipal Jwt jwt) {
        authorization.requireOperator(jwt, "notifications:template:read");
        Map<String, String> variables = new java.util.LinkedHashMap<>();
        for (Map<String, Object> variable : list(input, "variables")) {
            variables.put(requiredString(variable, "name"), requiredString(variable, "value"));
        }
        return map(templates.preview(new NotificationTemplateService.PreviewCommand(
            requiredString(input, "templateKey"), requiredString(input, "channel"),
            requiredString(input, "locale"), variables)));
    }

    @MutationMapping
    public TemplateView publishNotificationTemplate(
            @Argument Map<String, Object> input,
            @AuthenticationPrincipal Jwt jwt) {
        var actor = authorization.requireOperator(jwt, "notifications:template:publish");
        return map(templates.publish(templateCommand(input, actor)));
    }

    @MutationMapping
    public TemplateView retireNotificationTemplate(
            @Argument Map<String, Object> input,
            @AuthenticationPrincipal Jwt jwt) {
        var actor = authorization.requireOperator(jwt, "notifications:template:publish");
        return map(templates.retire(new NotificationTemplateService.RetireCommand(
            uuid(input, "templateId"), integer(input, "expectedVersion", 0),
            requiredString(input, "reason"), requiredString(input, "idempotencyKey"),
            actor.accountType(), actor.accountId())));
    }

    private PreferenceUpdateView map(NotificationPreferenceService.Preference preference) {
        QuietHours quietHours = preference.quietHoursStart() == null ? null
            : new QuietHours(preference.quietHoursStart().toString(), preference.quietHoursEnd().toString());
        return new PreferenceUpdateView(preference.id(), preference.notificationType(), preference.channel(),
            preference.enabled(), preference.required(), quietHours, preference.timeZoneId(),
            preference.revision(), preference.updatedAt() == null ? null : preference.updatedAt().toString());
    }

    private PreferenceConnection map(NotificationPreferenceService.PreferenceConnection connection) {
        return new PreferenceConnection(connection.nodes().stream().map(this::map).toList(), connection.revision());
    }

    private TemplateView map(NotificationTemplateService.Template template) {
        return new TemplateView(template.id(), template.templateKey(), template.channel(), template.locale(),
            template.status(), template.revision(), template.policyRevision(), template.requiredVariables(),
            template.subject(), template.body(), template.immutable(), template.createdAt().toString(),
            template.publishedAt() == null ? null : template.publishedAt().toString(),
            template.retiredAt() == null ? null : template.retiredAt().toString());
    }

    private TemplatePreviewView map(NotificationTemplateService.TemplatePreview preview) {
        return new TemplatePreviewView(preview.templateKey(), preview.channel(), preview.locale(),
            preview.revision(), preview.subject(), preview.body());
    }

    private static NotificationTemplateService.PublishCommand templateCommand(
            Map<String, Object> input,
            NotificationAuthorizationService.AuthorizedActor actor) {
        return new NotificationTemplateService.PublishCommand(
            requiredString(input, "templateKey"), requiredString(input, "channel"),
            requiredString(input, "locale"), string(input, "subject"),
            requiredString(input, "body"), strings(input, "requiredVariables"),
            integer(input, "policyRevision", 1), nullableInteger(input, "expectedVersion"),
            requiredString(input, "idempotencyKey"), actor.accountType(), actor.accountId());
    }

    private static NotificationPreferenceService.PreferenceUpdate preferenceUpdate(Map<String, Object> input) {
        Map<String, Object> quiet = map(input.get("quietHours"));
        return new NotificationPreferenceService.PreferenceUpdate(
            requiredString(input, "notificationType"), requiredString(input, "channel"),
            booleanValue(input, "enabled"), quiet == null ? null : LocalTime.parse(requiredString(quiet, "start")),
            quiet == null ? null : LocalTime.parse(requiredString(quiet, "end")),
            requiredString(input, "timeZoneId"), integer(input, "expectedVersion", 0));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object value) {
        return value instanceof Map<?, ?> map ? (Map<String, Object>) map : null;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> list(Map<String, Object> input, String field) {
        Object value = input == null ? null : input.get(field);
        return value instanceof List<?> list ? (List<Map<String, Object>>) (List<?>) list : List.of();
    }

    private static List<String> strings(Map<String, Object> input, String field) {
        Object value = input == null ? null : input.get(field);
        if (!(value instanceof List<?> list)) {
            return List.of();
        }
        return list.stream().map(String::valueOf).toList();
    }

    private static String requiredString(Map<String, Object> input, String field) {
        String value = string(input, field);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }

    private static String string(Map<String, Object> input, String field) {
        Object value = input == null ? null : input.get(field);
        return value == null ? null : String.valueOf(value);
    }

    private static int integer(Map<String, Object> input, String field, int defaultValue) {
        Integer value = nullableInteger(input, field);
        return value == null ? defaultValue : value;
    }

    private static Integer nullableInteger(Map<String, Object> input, String field) {
        Object value = input == null ? null : input.get(field);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.valueOf(String.valueOf(value));
    }

    private static boolean booleanValue(Map<String, Object> input, String field) {
        Object value = input == null ? null : input.get(field);
        return value instanceof Boolean bool ? bool : Boolean.parseBoolean(String.valueOf(value));
    }

    private static UUID uuid(Map<String, Object> input, String field) {
        return UUID.fromString(requiredString(input, field));
    }

    public record PreferenceConnection(List<PreferenceUpdateView> nodes, int revision) {}

    public record PreferenceUpdateView(
            UUID id,
            String notificationType,
            String channel,
            boolean enabled,
            boolean required,
            QuietHours quietHours,
            String timeZoneId,
            int revision,
            String updatedAt) {}

    public record QuietHours(String start, String end) {}

    public record TemplateConnection(List<TemplateView> nodes, NotificationPageInfo pageInfo) {}

    public record TemplatePreviewView(
            String templateKey,
            String channel,
            String locale,
            int revision,
            String subject,
            String body) {}

    public record NotificationPageInfo(
            boolean hasNextPage,
            String endCursor,
            String startCursor,
            String cursorState,
            String retainedFrom,
            String reconcileFrom) {}

    public record TemplateView(
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
            String createdAt,
            String publishedAt,
            String retiredAt) {}
}
