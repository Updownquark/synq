package org.quark.synq;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.ReactiveRedisTemplate;

/**
 * <p>
 * Auto-configures Synq for a Spring application.
 * </p>
 * <p>
 * A {@link SyncUserResolver} bean must be exposed by the application.
 * </p>
 * Configuration properties:
 * <ul>
 * <li><b>quark.synq.scaling.enabled</b>: Enables a REDIS-based event system for multi-instance applications. The default event
 * implementation is for single-instance applications.</li>
 * <li><b>quark.synq.scaling.stream-key</b>: For quark.synq.scaling.enabled=true only. The REDIS stream key to use for synq events.</li>
 * <li><b>quark.synq.max-events</b>: The maximum number of events to keep in the event cache. Affects the ability of clients to be updated
 * after a temporary connection loss without needing to completely repopulate the data set.</li>
 * </ul>
 */
@Configuration
public class SynqAutoConfiguration {
	SynqAutoConfiguration() {}

	@Bean
	@ConditionalOnProperty(name = "quark.synq.scaling.enabled", havingValue = "false", matchIfMissing = true)
	<U> LocalMutationNotificationService getLocalEntityNotificationService(//
		ApplicationEventPublisher springEventPublisher, //
		@Value("${quark.sync.scaling.max-events}") int maxEvents) {
		return new LocalMutationNotificationService(springEventPublisher, maxEvents);
	}

	@Bean
	@ConditionalOnProperty(name = "quark.synq.scaling.enabled", havingValue = "true", matchIfMissing = false)
	<U> RedisMutationNotificationService getRedisEntityNotificationService(//
		ReactiveRedisTemplate<String, String> redisTemplate, //
		ApplicationEventPublisher springEventPublisher, //
		@Value("${quark.synq.scaling.stream-key}") String streamKey, //
		@Value("${quark.synq.max-events}") int maxEvents) {
		return new RedisMutationNotificationService(redisTemplate, springEventPublisher, streamKey, maxEvents);
	}

	@Bean
	<U> SyncService<U> getSyncService(EntityMutationNotificationService entityNotificationService,
		SyncUserResolver<U> userIdentifier) {
		return new SyncService<>(entityNotificationService, userIdentifier);
	}

	@Bean
	<U> SyncController<U> getSyncController(SyncService<U> syncService, SyncUserResolver<U> userResolver) {
		return new SyncController<>(syncService, userResolver);
	}
}
