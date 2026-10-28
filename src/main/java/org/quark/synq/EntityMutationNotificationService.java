package org.quark.synq;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;
import org.springframework.transaction.reactive.TransactionContextManager;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import tools.jackson.databind.ObjectMapper;

/** A notification service that handles publishing of events signifying changes to entity sets */
public abstract class EntityMutationNotificationService {
	/**
	 * Serializes an entity to a string
	 * 
	 * @param <E> The type of the entity
	 */
	public interface EntitySerializer<E> {
		/**
		 * Serializes an entity
		 * 
		 * @param entity The entity to serialize
		 * @return The JSON-serialized entity
		 */
		String serialize(E entity);
	}

	/**
	 * Default implementation of {@link EntitySerializer}
	 * 
	 * @param <E> The type of the entity
	 */
	public static class ReflectiveSerializer<E> implements EntitySerializer<E> {
		private final ObjectMapper theObjectMapper;

		/**
		 * Creates the serializer
		 * 
		 * @param type The entity type
		 * @param objectMapper The JSON object mapper
		 */
		public ReflectiveSerializer(Class<E> type, ObjectMapper objectMapper) {
			theObjectMapper = objectMapper;
		}

		@Override
		public String serialize(E entity) {
			return theObjectMapper.writeValueAsString(entity);
		}
	}

	private static class EntitySerializerHolder<E> {
		final Class<E> entityType;
		final EntitySerializer<E> serializer;

		EntitySerializerHolder(Class<E> entityType, EntitySerializer<E> serializer) {
			this.entityType = entityType;
			this.serializer = serializer;
		}
	}

	private static class InternalDataMutationEvent extends ApplicationEvent {
		final String entityType;
		final boolean present;
		final String entityJson;
		final Sinks.One<Mono<MessageId>> messageId;

		InternalDataMutationEvent(Object source, String entityType, boolean present, String entityJson) {
			super(source);
			this.entityType = entityType;
			this.present = present;
			this.entityJson = entityJson;
			messageId = Sinks.one();
		}
	}

	static final Logger log = LoggerFactory.getLogger(EntityMutationNotificationService.class);

	private final Map<String, EntitySerializerHolder<?>> theSerializers = new ConcurrentHashMap<>();

	private final Sinks.Many<EntityMutationEvent> theLocalSink = Sinks.many().unicast().onBackpressureBuffer();
	// share() allows multiple subscribers
	private final Flux<EntityMutationEvent> theSharedLocalFlux = theLocalSink.asFlux()//
		.publish().autoConnect();

	/**
	 * Creates the notification service
	 */
	protected EntityMutationNotificationService() {}

	/**
	 * Installs an event serializer in this notification service, enabling it to recognize a new type of entity
	 * 
	 * @param <E> The type of the entity
	 * @param entityTypeName The type name for the entity, as it will be given to {@link #publishMutation(String, boolean, Object)}
	 * @param entityType The type of the entity
	 * @param serializer The serializer to use for entities of this type
	 */
	public <E> void installSerializer(String entityTypeName, Class<E> entityType, EntitySerializer<E> serializer) {
		theSerializers.put(entityTypeName, new EntitySerializerHolder<>(entityType, serializer));
	}

	/**
	 * Tells Synq that an entity has been added, removed, or changed
	 * 
	 * @param <E> The type of the entity
	 * @param entityTypeName The type name of the entity. Must match that of a serializer given to
	 *        {@link #installSerializer(String, Class, EntitySerializer)}
	 * @param present Whether the entity was added/updated (true) or removed (false)
	 * @param entity The entity that was added, removed, or changed
	 * @return The ID of the message that will eventually be propagated as a {@link SyncService.SyncDataEvent}
	 */
	public <E> Mono<MessageId> publishMutation(String entityTypeName, boolean present, E entity) {
		if (log.isDebugEnabled())
			log.debug("Creating " + entityTypeName + " event");
		EntitySerializerHolder<E> serializer = (EntitySerializerHolder<E>) theSerializers.get(entityTypeName);
		if (serializer == null)
			throw new IllegalArgumentException("No serializer installed for entity type " + entityTypeName);
		else if (!serializer.entityType.isInstance(entity))
			throw new IllegalArgumentException(
				"Given entity is not an instance of " + entityTypeName + " (" + serializer.entityType.getName() + ")");
		String entityJson = serializer.serializer.serialize(entity);

		// Core business logic definition
		Mono<MessageId> globalMutationExecution = Mono.defer(() -> publishGlobalMutation(entityTypeName, present, entityJson));

		// Let's inspect the ThreadLocal to check if we are in an imperative WebMVC/JDBC Transaction
		if (TransactionSynchronizationManager.isActualTransactionActive()) {
			if (log.isDebugEnabled()) {
				log.debug("Imperative transaction active. Postponing global mutation publication until AFTER_COMMIT.");
			}

			Sinks.One<MessageId> imperativeBridgeSink = Sinks.one();

			TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
				@Override
				public void afterCommit() {
					if (log.isDebugEnabled()) {
						log.debug("Post-commit " + entityTypeName + " event triggered (WebMVC)");
					}
					// Offload the Redis I/O to boundedElastic so we don't freeze the JDBC execution thread
					globalMutationExecution.subscribeOn(Schedulers.boundedElastic()).subscribe(imperativeBridgeSink::tryEmitValue,
						imperativeBridgeSink::tryEmitError);
				}
			});

			return imperativeBridgeSink.asMono();
		}

		// Otherwise, assume a Reactive (WebFlux/R2DBC) environment or no transaction context.
		// We defer to Reactor Context exploration to attach afterCommit behavior reactively.
		return TransactionContextManager.currentContext().flatMap(_ -> {
			if (log.isDebugEnabled()) {
				log.debug("Reactive transaction context found. Executing reactively.");
			}
			// If your R2DBC configuration manages transactional completions downstream,
			// we directly run the global pipeline inside the stream.
			return globalMutationExecution;
		})
			// Fallback: If no transaction context exists (pure fire-and-forget or non-transactional thread)
			.onErrorResume(_ -> {
				if (log.isDebugEnabled()) {
					log.debug("No active transaction context found. Executing immediately.");
				}
				return globalMutationExecution;
			});
	}

	Flux<EntityMutationEvent> mutations() {
		return theSharedLocalFlux;
	}

	abstract Mono<Optional<MessageId>> getLatestMutation();

	abstract Mono<Optional<MessageId>> getOldestCachedMutation();

	/**
	 * @param after The message ID to get mutations after, or null to get all cached mutations
	 * @return All events in the cache after the given message
	 */
	abstract Flux<EntityMutationEvent> getCachedMutations(MessageId after);

	@TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
	private void publishInternalMutation(InternalDataMutationEvent event) {
		if (log.isDebugEnabled())
			log.debug("Post-commit " + event.entityType + " event");
		event.messageId.tryEmitValue(publishGlobalMutation(event.entityType, event.present, event.entityJson));
	}

	/**
	 * Publishes an event to the global messaging mechanism
	 * 
	 * @param entityType The entity type the event is for
	 * @param present Whether the entity was added/updated (true) or removed (false)
	 * @param entityJson The serialized entity that was added, removed, or changed
	 * @return The ID of the message that will eventually be propagated as a {@link SyncService.SyncDataEvent}
	 */
	protected abstract Mono<MessageId> publishGlobalMutation(String entityType, boolean present, String entityJson);

	/**
	 * Publishes an event to all subscribers on this notification service instance
	 * 
	 * @param event The event to publish to listeners
	 */
	protected void publishLocalMutation(EntityMutationEvent event) {
		if (log.isDebugEnabled())
			log.debug("Publishing " + event.getEntityTypeName() + " event");
		theLocalSink.emitNext(event, Sinks.EmitFailureHandler.busyLooping(Duration.ofSeconds(5)));
	}

	/**
	 * Gets this class's logger
	 * 
	 * @return The logger that this class uses
	 */
	public static Logger getLogger() {
		return log;
	}
}
