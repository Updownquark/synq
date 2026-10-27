package org.quark.synq;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.qommons.fn.FunctionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
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

	private final ApplicationEventPublisher theInternalEventPublisher;

	private final Map<String, EntitySerializerHolder<?>> theSerializers = new ConcurrentHashMap<>();

	private final Sinks.Many<EntityMutationEvent> theLocalSink = Sinks.many().unicast().onBackpressureBuffer();
	// share() allows multiple subscribers
	private final Flux<EntityMutationEvent> theSharedLocalFlux = theLocalSink.asFlux()//
		.publish().autoConnect();

	/**
	 * Creates the notification service
	 * 
	 * @param internalEventPublisher The Spring event publisher to utilize for internal events
	 */
	protected EntityMutationNotificationService(ApplicationEventPublisher internalEventPublisher) {
		theInternalEventPublisher = internalEventPublisher;
	}

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
		InternalDataMutationEvent internalEvent = new InternalDataMutationEvent(this, entityTypeName, present, entityJson);
		theInternalEventPublisher.publishEvent(internalEvent);
		return internalEvent.messageId.asMono().flatMap(FunctionUtils.identity());
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
