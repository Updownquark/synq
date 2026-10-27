package org.quark.synq;

import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.qommons.IterableUtils;
import org.qommons.QommonsUtils;
import org.qommons.Subscription;
import org.qommons.collect.BetterList;
import org.qommons.json.JsonObject;
import org.qommons.json.JsonSerialReader;
import org.qommons.json.SAJParser;
import org.quark.synq.SyncDataSource.SyncDataFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.codec.ServerSentEvent;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

/**
 * The service implementation for Synq. Watches the {@link EntityMutationNotificationService} and publishes events to subscribed clients.
 * Handles reconnection and subscription.
 * 
 * @param <U> The type of user object used by the application
 */
public class SyncService<U> {
	/**
	 * An event that may affect one or more subscribed sync streams
	 * 
	 * @param <U> The type of user object used by the application
	 * @param <T> The type of the entity set that was affected
	 */
	public interface SyncEvent<U, T> {
		/**
		 * Gets the data source for this event's entity type
		 * 
		 * @return The data source that was affected
		 */
		public SyncDataSource<U, T> getSource();

		/**
		 * Gets the type name for this event's entity type
		 * 
		 * @return The name of the entity type that was affected
		 */
		default String getEntityTypeName() {
			return getSource().getEntityTypeName();
		}
	}

	/**
	 * A sync event signifying a change to an entity in a data set
	 * 
	 * @param <U> The type of user object used by the application
	 * @param <T> The type of the entity that was changed
	 */
	public static class SyncDataEvent<U, T> implements SyncEvent<U, T> {
		private final MessageId theMessageId;
		private final SyncDataSource<U, T> theSource;
		private final boolean isPresent;
		private final T theEntity;
		private final String theEntityJson;
		private final String theEventJson;

		SyncDataEvent(MessageId messageId, SyncDataSource<U, T> source, boolean present, T entity, String entityJson) {
			theMessageId = messageId;
			theSource = source;
			isPresent = present;
			theEntity = entity;
			theEntityJson = entityJson;
			theEventJson = new StringBuilder("{")//
				.append("\"type\":\"").append(isPresent ? "addOrUpdate" : "remove").append('"')//
				.append(",\"entityType\":\"").append(theSource.getEntityTypeName()).append('"')//
				.append(",\"entity\":").append(theEntityJson)//
				.append(",\"sequence\":").toString();
		}

		/**
		 * Gets this event's message ID
		 * 
		 * @return The ID of the message
		 */
		public MessageId getMessageId() {
			return theMessageId;
		}

		@Override
		public SyncDataSource<U, T> getSource() {
			return theSource;
		}

		@Override
		public String getEntityTypeName() {
			return theSource.getEntityTypeName();
		}

		/**
		 * Gets the type of this change
		 * 
		 * @return True if the entity was added or updated; false if it was deleted
		 */
		public boolean isPresent() {
			return isPresent;
		}

		/**
		 * Gets the affected entity
		 * 
		 * @return The entity that was added, removed, or updated
		 */
		public T getEntity() {
			return theEntity;
		}

		/**
		 * Creates a JSON event to send to a client
		 * 
		 * @param sequence The client sequence ID for the event
		 * @return The JSON event to propagate to the client
		 */
		public String toClientJson(long sequence) {
			return new StringBuilder(theEventJson).append(sequence).append('}').toString();
		}

		@Override
		public String toString() {
			return theEventJson;
		}
	}

	/**
	 * Represents a change to the set of entities a stream is subscribed to
	 * 
	 * @param <U> The type of user object used by the application
	 * @param <T> The type of the entity set that was affected
	 */
	static class SubscriptionChangeEvent<U, T> implements SyncEvent<U, T> {
		final String streamId;
		final SyncDataSource<U, T> source;
		final Set<String> unsubscribe;
		final List<SyncDataSubscription<U, T>> subscribe;
		final boolean withInitialData;

		SubscriptionChangeEvent(String streamId, SyncDataSource<U, T> source, Set<String> unsubscribe,
			List<SyncDataSubscription<U, T>> subscribe, boolean withInitialData) {
			this.streamId = streamId;
			this.source = source;
			this.unsubscribe = unsubscribe;
			this.subscribe = subscribe;
			this.withInitialData = withInitialData;
		}

		@Override
		public SyncDataSource<U, T> getSource() {
			return source;
		}

		@Override
		public String toString() {
			StringBuilder str = new StringBuilder()//
				.append(streamId, 0, 8)//
				.append(' ').append(getEntityTypeName())//
				.append(" subChange:");
			if (unsubscribe != null && !unsubscribe.isEmpty()) {
				str.append("\n\t-[");
				boolean first = true;
				for (String unsub : unsubscribe) {
					if (first)
						first = false;
					else
						str.append(", ");
					str.append(unsub.substring(0, 8));
				}
				str.append(']');
			}
			if (subscribe != null && !subscribe.isEmpty()) {
				str.append("\n\t+").append(subscribe);
			}
			return str.toString();
		}
	}

	static final Logger log = LoggerFactory.getLogger(SyncService.class);

	private final EntityMutationNotificationService theNotificationService;
	private SyncUserResolver<U> theUserIdentity;

	private final Map<String, SyncDataSource<U, ?>> theDataSources;
	private final Map<String, ClientEventStream> theClientStreams;

	private final Sinks.Many<SyncDataEvent<U, ?>> theLiveSink = Sinks.many().unicast().onBackpressureBuffer();
	private final Flux<SyncDataEvent<U, ?>> theSharedLiveFlux = theLiveSink.asFlux()//
		.publish().autoConnect();

	private final ConcurrentHashMap<String, Boolean> theLoggedMissingEntityTypes;

	/**
	 * Creates the sync service
	 * 
	 * @param notificationService The entity notification service to listen to
	 * @param userId Resolves IDs for user objects
	 */
	public SyncService(EntityMutationNotificationService notificationService, SyncUserResolver<U> userId) {
		theNotificationService = notificationService;
		theUserIdentity = userId;
		theDataSources = new ConcurrentHashMap<>();
		theClientStreams = new ConcurrentHashMap<>();
		theLoggedMissingEntityTypes = new ConcurrentHashMap<>();

		theNotificationService.mutations()//
		.publishOn(Schedulers.boundedElastic())//
		.concatMap(event -> Mono.fromCallable(() -> parseEvent(event)))//
		.filter(Objects::nonNull)//
		// .doOnNext(theLiveSink::tryEmitNext)//
		.doOnNext(event -> {
			if (log.isDebugEnabled())
				log.debug("Received data mod " + event);
			// Enforces a non-blocking busy loop back-off to guarantee thread emission safety
			theLiveSink.emitNext(event, Sinks.EmitFailureHandler.busyLooping(Duration.ofSeconds(2)));
		})//
		.subscribe();
	}

	/**
	 * Installs a new data source in this service, allowing a new data set to be subscribed to by clients
	 * 
	 * @param dataSource The data source to handle the new data set
	 */
	public void installDataSource(SyncDataSource<U, ?> dataSource) {
		theDataSources.put(dataSource.getEntityTypeName(), dataSource);
	}

	/**
	 * In-memory subscription to events
	 * 
	 * @param <T> The type of the entity to receive events for
	 * @param entityTypeName The type name of the entity to receive events for
	 * @param entityType The type of the entity to receive events for
	 * @param listener The listener to receive data events when entities of the given type are added, removed, or changed
	 * @return A subscription to {@link Subscription#unsubscribe()} to cease notification
	 */
	public <T> Subscription subscribe(String entityTypeName, Class<T> entityType, Consumer<SyncDataEvent<U, T>> listener) {
		SyncDataSource<U, T> dataSource = (SyncDataSource<U, T>) theDataSources.get(entityTypeName);
		if (dataSource == null)
			throw new IllegalArgumentException("No data source installed for entity type " + entityTypeName);
		else if (!entityType.isAssignableFrom(dataSource.getEntityType()))
			throw new IllegalArgumentException("Data source for entity type " + entityTypeName + " is not of the correct type ("
				+ dataSource.getEntityType().getName() + ")");
		final String realEntityType = dataSource.getEntityTypeName();
		var sub = theSharedLiveFlux//
			.publishOn(Schedulers.boundedElastic())//
			.doOnNext(event -> {
				if (event instanceof SyncDataEvent && realEntityType == event.getEntityTypeName()) {
					listener.accept((SyncDataEvent<U, T>) event);
				}
			})//
			.subscribe();
		return sub::dispose;
	}

	/**
	 * Initializes a sync stream
	 * 
	 * @param me The currently-authenticated user
	 * @param initialSubscriptions Optional configuration for initial data subscriptions
	 * @param lastEventId The optional ID of the most recent message that the client received before being disconnected
	 * @return The event stream
	 */
	public Flux<ServerSentEvent<String>> initStream(U me, String lastEventId, String initialSubscriptions) {
		List<SyncDataSubscription<U, ?>> subscriptions = new ArrayList<>();
		if (initialSubscriptions != null && !initialSubscriptions.isEmpty()) {
			Object json;
			try {
				json = new JsonSerialReader(new StringReader(initialSubscriptions)).parseNext(false);
			} catch (IOException e) {
				throw new IllegalStateException("IOException?", e);
			} catch (SAJParser.ParseException e) {
				throw new IllegalArgumentException("Bad subscriptions JSON: " + initialSubscriptions, e);
			}
			if (!(json instanceof List))
				throw new IllegalArgumentException("Subscriptions must be a JSON array: " + initialSubscriptions);
			for (Object jsonSub : (List<?>) json) {
				if (!(jsonSub instanceof JsonObject))
					throw new IllegalArgumentException("Subscriptions must be JSON objects");
				subscriptions.add(parseSubscription(me, (JsonObject) jsonSub));
			}
		}

		Mono<Optional<MessageId>> oldest = theNotificationService.getOldestCachedMutation();
		Mono<Optional<MessageId>> newest = theNotificationService.getLatestMutation();

		// 1. Create a dynamic processor sink specifically to listen for this individual client's disconnection signal
		Sinks.Empty<Void> disconnectSink = Sinks.empty();
		Flux<SyncDataEvent<U, ?>> liveFlux = theSharedLiveFlux//
			.takeUntilOther(disconnectSink.asMono());

		MessageId lastMessageId = lastEventId == null ? null : new MessageId(lastEventId);
		if (theUserIdentity == null)
			throw new IllegalStateException("User Identity function is not set");
		ClientEventStream clientStream = new ClientEventStream(theUserIdentity.getUserId(me));
		Flux<ServerSentEvent<String>> clientEventFlux;
		if (lastMessageId == null) { // New connection, no past memory
			clientEventFlux = newest.flatMapMany(newestId -> {
				Flux<SyncDataEvent<U, ?>> handshakeGapClosure = theNotificationService.getCachedMutations(newestId.orElse(null))//
					.map(this::parseEvent);
				return clientStream.init(newestId.orElse(null), false, subscriptions, Flux.concat(handshakeGapClosure, liveFlux));
			});
		} else {
			clientEventFlux = oldest.flatMapMany(oldestId -> {
				if (oldestId.isEmpty() || oldestId.get().compareTo(lastMessageId) > 0) {
					// Client's memory is out-of-date and it needs to re-sync
					return newest.flatMapMany(newestId -> {
						Flux<SyncDataEvent<U, ?>> handshakeGapClosure = theNotificationService.getCachedMutations(newestId.orElse(null))//
							.map(this::parseEvent);
						return clientStream.init(newestId.orElse(null), false, subscriptions,
							Flux.<SyncDataEvent<U, ?>> concat(handshakeGapClosure, liveFlux));
					});
				} else { // Client's memory is within the history window, so we can just catch it up
					Flux<? extends SyncDataEvent<U, ?>> catchUpFlux = theNotificationService.getCachedMutations(lastMessageId)//
						.map(this::parseEvent);

					return clientStream.init(lastMessageId, true, subscriptions, Flux.concat(catchUpFlux, liveFlux));
				}
			});
		}

		// Build an isolated background ticker that emits an empty comment frame every 30 seconds
		// Native SSE rules specify that comments (: keepalive) are discarded by browsers but keep connections active
		Flux<ServerSentEvent<String>> heartbeatFlux = Flux//
			.interval(Duration.ofSeconds(30))//
			.map(_ -> ServerSentEvent.<String> builder().comment("keepalive").build())//
			.takeUntilOther(disconnectSink.asMono()); // Dismantle ticker when connection closes

		Flux<ServerSentEvent<String>> fullClientFlux = Flux.merge(clientEventFlux, heartbeatFlux);

		// Register the client stream and return the event flux
		theClientStreams.put(clientStream.streamId, clientStream);
		return fullClientFlux.doOnCancel(() -> {
			theClientStreams.remove(clientStream.streamId);
			disconnectSink.tryEmitEmpty(); // Signal the liveFlux to stop emitting events for this client
		}).doOnTerminate(() -> {
			theClientStreams.remove(clientStream.streamId);
			disconnectSink.tryEmitEmpty(); // Signal the liveFlux to stop emitting events for this client
		});
	}

	private SyncDataSubscription<U, ?> parseSubscription(U me, JsonObject sub) {
		Object entityType = sub.get("subscribeEntityType");
		if (entityType == null)
			throw new IllegalArgumentException("Missing subscribeEntityType in subscription");
		else if (!(entityType instanceof String))
			throw new IllegalArgumentException("subscribeEntityType must be a string");
		SyncDataSource<U, ?> dataSource = theDataSources.get(entityType);
		if (dataSource == null)
			throw new IllegalArgumentException("No data source installed for entity type " + entityType);
		return new SyncDataSubscription<>(me, dataSource, sub);

	}

	/**
	 * Modifies an existing sync stream
	 * 
	 * @param me The currently-authenticated user
	 * @param streamId The ID of the stream to modify
	 * @param unsubscribe A list of IDs of subscriptions to unsubscribe
	 * @param subscribe Configuration for new subscriptions
	 * @param withInitialData Whether to send the full initial data set for any affected entity sets
	 * @return The IDs of the new subscriptions
	 */
	public List<String> modifySubscriptions(U me, String streamId, List<String> unsubscribe, String subscribe, boolean withInitialData) {
		ClientEventStream clientStream = theClientStreams.get(streamId);
		if (clientStream == null || !clientStream.ownerId.equals(theUserIdentity.getUserId(me)))
			throw new NoSuchElementException("No client stream with ID " + streamId);

		class SubChange<T> {
			final SyncDataSource<U, T> source;
			final Set<String> unsubscribe = new HashSet<>();
			final List<SyncDataSubscription<U, T>> subscribe = new ArrayList<>();

			SubChange(SyncDataSource<U, T> source) {
				this.source = source;
			}

			void add(SyncDataSubscription<U, ?> sub) {
				subscribe.add((SyncDataSubscription<U, T>) sub);
			}

			SubscriptionChangeEvent<U, T> toEvent() {
				return new SubscriptionChangeEvent<U, T>(streamId, source, //
					unsubscribe.isEmpty() ? null : unsubscribe, //
						subscribe.isEmpty() ? null : subscribe, withInitialData);
			}
		}
		Map<String, SubChange<?>> groupedSubChanges = new HashMap<>();
		if (unsubscribe != null) {
			for (String subId : unsubscribe) {
				String[] parts = subId.split("/", 2);
				if (parts.length != 2)
					throw new IllegalArgumentException("Invalid subscription ID " + subId);
				String entityType = parts[0];
				groupedSubChanges.computeIfAbsent(entityType, _ -> {
					SyncDataSource<U, ?> source = theDataSources.get(entityType);
					if (source == null)
						throw new IllegalArgumentException("No data source installed for entity type " + entityType);
					return new SubChange<>(source);
				}).unsubscribe.add(parts[1]);
			}
		}
		List<String> subscriptionIds;
		if (subscribe != null && !subscribe.isEmpty()) {
			subscriptionIds = new ArrayList<>();
			Object json;
			try {
				json = new JsonSerialReader(new StringReader(subscribe)).parseNext(false);
			} catch (IOException e) {
				throw new IllegalStateException("IOException?", e);
			} catch (SAJParser.ParseException e) {
				throw new IllegalArgumentException("Bad subscriptions JSON: " + subscribe, e);
			}
			if (!(json instanceof List))
				throw new IllegalArgumentException("Subscriptions must be a JSON array");
			for (Object jsonSub : (List<?>) json) {
				if (!(jsonSub instanceof JsonObject))
					throw new IllegalArgumentException("Subscriptions must be JSON objects");
				SyncDataSubscription<U, ?> sub = parseSubscription(me, (JsonObject) jsonSub);
				subscriptionIds
				.add(new StringBuilder(sub.getDataSource().getEntityTypeName()).append('/').append(sub.getSubscriptionId()).toString());
				groupedSubChanges.computeIfAbsent(sub.getDataSource().getEntityTypeName(), _ -> {
					return new SubChange<>(sub.getDataSource());
				}).add(sub);
			}
		} else
			subscriptionIds = Collections.emptyList();
		for (SubChange<?> change : groupedSubChanges.values()) {
			clientStream.queueSubscriptionChange(change.toEvent());
		}

		return subscriptionIds;
	}

	/**
	 * Gets this class's logger
	 * 
	 * @return The logger that this class uses
	 */
	public static Logger getLogger() {
		return log;
	}

	private <T> SyncDataEvent<U, T> parseEvent(EntityMutationEvent mutationEvent) {
		String entityTypeName = mutationEvent.getEntityTypeName();
		SyncDataSource<U, T> source = (SyncDataSource<U, T>) theDataSources.get(entityTypeName);
		if (source == null) {
			if (null == theLoggedMissingEntityTypes.put(entityTypeName, Boolean.TRUE)) {
				log.error("No data source installed for entity type {}", entityTypeName);
			}
			return null;
		}
		T entity;
		try {
			entity = source.deserialize(mutationEvent.getEntityJson());
		} catch (ParseException e) {
			log.error("Failed to deserialize entity for event " + mutationEvent.getId() + " of type " + entityTypeName, e);
			return null;
		}
		return new SyncDataEvent<>(mutationEvent.getId(), source, mutationEvent.isPresent(), entity, mutationEvent.getEntityJson());
	}

	private class ClientEventStream {
		final Object ownerId;
		final String streamId;
		private final AtomicLong theSequence;
		private final AtomicReference<MessageId> theLastMessageId;
		private final Map<String, Map<String, SyncDataSubscription<U, ?>>> theSubscriptions = new ConcurrentHashMap<>();
		private final Sinks.Many<SubscriptionChangeEvent<U, ?>> theSubscriptionUpdateEventSink = Sinks.many().unicast()
			.onBackpressureBuffer();

		ClientEventStream(Object ownerId) {
			this.ownerId = ownerId;
			this.streamId = QommonsUtils.getRandomString(100);
			theSequence = new AtomicLong();
			theLastMessageId = new AtomicReference<>();
		}

		Flux<ServerSentEvent<String>> init(MessageId lastEventId, boolean catchUp, List<SyncDataSubscription<U, ?>> subscriptions,
			Flux<? extends SyncDataEvent<U, ?>> eventFlux) {
			theLastMessageId.set(lastEventId);

			for (SyncDataSubscription<U, ?> sub : subscriptions) {
				theSubscriptions.computeIfAbsent(sub.getDataSource().getEntityTypeName(), _ -> new ConcurrentHashMap<>())
				.put(sub.getSubscriptionId(), sub);
			}

			// Use Flux.defer to guarantee that the underlying hot eventFlux is subscribed to
			// at the exact moment the HTTP client initiates demand, preventing connection-window gaps.
			Flux<ServerSentEvent<String>> processedEventFlux = Flux.defer(() -> eventFlux//
				.filter(Objects::nonNull)//
				.filter(event -> {
					// Ignore duplicate events due to overlap between catchup and events that are currently live-firing.
					MessageId eventId = ((SyncDataEvent<U, ?>) event).getMessageId();
					MessageId newLastId = theLastMessageId.accumulateAndGet(eventId, QommonsUtils::max);
					boolean pass = newLastId == eventId;
					if (!pass && log.isDebugEnabled())
						log.debug("Ignoring duplicate data event " + event);
					return pass;
				})//
				.flatMap(this::processDataEvent, 1)//
				);

			// Compile the handshake payload sequence records lazily inside an iterable block
			Flux<ServerSentEvent<String>> initFlux = Flux.defer(() -> {
				JsonObject initEventJson = new JsonObject()//
					.with("type", "init")//
					.with("sequence", theSequence.getAndIncrement())//
					.with("streamId", streamId)//
					.with("resume", catchUp)//
					;
				if (!catchUp) {
					initEventJson.with("lastEventId", lastEventId == null ? null : lastEventId.toString());
				}
				initEventJson.with("initSubscriptions", subscriptions.stream().map(sub -> new StringBuilder()//
					.append(sub.getDataSource().getEntityTypeName()).append('/').append(sub.getSubscriptionId()).toString())//
					.toList());
				List<ServerSentEvent<String>> initialEvents = new ArrayList<>();
				initialEvents.add(ServerSentEvent.<String> builder()//
					.id("")//
					.data(initEventJson.toString())//
					.build());
				if (!catchUp && !subscriptions.isEmpty()) {
					// If we're not catching up, we need to populate the initial data for all initial subscriptions
					for (Map<String, SyncDataSubscription<U, ?>> typeSubscriptions : theSubscriptions.values()) {
						String initialData = populateInitialData(typeSubscriptions);
						initialEvents.add(ServerSentEvent.<String> builder()//
							.id("")//
							.data(initialData)//
							.build());
					}
				}
				return Flux.fromIterable(initialEvents);
			});
			Flux<ServerSentEvent<String>> staticOutputFlux = Flux.concat(initFlux, processedEventFlux);
			Flux<ServerSentEvent<String>> subUpdateOutputFlux = theSubscriptionUpdateEventSink.asFlux()//
				.publishOn(Schedulers.single()) //
				.flatMap(this::modifySubscriptions)//
				.publish()//
				.autoConnect();
			return Flux.merge(staticOutputFlux, subUpdateOutputFlux);
		}

		public void queueSubscriptionChange(SubscriptionChangeEvent<U, ?> event) {
			if (log.isDebugEnabled())
				log.debug("\nQueueing " + event.getEntityTypeName() + " " + event);
			theSubscriptionUpdateEventSink.emitNext(event, //
				Sinks.EmitFailureHandler.busyLooping(Duration.ofSeconds(2)));
		}

		private <T> String populateInitialData(Map<String, SyncDataSubscription<U, ?>> typeSubscriptions) {
			SyncDataSource<U, T> dataSource = typeSubscriptions.values().stream().findFirst().map(SyncDataSubscription::getDataSource)
				.map(ds -> (SyncDataSource<U, T>) ds).get();
			StringBuilder initialDataJson = new StringBuilder("{\"type\":\"reset\"")//
				.append(",\"sequence\":").append(theSequence.getAndIncrement())//
				.append(",\"entityType\":\"").append(dataSource.getEntityTypeName()).append('"')//
				.append(",\"entities\":[");
			boolean first = true;
			Collection<T> initialEntities = dataSource.queryEntities(typeSubscriptions.values().stream()
				.map(sub -> (SyncDataSubscription<U, T>) sub).map(sub -> sub.getFilters(false)).toList());
			for (T entity : initialEntities) {
				boolean include = false;
				for (SyncDataSubscription<U, ?> sub : typeSubscriptions.values()) {
					if (((SyncDataSubscription<U, T>) sub).isIncluded(entity)) {
						include = true;
						break;
					}
				}
				if (include) {
					if (first)
						first = false;
					else
						initialDataJson.append(',');
					initialDataJson.append(dataSource.serialize(entity).toString());
				}
			}
			initialDataJson.append("]}");
			return initialDataJson.toString();
		}

		private <T> Flux<ServerSentEvent<String>> modifySubscriptions(SubscriptionChangeEvent<U, T> event) {
			if (!streamId.equals(event.streamId))
				return Flux.empty();

			String typeName = event.getEntityTypeName();
			List<Map<String, SyncDataSource.SyncDataFilter<T>>> targetFilters = new ArrayList<>();

			StringBuilder debugStr = log.isDebugEnabled() ? new StringBuilder() : null;
			if (debugStr != null)
				debugStr.append("\nProcessing ").append(event);
			synchronized (theSubscriptions) {
				Map<String, SyncDataSubscription<U, ?>> typeSubscriptions = theSubscriptions.computeIfAbsent(typeName,
					_ -> new ConcurrentHashMap<>());

				if (event.subscribe != null) {
					for (SyncDataSubscription<U, ?> sub : event.subscribe)
						typeSubscriptions.put(sub.getSubscriptionId(), sub);
				}
				if (event.unsubscribe != null)
					typeSubscriptions.keySet().removeAll(event.unsubscribe);

				if (typeSubscriptions.isEmpty()) {
					theSubscriptions.remove(typeName);
					typeSubscriptions = null;
					if (debugStr != null)
						debugStr.append("\n\tNo remaining subscriptions");
				} else if (debugStr != null) {
					debugStr.append("\n\t").append(typeSubscriptions.size()).append(" Remaining subscriptions: ")
					.append(typeSubscriptions.values());
				}

				if (event.withInitialData && typeSubscriptions != null) {
					for (SyncDataSubscription<U, ?> sub : typeSubscriptions.values())
						targetFilters.add(((SyncDataSubscription<U, T>) sub).getFilters(true));
				}
			}
			if (debugStr != null)
				log.debug(debugStr.toString());

			List<ServerSentEvent<String>> clientEvents = new ArrayList<>();
			List<String> subIds = event.subscribe == null ? Collections.emptyList()
				: event.subscribe.stream().map(SyncDataSubscription::getSubscriptionId).toList();
			JsonObject clientEventJson = new JsonObject()//
				.with("type", "subscriptionChanged")//
				.with("sequence", theSequence.getAndIncrement())//
				.with("entityType", event.getEntityTypeName())//
				.with("newSubscriptions", subIds)//
				.with("unsubscribed", event.unsubscribe == null ? Collections.emptyList() : BetterList.of(event.unsubscribe))//
				;
			clientEvents.add(ServerSentEvent.<String> builder()//
				.id("")//
				.data(clientEventJson.toString())//
				.build());

			if (event.withInitialData) {
				clientEventJson.clear()//
				.with("type", "reset")//
				.with("sequence", theSequence.getAndIncrement())//
				.with("entityType", typeName);
				if (targetFilters.isEmpty())
					clientEventJson.with("entities", Collections.emptyList());
				else {
					Collection<T> initialEntities = event.source.queryEntities(targetFilters);
					Map<String, SyncDataSubscription<U, ?>> activeSubs = theSubscriptions.get(typeName);
					if (activeSubs == null)
						clientEventJson.with("entities", Collections.emptyList());
					else {
						clientEventJson.with("entities", initialEntities.stream()//
							.filter(entity -> activeSubs.values().stream()
								.anyMatch(sub -> ((SyncDataSubscription<U, T>) sub).isIncluded(entity)))//
							.map(entity -> event.source.serialize(entity))//
							.toList());
					}
				}
				clientEvents.add(ServerSentEvent.<String> builder()//
					.id("")//
					.data(clientEventJson.toString())//
					.build());
			}
			return Flux.fromIterable(clientEvents);
		}

		private <T> Flux<ServerSentEvent<String>> processDataEvent(SyncDataEvent<U, T> event) {
			StringBuilder logMsg = log.isDebugEnabled() ? new StringBuilder() : null;
			if (logMsg != null)
				logMsg.append(streamId, 0, 8).append(" Processing data mod ").append(event);
			List<ServerSentEvent<String>> events = new ArrayList<>(2);
			boolean[] include = new boolean[1];
			Map<String, Set<String>> pendingRemovals = new HashMap<>();
			JsonObject json = new JsonObject();
			theSubscriptions.forEach((entityType, typeSubscriptions) -> {
				typeSubscriptions.forEach((subId, subscription) -> {
					switch (subscription.process(event)) {
					case Ignore:
						return;
					case Include:
						if (logMsg != null)
							logMsg.append("\n\tInclude for ").append(subscription);
						include[0] = true;
						break;
					case Remove:
						if (!SyncDataSource.passesAny(event.getEntity(), //
							IterableUtils.map(typeSubscriptions.values(), sub -> ((SyncDataSubscription<U, T>) sub).getFilters(false)))) {
							if (logMsg != null)
								logMsg.append("\n\tRemove for ").append(subscription);
							include[0] = false;
							events.add(ServerSentEvent.<String> builder()//
								.id("")//
								.data(json.clear()//
									.with("type", "remove")//
									.with("sequence", theSequence.getAndIncrement())//
									.with("entityType", entityType)//
									.with("entity", event.getSource().serialize(event.getEntity()))//
									.toString())//
								.build());
						}
						break;
					case Revoke:
						if (logMsg != null)
							logMsg.append("\n\tRevoke for ").append(subscription);
						events.add(ServerSentEvent.<String> builder()//
							.id(event.getMessageId().toString())//
							.data(json.clear()//
								.with("type", "subscriptionRevoked")//
								.with("sequence", theSequence.getAndIncrement())//
								.with("subscription", subId)//
								.toString())//
							.build());
						pendingRemovals.computeIfAbsent(entityType, _ -> new HashSet<>()).add(subId);
						break;
					}
				});
			});

			for (Map.Entry<String, Set<String>> typeRemovals : pendingRemovals.entrySet())
				revokeSubscriptions(typeRemovals.getKey(), typeRemovals.getValue(), events::add);

			if (include[0]) {
				if (logMsg != null)
					logMsg.append("\n\tSend");
				events.add(ServerSentEvent.<String> builder()//
					.id(event.getMessageId().toString())//
					.data(event.toClientJson(theSequence.getAndIncrement()))//
					.build());
			}
			if (logMsg != null)
				log.debug(logMsg.toString());
			return Flux.fromIterable(events);
		}

		private <T> void revokeSubscriptions(String entityType, Set<String> revocations, Consumer<ServerSentEvent<String>> onRevoke) {
			List<Map<String, SyncDataFilter<T>>> revokedFilters = new ArrayList<>();

			// Extract the revoked filters quickly inside an atomic synchronization block
			synchronized (theSubscriptions) {
				Map<String, SyncDataSubscription<U, ?>> typeSubscriptions = theSubscriptions.get(entityType);
				if (typeSubscriptions == null)
					return;

				for (String subId : revocations) {
					SyncDataSubscription<U, ?> sub = typeSubscriptions.get(subId);
					if (sub != null) {
						revokedFilters.add(((SyncDataSubscription<U, T>) sub).getFilters(false));
					}
				}
			}
			if (revokedFilters.isEmpty())
				return;

			SyncDataSource<U, T> source = (SyncDataSource<U, T>) theDataSources.get(entityType);
			// Query the database safely outside all locks
			Collection<T> potentiallyRevokedEntities = source.queryEntities(revokedFilters);

			synchronized (theSubscriptions) {
				Map<String, SyncDataSubscription<U, ?>> typeSubscriptions = theSubscriptions.get(entityType);

				// Remove the revoked subscriptions from memory atomically
				if (typeSubscriptions != null) {
					typeSubscriptions.keySet().removeAll(revocations);
					if (typeSubscriptions.isEmpty()) {
						theSubscriptions.remove(entityType);
						typeSubscriptions = null;
					}
				}

				// Evaluate visibility safely inside the lock block.
				// No concurrent API call can modify the filters while this loop executes.
				JsonObject json = new JsonObject();
				for (T entity : potentiallyRevokedEntities) {
					boolean stillVisible = false;

					if (typeSubscriptions != null) {
						stillVisible = typeSubscriptions.values().stream()
							.anyMatch(sub -> ((SyncDataSubscription<U, T>) sub).isIncluded(entity));
					}

					if (!stillVisible) {
						onRevoke.accept(ServerSentEvent.<String> builder()//
							.id("")//
							.data(json.clear()//
								.with("type", "remove")//
								.with("sequence", theSequence.getAndIncrement())//
								.with("entityType", entityType)//
								.with("entity", source.serialize(entity))//
								.toString())//
							.build());
					}
				}
			}
		}
	}

	private enum EventResponse {
		Ignore, Include, Remove, Revoke
	}

	private static class SyncDataSubscription<U, T> {
		private final SyncDataSource<U, T> theDataSource;
		private final String theSubscriptionId;
		private final Map<String, SyncDataSource.SyncDataFilter<T>> theFilters;
		private final SyncDataSource.ValidationMaintainer<U> theValidationMaintainer;

		SyncDataSubscription(U me, SyncDataSource<U, T> source, JsonObject jsonFilters) {
			Map<String, SyncDataSource.SyncDataFilter<T>> filters = new LinkedHashMap<>();
			for (Map.Entry<String, Object> filterField : jsonFilters.entrySet()) {
				if (filterField.getKey().equals("subscribeEntityType"))
					continue;
				SyncDataSource.SyncDataFilterType<T> filterType = source.getFilterTypes().get(filterField.getKey());
				if (filterType == null)
					throw new IllegalArgumentException(
						"Unknown filter type " + filterField.getKey() + " for entity type " + source.getEntityTypeName());
				try {
					filters.put(filterField.getKey(), filterType.parseFilter(filterField.getValue()));
				} catch (ParseException e) {
					throw new IllegalArgumentException(
						"Failed to parse filter " + filterField.getKey() + " for entity type " + source.getEntityTypeName(), e);
				}
			}
			theDataSource = source;
			theSubscriptionId = QommonsUtils.getRandomString(100);
			theFilters = Collections.unmodifiableMap(filters);
			theValidationMaintainer = source.validateSubscription(me, theFilters);
		}

		SyncDataSource<U, T> getDataSource() {
			return theDataSource;
		}

		String getSubscriptionId() {
			return theSubscriptionId;
		}

		Map<String, SyncDataSource.SyncDataFilter<T>> getFilters(boolean copy) {
			return copy ? new LinkedHashMap<>(theFilters) : theFilters;
		}

		EventResponse process(SyncDataEvent<U, ?> event) {
			switch (theValidationMaintainer.checkSubscription(event)) {
			case Ignore:
				if (theDataSource != event.getSource())
					return EventResponse.Ignore;
				if (isIncluded((T) event.getEntity()))
					return EventResponse.Include;
				return EventResponse.Ignore;
			case AddEntity:
				return EventResponse.Include;
			case RemoveEntity:
				return EventResponse.Remove;
			case RevokeSubscription:
				return EventResponse.Revoke;
			}
			return EventResponse.Ignore;
		}

		boolean isIncluded(T entity) {
			for (SyncDataSource.SyncDataFilter<T> filter : theFilters.values()) {
				if (!filter.include(entity))
					return false;
			}
			return true;
		}

		@Override
		public String toString() {
			return theSubscriptionId.substring(0, 8) + ":" + theFilters;
		}
	}
}
