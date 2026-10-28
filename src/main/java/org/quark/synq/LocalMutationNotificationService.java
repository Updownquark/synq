package org.quark.synq;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.qommons.ArrayUtils;
import org.qommons.collect.BetterList;
import org.qommons.collect.CircularArrayList;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A {@link EntityMutationNotificationService} implementation that utilizes a local cache of events. This is a high-performance,
 * low-overhead implementation for single-instance applications.
 */
public class LocalMutationNotificationService extends EntityMutationNotificationService {
	private final CircularArrayList<EntityMutationEvent> theEventCache;

	/**
	 * Creates the event service
	 * 
	 * @param maxEvents The maximum number of events to keep in the cache. Affects the ability of clients to be updated after a temporary
	 *        connection loss without needing to completely repopulate the data set.
	 */
	public LocalMutationNotificationService(int maxEvents) {
		theEventCache = CircularArrayList.build()//
			.withMaxCapacity(maxEvents)//
			.build();
	}

	@Override
	protected synchronized Mono<MessageId> publishGlobalMutation(String entityType, boolean present, String entityJson) {
		MessageId messageId = MessageId.generate();
		EntityMutationEvent event = new EntityMutationEvent(messageId, entityType, present, entityJson);
		theEventCache.add(event);
		publishLocalMutation(event);
		return Mono.just(messageId);
	}

	@Override
	Mono<Optional<MessageId>> getLatestMutation() {
		return Mono.fromCallable(() -> {
			synchronized (this) {
				EntityMutationEvent message = theEventCache.peekLast();
				return message == null ? Optional.empty() : Optional.of(message.getId());
			}
		});
	}

	@Override
	Mono<Optional<MessageId>> getOldestCachedMutation() {
		return Mono.fromCallable(() -> {
			synchronized (this) {
				EntityMutationEvent message = theEventCache.peekFirst();
				return message == null ? Optional.empty() : Optional.of(message.getId());
			}
		});
	}

	@Override
	Flux<EntityMutationEvent> getCachedMutations(MessageId after) {
		return Flux.defer(() -> {
			synchronized (this) {
				if (after == null)
					return Flux.fromIterable(BetterList.of(theEventCache)); // Immutable copy
				int index = ArrayUtils.binarySearch(theEventCache, evt -> after.compareTo(evt.getId()));
				if (index < 0)
					index = -index - 1;
				else
					index++;
				List<EntityMutationEvent> rangeCopy = new ArrayList<>(theEventCache.size() - index);
				rangeCopy.addAll(theEventCache.subList(index, theEventCache.size()));
				return Flux.fromStream(rangeCopy.stream());
			}
		});
	}
}
