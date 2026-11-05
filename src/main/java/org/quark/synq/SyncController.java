package org.quark.synq;

import java.util.Collections;
import java.util.List;

import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * REST controller for synq
 * 
 * @param <U> The type of user object used by the application
 */
@RestController
@RequestMapping("/api/synq")
public class SyncController<U> {
	private final SyncService<U> theSyncService;
	private final SynqUserResolver<U> theUserResolver;

	/**
	 * Creates the sync controller
	 * 
	 * @param syncService The sync service
	 * @param userResolver The resolver for the currently authenticated user
	 */
	public SyncController(SyncService<U> syncService, SynqUserResolver<U> userResolver) {
		theSyncService = syncService;
		theUserResolver = userResolver;
	}

	/**
	 * Initializes a sync stream
	 * 
	 * @param subscriptions Optional configuration for initial data subscriptions
	 * @param lastEventId The optional ID of the most recent message that the client received before being disconnected
	 * @return The event stream
	 */
	@GetMapping(value = "/init", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	public Flux<ServerSentEvent<String>> initStream(//
		@RequestParam(value = "subscriptions", required = false) String subscriptions, //
		@RequestParam(value = "lastEventId", required = false) String lastEventId) {

		return theUserResolver.getCurrentUser()//
			.flatMapMany(user -> theSyncService.initStream(user, lastEventId, subscriptions));
	}

	/**
	 * Modifies an existing sync stream
	 * 
	 * @param request The subscription modification request
	 * @return The IDs of the new subscriptions
	 */
	@PostMapping("/modify")
	public Mono<List<String>> modifySubscriptions(@RequestBody SubscriptionModificationRequest request) {
		if (request.unsubscribe() == null && request.subscribe() == null)
			return Mono.just(Collections.emptyList());

		return theUserResolver.getCurrentUser()//
			.map(user -> theSyncService.modifySubscriptions(user, request.streamId(), request.unsubscribe(), request.subscribe(),
				Boolean.TRUE.equals(request.withInitialData())));
	}

	/**
	 * The body of a subscription modification request
	 * 
	 * @param streamId The ID of the stream to modify
	 * @param unsubscribe A list of IDs of subscriptions to unsubscribe
	 * @param subscribe Configuration for new subscriptions
	 * @param withInitialData Whether to send the full initial data set for any affected entity sets
	 */
	public static record SubscriptionModificationRequest(@NotNull @NotBlank String streamId, List<String> unsubscribe, String subscribe,
		Boolean withInitialData) {}
}
