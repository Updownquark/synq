package org.quark.synq;

import java.util.Collections;
import java.util.List;

import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Flux;

/**
 * REST controller for synq
 * 
 * @param <U> The type of user object used by the application
 */
@RestController
@RequestMapping("/api/synq")
public class SyncController<U> {
	private final SyncService<U> theSyncService;
	private final SyncUserResolver<U> theUserResolver;

	/**
	 * Creates the sync controller
	 * 
	 * @param syncService The sync service
	 * @param userResolver The resolver for the currently authenticated user
	 */
	public SyncController(SyncService<U> syncService, SyncUserResolver<U> userResolver) {
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
		@RequestParam(required = false) String subscriptions, //
		@RequestParam(required = false) String lastEventId) {

		U user = theUserResolver.getCurrentUser();
		return theSyncService.initStream(user, lastEventId, subscriptions);
	}

	/**
	 * Modifies an existing sync stream
	 * 
	 * @param streamId The ID of the stream to modify
	 * @param unsubscribe A list of IDs of subscriptions to unsubscribe
	 * @param subscribe Configuration for new subscriptions
	 * @param withInitialData Whether to send the full initial data set for any affected entity sets
	 * @return The IDs of the new subscriptions
	 */
	@PostMapping("/modify")
	public List<String> modifySubscriptions(//
		@RequestParam(required = true) String streamId, //
		@RequestParam(required = false) List<String> unsubscribe, //
		@RequestParam(required = false) String subscribe, //
		@RequestParam(required = false) Boolean withInitialData) {
		if (unsubscribe == null && subscribe == null)
			return Collections.emptyList();

		U user = theUserResolver.getCurrentUser();
		return theSyncService.modifySubscriptions(user, streamId, unsubscribe, subscribe, Boolean.TRUE.equals(withInitialData));
	}
}
