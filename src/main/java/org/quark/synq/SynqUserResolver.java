package org.quark.synq;

import reactor.core.publisher.Mono;

/**
 * Resolves the currently authentication user for a controller call. This interface allows applications using Synq to use their own user
 * objects for authentication and querying.
 * 
 * @param <U> The type of the user object used by the application
 */
public interface SynqUserResolver<U> {
	/**
	 * Determines the user that is currently calling a Synq controller end point
	 * 
	 * @return The user that is calling the endpoint currently
	 */
	Mono<U> getCurrentUser();
}
