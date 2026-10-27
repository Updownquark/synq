package org.quark.synq;

/**
 * Resolves the currently authentication user for a controller call and gets the ID of a user. This interface allows applications using Synq
 * to use their own user objects for authentication and querying.
 * 
 * @param <U> The type of the user object used by the application
 */
public interface SyncUserResolver<U> {
	/**
	 * Determines the user that is currently calling a Synq controller end point
	 * 
	 * @return The user that is calling the endpoint currently
	 */
	U getCurrentUser();

	/**
	 * Gets a user's ID
	 * 
	 * @param user The user that caused a change
	 * @return The user's identifier
	 */
	Object getUserId(U user);
}
