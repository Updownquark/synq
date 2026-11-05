package org.quark.synq;

/**
 * Gets the identity of a user
 * 
 * @param <U> The type of user used with Synq
 */
public interface SynqUserIdentifier<U> {
	/**
	 * Gets a user's ID
	 * 
	 * @param user The user that caused a change
	 * @return The user's identifier
	 */
	Object getUserId(U user);
}
