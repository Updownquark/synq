package org.quark.synq;

/** Internal event used by {@link EntityMutationNotificationService} */
class EntityMutationEvent {
	private final MessageId theId;
	private final String theEntityTypeName;
	private final boolean isPresent;
	private final String theEntityJson;

	/**
	 * @param id
	 * @param entityTypeName
	 * @param isPresent
	 * @param entityJson
	 */
	public EntityMutationEvent(MessageId id, String entityTypeName, boolean isPresent, String entityJson) {
		theId = id;
		theEntityTypeName = entityTypeName;
		this.isPresent = isPresent;
		theEntityJson = entityJson;
	}

	public MessageId getId() {
		return theId;
	}

	public String getEntityTypeName() {
		return theEntityTypeName;
	}

	public boolean isPresent() {
		return isPresent;
	}

	public String getEntityJson() {
		return theEntityJson;
	}

	@Override
	public String toString() {
		return theEntityTypeName + " " + (isPresent ? "addOrUpdate" : "remove") + " " + theEntityJson;
	}
}
