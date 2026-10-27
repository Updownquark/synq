package org.quark.synq;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.text.ParseException;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.qommons.json.JsonObject;
import org.quark.synq.SyncService.SyncDataEvent;

import tools.jackson.databind.ObjectMapper;

/**
 * A data source to be installed in a {@link SyncService} via {@link SyncService#installDataSource(SyncDataSource) installDataSource()}.
 * This class handles serialization of entity instances, filtering by subscription, and queries for initial data.
 * 
 * @param <U> The type of user object used by the application
 * @param <T> The type of entity this data source knows how to handle
 */
public interface SyncDataSource<U, T> {
	/**
	 * Parser for a filters on a notional or real field in the entity
	 * 
	 * @param <T> The type of the entity to filter
	 */
	interface SyncDataFilterType<T> {
		/**
		 * Parses a filter from JSON
		 * 
		 * @param jsonFilter The {@link String}, {@link Number}, {@link Boolean}, {@link JsonObject}, or {@link List} thereof to parse the
		 *        filter from
		 * @return The parsed filter
		 * @throws ParseException If the filter could not be parsed from the input
		 */
		public SyncDataSource.SyncDataFilter<T> parseFilter(Object jsonFilter) throws ParseException;
	}

	/**
	 * A filter for entities of a type
	 * 
	 * @param <T> The type of entities to filter
	 */
	interface SyncDataFilter<T> {
		/**
		 * Tests an entity for whether it passes this filter
		 * 
		 * @param entity The entity to test
		 * @return Whether
		 */
		boolean include(T entity);
	}

	/**
	 * Tests an entity against a single filter set
	 * 
	 * @param <T> The type of the entity to test
	 * @param entity The entity to test
	 * @param filters The map of filters to test the entity with
	 * @return Whether the given entity passes all of the filters in the map
	 */
	public static <T> boolean passesAll(T entity, Map<String, SyncDataFilter<T>> filters) {
		return filters.values().stream().allMatch(filter -> filter.include(entity));
	}

	/**
	 * Tests an entity against a set of filter sets for different subscriptions
	 * 
	 * @param <T> The type of the entity to test
	 * @param entity The entity to test
	 * @param filters The set of filter maps to test the entity with
	 * @return Whether the given entity passes all of the filters any single map in the filter set
	 */
	public static <T> boolean passesAny(T entity, Iterable<Map<String, SyncDataFilter<T>>> filters) {
		for (var filter : filters) {
			if (passesAll(entity, filter))
				return true;
		}
		return false;
	}

	/**
	 * A simple {@link SyncDataFilter} implementation that tests the value of an entity's field against a constant value.
	 * 
	 * @param <T> The type of the entity to filter
	 * @param <F> The type of the field value to test with
	 */
	public static class ConstValueFilter<T, F> implements SyncDataFilter<T> {
		private final Function<? super T, ? extends F> theGetter;
		private final F theValue;

		/**
		 * Creates the filter
		 * 
		 * @param getter A getter for the field on the entity
		 * @param value The value to test the field against
		 */
		public ConstValueFilter(Function<? super T, ? extends F> getter, F value) {
			theGetter = getter;
			theValue = value;
		}

		/**
		 * The constant value this filter uses to test entities
		 * 
		 * @return The field value to test against each entity
		 */
		public F getValue() {
			return theValue;
		}

		@Override
		public boolean include(T entity) {
			F value = theGetter.apply(entity);
			return Objects.equals(theValue, value);
		}

		@Override
		public String toString() {
			return String.valueOf(theValue);
		}
	}

	/**
	 * Gets the single value for a field query if the query is against a constant value for that field
	 * 
	 * @param <T> The type of the field
	 * @param filters The map of filters
	 * @param field The name of the field to get the filter value for
	 * @return If the set of filters is against a constant value for the given field, the filter value. Otherwise null.
	 */
	public static <T> T getConstantQueryBy(Map<String, ? extends SyncDataFilter<?>> filters, String field) {
		SyncDataFilter<?> filter = filters.get(field);
		if (!(filter instanceof ConstValueFilter))
			return null;
		return ((ConstValueFilter<?, T>) filter).getValue();
	}

	/** Possible results of checking a subscription against a data change event */
	public enum ValidationChange {
		/** The data change does not affect the status of the subscription */
		Ignore,
		/** The event's entity may possibly have been invisible to the subscription previously, but now it is. */
		AddEntity,
		/**
		 * The event's entity may possibly have been visible to the subscription previously, but now is not.<br>
		 * This type should be avoided if possible when the entity was not previously visible to the subscription.
		 */
		RemoveEntity,
		/**
		 * The event carries information that means the user may no longer have access to all entities matching the subscription's filter(s)
		 */
		RevokeSubscription;
	}

	/**
	 * Maintains a subscription to an entity set. This interface watches every event to every entity in every entity set, determining if the
	 * event is relevant to the state of the subscription. This interface is not responsible for determining whether an event for the
	 * subscription's type is included in the subscription--just whether the event changes the set of entities available to the
	 * subscription.
	 * 
	 * @param <U> The type of user object used by the application
	 */
	public interface ValidationMaintainer<U> {
		/**
		 * Checks an event to see if it affects the status of this maintainer's subscription
		 * 
		 * @param event The event to check
		 * @return Whether the event affects the subscription
		 */
		ValidationChange checkSubscription(SyncDataEvent<U, ?> event);
	}

	/**
	 * Gets the entity type name of the entity managed by this data source
	 * 
	 * @return The name of this data source's entity type
	 */
	String getEntityTypeName();

	/**
	 * Gets the run time type of the entity managed by this data source
	 * 
	 * @return The entity type that this data source handles
	 */
	Class<T> getEntityType();

	/**
	 * Serializes an entity
	 * 
	 * @param entity The entity to serialize
	 * @return A {@link String}, {@link Number}, {@link Boolean}, {@link JsonObject}, or {@link List} thereof representing the entity
	 */
	Object serialize(T entity);

	/**
	 * Deserializes an entity
	 * 
	 * @param json The JSON text to deserialize the entity from
	 * @return The deserialized entity
	 * @throws ParseException If the entity could not be parsed from the input
	 */
	T deserialize(String json) throws ParseException;

	/**
	 * Gets the set of filter types supported by this data source
	 * 
	 * @return A map of filter types by field name. The field names do not need to correspond to anything directly on the entity type, they
	 *         may be notional, corresponding only to a type of query.
	 */
	Map<String, SyncDataSource.SyncDataFilterType<T>> getFilterTypes();

	/**
	 * Creates a validation maintainer for a user's subscription
	 * 
	 * @param user The user requesting the subscription
	 * @param filters The filters requested by the user, by type name
	 * @return A function to be called for every entity event that occurs in the service to ensure the subscription remains valid.<br>
	 *         When an event does not invalidate this subscription, <code>null</code> should be returned.<br>
	 *         If an event occurs which means the user no longer has access to the data for this entity source and the given filter, the
	 *         function should return a list of the entities that the user no longer has access to.<br>
	 *         These entities will be sent as removals to the client along with the notification that the subscription has been cancelled.
	 * @throws UnsupportedOperationException If the user does not have access to the data from this data source with the given filters.
	 */
	ValidationMaintainer<U> validateSubscription(U user, Map<String, SyncDataSource.SyncDataFilter<T>> filters)
		throws UnsupportedOperationException;

	/**
	 * Gets the initial set of entities currently in the data source matching any of a list of filter sets for a set of subscriptions to
	 * this data source's entity set
	 * 
	 * @param filters The filters to apply
	 * @return All entities in the data set which match any of the given filter combos.<br>
	 *         <b>THIS METHOD MUST NOT FILTER BASED ON PERMISSIONS or any other criterion other than the filters parameter.</b><br>
	 *         The opportunity for permission checks happens in {@link #validateSubscription(Object, Map)}.
	 */
	Collection<T> queryEntities(List<Map<String, SyncDataSource.SyncDataFilter<T>>> filters);

	/**
	 * Reflection-based partial {@link SyncDataSource} implementation
	 * 
	 * @param <U> The type of user object used by the application
	 * @param <T> The type of entity this data source knows how to handle
	 */
	public static abstract class AbstractReflectedDataSource<U, T> implements SyncDataSource<U, T> {
		private final String theEntityTypeName;
		private final Class<T> theEntityType;
		private final ObjectMapper theObjectMapper;
		private final Map<String, SyncDataFilterType<T>> theFilterTypes;

		/**
		 * Creates the data source
		 * 
		 * @param entityTypeName The type name of this data source's entity
		 * @param entityType The type of entity this data source can handle
		 * @param objectMapper The Jackson object mapper for (de)serialization
		 */
		public AbstractReflectedDataSource(String entityTypeName, Class<T> entityType, ObjectMapper objectMapper) {
			theEntityTypeName = entityTypeName;
			theEntityType = entityType;
			theObjectMapper = objectMapper;

			Field[] fields = entityType.getDeclaredFields();
			// Discover fields for filtering
			Map<String, SyncDataFilterType<T>> filterTypes = new LinkedHashMap<>();
			for (Field field : fields) {
				if (filterTypes.containsKey(field.getName()))
					continue;
				filterTypes.put(field.getName(), createFilterType(field, theObjectMapper));
			}
			theFilterTypes = Collections.unmodifiableMap(filterTypes);
		}

		private static Field getIdField(Field[] fields) {
			Field idField = findField(fields, f -> hasAnnotation(f, "jakarta.persistence.Id", "org.springframework.data.annotation.Id"));
			if (idField == null)
				idField = findField(fields, f -> f.getName().equals("id"));
			return idField;
		}

		private static Field findField(Field[] fields, Predicate<Field> filter) {
			for (Field f : fields)
				if (filter.test(f))
					return f;
			return null;
		}

		private static boolean hasAnnotation(Field field, String... annotationNames) {
			for (Annotation annotation : field.getAnnotations()) {
				String typeName = annotation.getClass().getName();
				for (String annName : annotationNames) {
					if (typeName.equals(annName))
						return true;
				}
			}
			return false;
		}

		private static <T, F> SyncDataFilterType<T> createFilterType(Field field, ObjectMapper objectMapper) {
			Class<?> type = field.getType();
			boolean primitive = type.isPrimitive() || type == String.class || Number.class.isAssignableFrom(type) || type == Boolean.class;
			Field idField = primitive ? null : getIdField(type.getDeclaredFields());
			if (!primitive && idField == null)
				return null;
			field.setAccessible(true);
			if (idField != null)
				idField.setAccessible(true);

			Function<T, ?> simpleGetter = entity -> {
				try {
					return field.get(entity);
				} catch (IllegalArgumentException | IllegalAccessException e) {
					throw new IllegalStateException("Could not retrieve value of field " + field + " for serialization");
				}
			};
			Function<T, F> getter;
			Class<F> fieldType;
			if (primitive) {
				getter = (Function<T, F>) simpleGetter;
				fieldType = (Class<F>) type;
			} else {
				getter = entity -> {
					Object fieldValue = simpleGetter.apply(entity);
					if (fieldValue == null)
						return null;
					try {
						return (F) idField.get(fieldValue);
					} catch (IllegalArgumentException | IllegalAccessException e) {
						throw new IllegalStateException("Could not retrieve value of field " + idField + " for serialization");
					}
				};
				fieldType = (Class<F>) idField.getType();
			}
			return jsonFilterValue -> {
				try {
					Object coercedValue = objectMapper.convertValue(jsonFilterValue, fieldType);
					return new ConstValueFilter<>(getter, coercedValue);
				} catch (RuntimeException e) {
					ParseException x = new ParseException(
						"Failed to parse " + type.getSimpleName() + "." + field.getName() + " (" + fieldType + "): " + e.getMessage(), 0);
					x.addSuppressed(e);
					throw x;
				}
			};
		}

		@Override
		public String getEntityTypeName() {
			return theEntityTypeName;
		}

		@Override
		public Class<T> getEntityType() {
			return theEntityType;
		}

		@Override
		public Object serialize(T entity) {
			String jsonStr = theObjectMapper.writeValueAsString(entity);
			Map<String, Object> map = theObjectMapper.readValue(jsonStr, Map.class);
			return new JsonObject().withAll(map);
		}

		@Override
		public T deserialize(String json) throws ParseException {
			try {
				return theObjectMapper.readValue(json, theEntityType);
			} catch (RuntimeException e) {
				ParseException x = new ParseException("Could not parse " + theEntityTypeName + " JSON", 0);
				x.addSuppressed(e);
				throw x;
			}
		}

		@Override
		public Map<String, SyncDataFilterType<T>> getFilterTypes() {
			return theFilterTypes;
		}
	}

	/**
	 * A simple data source for the use case where a user's access to an entity can be determined from the value of a single field on the
	 * entity.
	 * 
	 * @param <U> The type of user object used by the application
	 * @param <T> The type of entity this data source knows how to handle
	 * @param <ID> The type of the field by which access is determined
	 */
	public static abstract class SingleFieldValidationDataSource<U, T, ID> extends AbstractReflectedDataSource<U, T> {
		private final String theAuthFieldName;

		/**
		 * Creates the data source
		 * 
		 * @param entityTypeName The type name of this data source's entity
		 * @param entityType The type of entity this data source can handle
		 * @param objectMapper The Jackson object mapper for (de)serialization
		 * @param authFieldName The name of the field by which access id determined
		 */
		public SingleFieldValidationDataSource(String entityTypeName, Class<T> entityType, ObjectMapper objectMapper,
			String authFieldName) {
			super(entityTypeName, entityType, objectMapper);
			theAuthFieldName = authFieldName;
		}

		/**
		 * Tests a user's ability to make a query for entities with a given authorization field value
		 * 
		 * @param user The user object to determine access for
		 * @param authFieldValue The authorization field of the entity to test
		 * @return Null if the user should be able to see entities with the given authorization ID, or a human-readable description for why
		 *         this is not allowed
		 */
		protected abstract String isAuthorized(U user, ID authFieldValue);

		/**
		 * Creates the validataion maintainer for a subscription to entities with a given authorization value, already authorized with
		 * {@link #isAuthorized(Object, Object)}
		 * 
		 * @param user The user subscribing to the entities
		 * @param authFieldValue The authorization field value by which the user is subscribing
		 * @return A validation maintainer
		 */
		protected abstract ValidationMaintainer<U> maintainValidation(U user, ID authFieldValue);

		/**
		 * Queries for all entities with a given authorization field value
		 * 
		 * @param authFieldValue The authorization field value to get entities for
		 * @return All entities in the data store with the given value for the authorization field
		 */
		protected abstract Stream<T> getEntities(ID authFieldValue);

		@Override
		public ValidationMaintainer<U> validateSubscription(U user, Map<String, SyncDataFilter<T>> filters)
			throws UnsupportedOperationException {
			SyncDataFilter<T> orgFilter = filters.get(theAuthFieldName);
			if (orgFilter == null)
				throw new UnsupportedOperationException("Query requires a filter for " + theAuthFieldName);
			else if (!(orgFilter instanceof SyncDataSource.ConstValueFilter))
				throw new UnsupportedOperationException(theAuthFieldName + " filter must be a constant value");
			ID authFieldValue = ((SyncDataSource.ConstValueFilter<T, ID>) orgFilter).getValue();
			String authFailed = isAuthorized(user, authFieldValue);
			if (authFailed != null)
				throw new NoSuchElementException(authFailed);
			return maintainValidation(user, authFieldValue);
		}

		@Override
		public List<T> queryEntities(List<Map<String, SyncDataFilter<T>>> filters) {
			// The filters should have already passed validation above
			if (filters.size() == 1) {
				ID authFieldValue = ((SyncDataSource.ConstValueFilter<T, ID>) filters.get(0).get(theAuthFieldName)).getValue();
				return getEntities(authFieldValue)//
					.filter(job -> SyncDataSource.passesAll(job, filters.get(0)))//
					.toList();
			} else {
				Map<ID, List<Map<String, SyncDataFilter<T>>>> filtersByAuth = new HashMap<>();
				for (Map<String, SyncDataFilter<T>> filter : filters) {
					ID authFieldValue = ((SyncDataSource.ConstValueFilter<T, ID>) filters.get(0).get(theAuthFieldName)).getValue();
					var valueFilters = filtersByAuth.computeIfAbsent(authFieldValue, _ -> new ArrayList<>());
					if (filter.size() > 1)
						valueFilters.add(filter);
				}
				List<T> entities = new ArrayList<>();
				for (Map.Entry<ID, List<Map<String, SyncDataFilter<T>>>> authValue : filtersByAuth.entrySet()) {
					Stream<T> authValues = getEntities(authValue.getKey());
					if (!authValue.getValue().isEmpty())
						authValues = authValues.filter(job -> SyncDataSource.passesAny(job, authValue.getValue()));
					authValues.forEach(entities::add);
				}
				return entities;
			}
		}
	}
}