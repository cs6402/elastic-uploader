package com.tdsoft.uploader.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.MapType;

/**
 * JsonUtil.
 * 
 * @author Daniel
 */
public final class JsonUtils {
	private static final ObjectMapper OM = new ObjectMapper();

	private JsonUtils() {}


	/**
	 * convertObjectToJson.
	 * 
	 * @param obj Object
	 * @return String
	 * @throws IOException if has error
	 */
	public static String convertObjectToJson(final Object obj) {
		try {
			return OM.writeValueAsString(obj);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * convertJsonToObject.
	 * 
	 * @param json String
	 * @param clazz Class<T>
	 * @param <T> object
	 * @return <T>
	 * @throws IOException if has error
	 */
	public static <T> T convertJsonToObject(final String json, final Class<T> clazz) {
		try {
			return OM.readValue(json, clazz);
		} catch (JsonParseException e) {
			throw new RuntimeException(e);
		} catch (JsonMappingException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


	/**
	 * convertJsonNodeToObject.
	 * 
	 * @param json JsonNode
	 * @param clazz Class<T>
	 * @param <T> object
	 * @return T
	 * @throws IOException if has error
	 */
	// public static <T> T convertJsonNodeToObject(final JsonNode json, final Class<T> clazz)
	// throws IOException {
	// return OM.readValues(json, clazz);
	// }

	/**
	 * convertJsonToMap.
	 * 
	 * @param json String
	 * @param keyClass Class<V>
	 * @param valueClass Class<K>
	 * @param <V> key
	 * @param <K> value
	 * @return Map<V, K>
	 * @throws IOException if has error
	 */
	public static <V, K> Map<V, K> convertJsonToMap(final String json, final Class<V> keyClass,
			final Class<K> valueClass) {
		MapType mapType = OM.getTypeFactory().constructMapType(Map.class, keyClass, valueClass);
		try {
			return OM.readValue(json, mapType);
		} catch (JsonParseException e) {
			throw new RuntimeException(e);
		} catch (JsonMappingException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * convertJsonToList.
	 * 
	 * @param json String
	 * @param elementClass Class<T>
	 * @param <T> element
	 * @return List<T>
	 * @throws IOException if has error
	 */
	public static <T> List<T> convertJsonToList(final String json, final Class<T> elementClass) {
		CollectionType collectionTyoe = OM.getTypeFactory().constructCollectionType(List.class, elementClass);
		try {
			return OM.readValue(json, collectionTyoe);
		} catch (JsonParseException e) {
			throw new RuntimeException(e);
		} catch (JsonMappingException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * readJsonNode.
	 * 
	 * @param json String
	 * @return String
	 * @throws IOException if has error
	 */
	public static JsonNode readJsonNode(final String json) {
		try {
			return OM.readTree(json);
		} catch (JsonParseException e) {
			throw new RuntimeException(e);
		} catch (JsonMappingException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static ObjectMapper getObjectMapper() {
		 return OM;
	 }
}
