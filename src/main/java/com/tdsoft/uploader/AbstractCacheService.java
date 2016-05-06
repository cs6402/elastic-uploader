package com.tdsoft.uploader;

public abstract class AbstractCacheService {
	/**
	 * 加上key前綴
	 * 
	 * @param key 愈加上前綴
	 * @return 已加完key
	 */
	protected String appendPrefix(String prefix, String key) {
		StringBuilder sb = new StringBuilder();
		sb.append(prefix).append(key);
		return sb.toString();
	}
	
}
