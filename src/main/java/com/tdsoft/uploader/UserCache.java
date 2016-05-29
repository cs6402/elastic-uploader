package com.tdsoft.uploader;

import java.time.Instant;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.stereotype.Service;

import com.tdsoft.uploader.entity.User;
import com.tdsoft.uploader.util.JsonUtils;

@Service("userCache")
public class UserCache extends AbstractCacheService {

	private static final Logger logger = LoggerFactory.getLogger(UserCache.class);

	@Resource(name = "redisClient")
	private RedisTemplate<String, String> redisClient;

	@Value("${user_content_key}")
	private String userContentKey;

	@Value("${user_temp_key}")
	private String userTempKey;

	@Value("${retrieve_page_count}")
	private int count;

	public void putUserToContentAndUploadList(User bean) {
		String json = JsonUtils.convertObjectToJson(bean);
		String userId = appendPrefix(userContentKey, bean.getId());
		redisClient.opsForValue().set(userId, json);
		redisClient.opsForZSet().add(userTempKey, bean.getId(), Instant.now().getEpochSecond());
	}

	public List<SimpleEntry<Double, String>> getUsersInUploadList(double timestamp, Long page) {
		// get UserId from UploadList by current timestamp and page 
		Set<TypedTuple<String>> userIdSet =
				redisClient.opsForZSet().rangeByScoreWithScores(userTempKey, 0, timestamp, (page - 1) * count, count);
		// empty
		if (userIdSet.isEmpty()) {
			return Collections.emptyList();
		}
		
		Iterator<TypedTuple<String>> iterator = userIdSet.iterator();
		List<SimpleEntry<Double, String>> contentWithLatestScore = new ArrayList<SimpleEntry<Double, String>>(userIdSet.size());
		while (iterator.hasNext()) {
			TypedTuple<String> userContentWithScore = iterator.next();
			SimpleEntry<Double, String> entry = new SimpleEntry<Double, String>(userContentWithScore.getScore(), userContentWithScore.getValue());
			contentWithLatestScore.add(entry);
		}

		return contentWithLatestScore;
	}

	public void removeUsersInUploadList(double timestamp) {
		redisClient.opsForZSet().removeRangeByScore(userTempKey, 0, timestamp);
	}

	public User getUser(String userId) {
		userId = appendPrefix(userContentKey, userId);
		String userBeanContent = redisClient.opsForValue().get(userId);
		User bean = JsonUtils.convertJsonToObject(userBeanContent, User.class);
		return bean;
	}
}
