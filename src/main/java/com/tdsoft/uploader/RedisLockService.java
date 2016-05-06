package com.tdsoft.uploader;

import java.time.Instant;
import java.util.Optional;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

public class RedisLockService {
	private static final Logger logger = LoggerFactory.getLogger(RedisLockService.class);

	@Resource(name = "redisClient")
	private RedisTemplate<String, String> redisClient;

	@Value("${lock_timeout}")
	private long lockTimeout;

	@Value("${lock_key}")
	private String lockKey;

	@Value("${time_for_next_acquire}")
	private long timeForNextAcquire;

	public String acquireLock() {
		ValueOperations<String, String> opsForValue = redisClient.opsForValue();
		while (true) {
			String tokenValue = String.valueOf(Instant.now().getEpochSecond() + lockTimeout + 1l);
			Boolean isSuccessful = opsForValue.setIfAbsent(lockKey, tokenValue);
			if (isSuccessful) {
				return tokenValue;
			} else {
				String currentTokenMillis = opsForValue.get(lockKey);
				if (Optional.ofNullable(currentTokenMillis).isPresent()
						&& Long.parseLong(currentTokenMillis) < Instant.now().getEpochSecond()) {
					String newToken = String.valueOf(Instant.now().getEpochSecond() + lockTimeout + 1l);
					String oldToken = opsForValue.getAndSet(lockKey, newToken);
					if (oldToken.equals(currentTokenMillis)) {
						logger.info("Got lock!{}", newToken);
						return newToken;
					}
				}
			}
			try {
				Thread.sleep(timeForNextAcquire);
			} catch (InterruptedException e) {
				logger.error("Waiting for retrieving lock failed!", e);
			}
			logger.debug("Trying get lock");
		}
	}

	public void releaseLock() {
		redisClient.opsForValue().getOperations().delete(lockKey);
	}
}
