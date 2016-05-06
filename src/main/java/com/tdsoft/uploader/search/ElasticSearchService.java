package com.tdsoft.uploader.search;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import javax.annotation.PostConstruct;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.tdsoft.uploader.entity.User;
import com.tdsoft.uploader.util.JsonUtils;
@Service("elasticSearchService")
public class ElasticSearchService implements SearchService {

	@Value("${search_service_hostname}")
	private String hostname = "localhost";
	@Value("${search_service_port}")
	private int port = 9300;

	private Client client;
	private static final String INDEX = "application";
	private static final String TYPE = "user";

	private static final Logger logger = LoggerFactory.getLogger(ElasticSearchService.class);


	@PostConstruct
	private void init() throws UnknownHostException {
		client = TransportClient.builder().build()
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostname), port));
		boolean indexExists = client.admin().indices().prepareExists(INDEX).execute().actionGet().isExists();
		if (!indexExists) {
			client.admin().indices().prepareCreate(INDEX).execute().actionGet();
			throw new RuntimeException("Not found index in elasticsearch! Please check!");
		}
	}

	public void postUsers(byte[] documents, List<String> allDoc) {
		BulkRequestBuilder bulkBuilder = client.prepareBulk();
		for (String userContent : allDoc) {
			User user = JsonUtils.convertJsonToObject(userContent, User.class);

			IndexRequestBuilder idxRequest = client.prepareIndex(INDEX, TYPE, user.getId()).setSource(userContent);
			bulkBuilder.add(idxRequest);
		}
		BulkResponse response = bulkBuilder.execute().actionGet();
		if (response.hasFailures()) {
			logger.error("Post Tag Error Respone : {}", response.buildFailureMessage());
		}
	}

}
