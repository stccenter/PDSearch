package org.apache.sdap.mudrod.discoveryengine;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.metadata.structure.Metadata;
import org.apache.sdap.mudrod.metadata.structure.PODAACMetadata;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

public class PDRank {

	String[] rankList = {"nasa"};

	public PDRank(ESDriver es, String index, String type) {
		es.createBulkProcessor();
		List<Metadata> metadatas = new ArrayList<>();
		SearchResponse scrollResp = es.getClient().prepareSearch(index).setTypes(type)
				.setQuery(QueryBuilders.matchAllQuery()).setScroll(new TimeValue(60000)).setSize(100).execute()
				.actionGet();

		while (true) {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				String value = "";
				String id = hit.getId();

				Map<String, Object> result = hit.getSource();
				value = (String) result.get("url");
//				System.out.println(value);
				for (String var : rankList) {
					if (value.contains(var)) {
						double ranking = 1;
						UpdateRequest ur = null;
						ur = updateES(ur, es, index, type, id, ranking);
						es.getBulkProcessor().add(ur);
					} else {
						double ranking = 0.0;
						UpdateRequest ur = null;
						ur = updateES(ur, es, index, type, id, ranking);
						es.getBulkProcessor().add(ur);
					}
				}

			}
			scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
					.execute().actionGet();
			if (scrollResp.getHits().getHits().length == 0) {
				break;
			}
		}

		es.destroyBulkProcessor();
	}

	public UpdateRequest updateES(UpdateRequest ur, ESDriver es, String index, String type, String id, double ranking) {
		try {
			ur = new UpdateRequest(index, type, id)
					.doc(jsonBuilder().startObject().field("credibility", ranking).endObject());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ur;
	}

}
