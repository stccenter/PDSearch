/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sdap.mudrod.ssearch;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.sdap.mudrod.discoveryengine.MudrodAbstract;
import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.ssearch.structure.SResult;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Supports ability to performance semantic search with a given query
 */
public class Searcher extends MudrodAbstract implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public Searcher(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  /**
   * Main method of semantic search
   *
   * @param index
   *          index name in Elasticsearch
   * @param type
   *          type name in Elasticsearch
   * @param query
   *          regular query string
   * @param queryOperator
   *          query mode- query, or, and
   * @param rankOption
   *          a keyword used to dertermine the ElasticSearch SortOrder
   * @return a list of search result
   */
  @SuppressWarnings("unchecked")
  public List<SResult> searchByQuery(String index, String type, String query, String queryOperator, String rankOption) {
    boolean exists = es.getClient().admin().indices().prepareExists(index).execute().actionGet().isExists();
    if (!exists) {
      return new ArrayList<>();
    }

    SortOrder order = null;
    String sortFiled = "";
    switch (rankOption) {
    case "Rank-AllTimePopularity":
      sortFiled = "Dataset-AllTimePopularity";
      order = SortOrder.DESC;
      break;
    case "Rank-MonthlyPopularity":
      sortFiled = "Dataset-MonthlyPopularity";
      order = SortOrder.DESC;
      break;
    case "Rank-UserPopularity":
      sortFiled = "Dataset-UserPopularity";
      order = SortOrder.DESC;
      break;
    case "Rank-LongName-Full":
      sortFiled = "Dataset-LongName.raw";
      order = SortOrder.ASC;
      break;
    case "Rank-ShortName-Full":
      sortFiled = "Dataset-ShortName.raw";
      order = SortOrder.ASC;
      break;
    case "Rank-GridSpatialResolution":
      sortFiled = "Dataset-GridSpatialResolution";
      order = SortOrder.DESC;
      break;
    case "Rank-SatelliteSpatialResolution":
      sortFiled = "Dataset-SatelliteSpatialResolution";
      order = SortOrder.DESC;
      break;
    case "Rank-StartTimeLong-Long":
      sortFiled = "DatasetCoverage-StartTimeLong-Long";
      order = SortOrder.ASC;
      break;
    case "Rank-StopTimeLong-Long":
      sortFiled = "DatasetCoverage-StopTimeLong-Long";
      order = SortOrder.DESC;
      break;
    case "Rank-Credibility":
        sortFiled = "ranking";
        order = SortOrder.DESC;
        break;
    case "Rank-Modify-Time":
        sortFiled = "lastModified";
        order = SortOrder.DESC;
        break;
    default:
      sortFiled = props.getProperty(MudrodConstants.RANKING_SEARCH_FIELDS).split(",")[0];
      order = SortOrder.ASC;
      break;
    }

    Dispatcher dp = new Dispatcher(this.getConfig(), this.getES(), null);
    BoolQueryBuilder qb = dp.createSemQuery(query, 1.0, queryOperator);
    List<SResult> resultList = new ArrayList<>();
    SearchRequestBuilder builder = es.getClient().prepareSearch(index).setTypes(type).setQuery(qb).addSort(sortFiled, order).setSize(500).setTrackScores(true);
    SearchResponse response = builder.execute().actionGet();
    
    //System.out.println(builder.toString());

    SResult r = new SResult();
    for (SearchHit hit : response.getHits().getHits()) {
      SResult re = r.makeResult(props.getProperty(MudrodConstants.RANKING_META_FORMAT), hit);
      resultList.add(re);
    }

    return resultList;
  }

  /**
   * Method of semantic search to generate JSON string
   *
   * @param index
   *          index name in Elasticsearch
   * @param type
   *          type name in Elasticsearch
   * @param query
   *          regular query string
   * @param queryOperator
   *          query mode- query, or, and
   * @param rankOption
   *          a keyword used to dertermine the ElasticSearch SortOrder
   * @param rr
   *          selected ranking method
   * @return search results
   */
  public String ssearch(String index, String type, String query, String queryOperator, String rankOption, Ranker rr) {
    List<SResult> li = searchByQuery(index, type, query, queryOperator, rankOption);
    /*if ("Rank-SVM".equals(rankOption)) {
      li = rr.rank(li);
    }*/
    Gson gson = new Gson();
    List<JsonObject> fileList = new ArrayList<>();

    for (SResult aLi : li) {
      JsonObject file = new JsonObject();
      file.addProperty("Short Name", (String) SResult.get(aLi, "shortName"));
      file.addProperty("Long Name", (String) SResult.get(aLi, "longName"));
      file.addProperty("Topic", (String) SResult.get(aLi, "topic"));
      file.addProperty("Description", (String) SResult.get(aLi, "description"));
      file.addProperty("Release Date", (String) SResult.get(aLi, "release_date"));
      file.addProperty("Start/End Date", (String) SResult.get(aLi, "startDate") + " - " + (String) SResult.get(aLi, "endDate"));
      file.addProperty("Processing Level", (String) SResult.get(aLi, "processingLevel"));
      file.addProperty("Sensor", (String) SResult.get(aLi, "sensors"));
      fileList.add(file);
    }
    JsonElement fileListElement = gson.toJsonTree(fileList);

    JsonObject SResults = new JsonObject();
    SResults.add("PDResults", fileListElement);
    return SResults.toString();
  }
}
