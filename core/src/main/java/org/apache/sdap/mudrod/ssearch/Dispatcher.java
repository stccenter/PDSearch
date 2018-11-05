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

import org.apache.sdap.mudrod.discoveryengine.MudrodAbstract;
import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.integration.LinkageIntegration;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Supports ability to transform regular user query into a semantic query
 */
public class Dispatcher extends MudrodAbstract {

  public Dispatcher(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  /**
   * Method of getting semantically most related terms by number
   *
   * @param input regular input query
   * @param num   the number of most related terms
   * @return a map from term to similarity
   */
  public Map<String, Double> getRelatedTerms(String input, int num) {
    LinkageIntegration li = new LinkageIntegration(props, this.es, null);
    Map<String, Double> sortedMap = li.appyMajorRule(input);
    Map<String, Double> selected_Map = new HashMap<>();
    int count = 0;
    for (Entry<String, Double> entry : sortedMap.entrySet()) {
      if (count < num) {
        selected_Map.put(entry.getKey(), entry.getValue());
      }
      count++;
    }
    return selected_Map;
  }

  /**
   * Method of getting semantically most related terms by similarity threshold
   *
   * @param input regular input query
   * @param T     value of threshold, raning from 0 to 1
   * @return a map from term to similarity
   */
  public Map<String, Double> getRelatedTermsByT(String input, double T) {
    LinkageIntegration li = new LinkageIntegration(this.props, this.es, null);
    Map<String, Double> sortedMap = li.appyMajorRule(input);
    Map<String, Double> selected_Map = new HashMap<>();
    
    if(sortedMap==null)
    {
      selected_Map.put(input, 1.0);
      return selected_Map;
    }

    for (Entry<String, Double> entry : sortedMap.entrySet()) {
      if (entry.getValue() >= T) {
        selected_Map.put(entry.getKey(), entry.getValue());
      }
    }
    return selected_Map;
  }

  /**
   * Method of creating semantic query based on Threshold
   *
   * @param input          regular query
   * @param T              threshold raning from 0 to 1
   * @param query_operator query mode
   * @return a multiMatch query builder
   */
  public BoolQueryBuilder createSemQuery(String input, double T, String query_operator) {
    Map<String, Double> selected_Map = getRelatedTermsByT(input, T);
    selected_Map.put(input, (double) 1);

    String fieldsList[] = props.getProperty(MudrodConstants.RANKING_SEARCH_FIELDS).split(",");
    
    BoolQueryBuilder qb = new BoolQueryBuilder();
    for (Entry<String, Double> entry : selected_Map.entrySet()) {
      if (query_operator.toLowerCase().trim().equals("phrase")) {
        qb.should(QueryBuilders.multiMatchQuery(entry.getKey(), fieldsList).boost(entry.getValue().floatValue()).type(MultiMatchQueryBuilder.Type.PHRASE).tieBreaker((float) 0.5));
      } else if (query_operator.toLowerCase().trim().equals("and")) {
        qb.should(QueryBuilders.multiMatchQuery(entry.getKey(), fieldsList).boost(entry.getValue().floatValue()).operator(MatchQueryBuilder.DEFAULT_OPERATOR.AND).tieBreaker((float) 0.5));
      } else {
        qb.should(QueryBuilders.multiMatchQuery(entry.getKey(), fieldsList).boost(entry.getValue().floatValue()).operator(MatchQueryBuilder.DEFAULT_OPERATOR.OR).tieBreaker((float) 0.5));
      }
    }
    
    qb.filter(QueryBuilders.termQuery("lang", "en"));

    return qb;
  }

}
