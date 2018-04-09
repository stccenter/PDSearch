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
package org.apache.sdap.mudrod.ssearch.structure;

import org.elasticsearch.search.SearchHit;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class Podaac extends SResult {
  DecimalFormat NDForm = new DecimalFormat("#.##");
  SimpleDateFormat df2 = new SimpleDateFormat("MM/dd/yyyy");
  final Integer MAX_CHAR = 700;

  public Podaac(SResult sr) {
    super(sr);
  }

  public Podaac(SearchHit hit) {
    Map<String, Object> result = hit.getSource();
    this.term = Double.valueOf(NDForm.format(hit.getScore()));
    this.shortName = (String) result.get("Dataset-ShortName");
    this.longName = (String) result.get("Dataset-LongName");
    this.topic = getTopic(result);
    this.description = getDescription(result);
    this.releaseDate = getReleaseD(result);
    this.startDate = getStartD(result);
    this.endDate = getEndD(result);
    
    // string format
    this.processingLevel = (String) result.get("Dataset-ProcessingLevel");
    // numeric format
    this.processingL = getProLevel(processingLevel);
    this.userPop = getNormPopularity(((Integer) result.get("Dataset-UserPopularity")).doubleValue());
    this.allPop = getNormPopularity(((Integer) result.get("Dataset-AllTimePopularity")).doubleValue());
    this.monthPop = getNormPopularity(((Integer) result.get("Dataset-MonthlyPopularity")).doubleValue());
    this.sensors = getSensors(result);
  }

  public Double getProLevel(String pro) {
    if (pro == null) {
      return 1.0;
    }
    Double proNum;
    Pattern p = Pattern.compile(".*[a-zA-Z].*");
    if (pro.matches("[0-9]{1}[a-zA-Z]{1}")) {
      proNum = Double.parseDouble(pro.substring(0, 1));
    } else if (p.matcher(pro).find()) {
      proNum = 1.0;
    } else {
      proNum = Double.parseDouble(pro);
    }

    return proNum;
  }

  public Double getNormPopularity(Double pop) {
    if (pop > 1000) {
      pop = 1000.0;
    }
    return pop;
  }
  
  public String getTopic(Map<String, Object> result) {
    @SuppressWarnings("unchecked")
    ArrayList<String> topicList = (ArrayList<String>) result.get("DatasetParameter-Variable");
    String topic = "";
    if (null != topicList) {
      topic = String.join(", ", topicList);
    }
    return topic;
  }
  
  public String getDescription(Map<String, Object> result) {
    String content = (String) result.get("Dataset-Description");
    if (!"".equals(content)) {
      int maxLength = (content.length() < MAX_CHAR) ? content.length() : MAX_CHAR;
      content = content.trim().substring(0, maxLength - 1) + "...";
    }
    return content;
  }
  
  public Double getReleaseD(Map<String, Object> result){
    @SuppressWarnings("unchecked")
    ArrayList<String> longdate = (ArrayList<String>) result.get("DatasetCitation-ReleaseDateLong");
    Date date = new Date(Long.valueOf(longdate.get(0)));
    this.release_date = df2.format(date);
    return Long.valueOf(longdate.get(0)).doubleValue();
  }
  
  public String getStartD(Map<String, Object> result){
    Long start = (Long) result.get("DatasetCoverage-StartTimeLong-Long");
    Date startDate = new Date(start);
    return df2.format(startDate);
  }
  
  public String getEndD(Map<String, Object> result){
    String end = (String) result.get("Dataset-DatasetCoverage-StopTimeLong");
    String endDateTxt = "";
    if ("".equals(end)) {
      endDateTxt = "Present";
    } else {
      Date endDate = new Date(Long.valueOf(end));
      endDateTxt = df2.format(endDate);
    }
    return endDateTxt;
  }
  
  public String getSensors(Map<String, Object> result){
    @SuppressWarnings("unchecked")
    List<String> sensors = (List<String>) result.get("DatasetSource-Sensor-ShortName");
    return String.join(", ", sensors);
  }
}
