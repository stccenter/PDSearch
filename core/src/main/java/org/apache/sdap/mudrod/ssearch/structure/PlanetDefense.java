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

public class PlanetDefense extends SResult {
  DecimalFormat NDForm = new DecimalFormat("#.##");
  SimpleDateFormat df2 = new SimpleDateFormat("MM/dd/yyyy");
  final Integer MAX_CHAR = 700;

  public PlanetDefense(SResult sr) {
    super(sr);
  }

  public PlanetDefense(SearchHit hit) {
    Map<String, Object> result = hit.getSource();
    this.term = Double.valueOf(NDForm.format(hit.getScore()));
    this.shortName = "";
    this.longName = (String) result.get("title");
    this.topic = (String) result.get("gold_keywords");
    this.description = getDescription(result);
    this.releaseDate = getReleaseD(result);
    this.startDate = "";
    this.endDate = "";
    
    // string format
    this.processingLevel = "";
    // numeric format
    this.processingL = 0.0;
    this.userPop = 0.0;
    this.allPop = 0.0;
    this.monthPop = 0.0;
    this.sensors = "";
  }
/*
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
*/
  
  public String getDescription(Map<String, Object> result) {
    String content = (String) result.get("content");
    if (!"".equals(content)) {
      int maxLength = (content.length() < MAX_CHAR) ? content.length() : MAX_CHAR;
      content = content.trim().substring(0, maxLength - 1) + "...";
    }
    return content;
  }
  
  public Double getReleaseD(Map<String, Object> result){
    @SuppressWarnings("unchecked")
    String longdate = (String) result.get("date");
    Date date = new Date(longdate.substring(0, 10));
    this.release_date = df2.format(date);
    return Long.valueOf(longdate.substring(0, 10)).doubleValue();
  }
 /*
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
  */
  /*
  public String getSensors(Map<String, Object> result){
    @SuppressWarnings("unchecked")
    List<String> sensors = (List<String>) result.get("DatasetSource-Sensor-ShortName");
    return String.join(", ", sensors);
  }
  */
}
