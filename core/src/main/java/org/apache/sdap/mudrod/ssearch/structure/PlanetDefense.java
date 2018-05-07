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
    this.shortName = (String) result.get("title");
    this.longName = "";
    this.topic = (String) result.get("gold_keywords");
    this.description = getDescription(result);
    this.releaseDate = 0.0;
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
  
  public String getDescription(Map<String, Object> result) {
    String content = (String) result.get("content");
    if (!"".equals(content)) {
      int maxLength = (content.length() < MAX_CHAR) ? content.length() : MAX_CHAR;
      content = content.trim().substring(0, maxLength - 1) + "...";
    }
    return content;
  }
}
