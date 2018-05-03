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

import org.apache.sdap.mudrod.main.MudrodConstants;
import org.elasticsearch.search.SearchHit;

import java.lang.reflect.Field;

/**
 * Interface class for search result. All metadata formats should be mapped to this format
 */
public class SResult {
  // Ranking features
  public static final String rlist[] = { "term_score", "releaseDate_score", "processingL_score", 
      "allPop_score", "monthPop_score", "userPop_score"};
  
  public String fieldsList[] = {};
  
  // Descriptive variables
  public String shortName;
  public String longName;
  public String topic;
  public String description;
  public String release_date;
  public String version;
  public String processingLevel;
  public String startDate;
  public String endDate;
  public String sensors;
 
  // Numeric variables
  public Double term;
  public Double releaseDate;
  public Double processingL;
  public Double allPop;
  public Double monthPop;
  public Double userPop;
  
  // Normalized numeric variables
  public Double term_score;
  public Double releaseDate_score;
  public Double processingL_score;
  public Double allPop_score;
  public Double monthPop_score;
  public Double userPop_score;

  public Double prediction = 0.0;
  public Integer below = 0;

  public SResult() {
  }
  
  public SResult(SResult sr) {
    for (String aRlist : rlist) {
      set(this, aRlist, get(sr, aRlist));
    }
  }

  public SResult makeResult(String format, SearchHit hit) {
    if(MudrodConstants.PODAAC_META_FORMAT.equals(format))
      return new Podaac(hit);
    else if(MudrodConstants.PD_META_FORMAT.equals(format))
      return new PlanetDefense(hit);
    else
      return null;
  }

  /**
   * Generic setter method
   *
   * @param object     instance of SResult
   * @param fieldName  field name that needs to be set on
   * @param fieldValue field value that needs to be set to
   * @return 1 means success, and 0 otherwise
   */
  public static boolean set(Object object, String fieldName, Object fieldValue) {
    Class<?> clazz = object.getClass();
    while (clazz != null) {
      try {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(object, fieldValue);
        return true;
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
    return false;
  }

  /**
   * Generic getter method
   *
   * @param object    instance of SResult
   * @param fieldName field name of search result
   * @param <V>       data type
   * @return the value of the filed in the object
   */
  @SuppressWarnings("unchecked")
  public static <V> V get(Object object, String fieldName) {
    Class<?> clazz = object.getClass();
    while (clazz != null) {
      try {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (V) field.get(object);
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
    return null;
  }
}
