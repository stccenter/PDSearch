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
/**
 * This package includes the preprocessing, processing, and data structure used
 * by recommendation module.
 */
package org.apache.sdap.mudrod.utils;

import java.text.DecimalFormat;
import java.util.List;

import org.apache.sdap.mudrod.ssearch.structure.SResult;

public class Stats {
  
  /**
   * Method of calculating mean value
   *
   * @param attribute  the attribute name that need to be calculated on
   * @param resultList an array list of result
   * @return mean value
   */
  public double getMean(String attribute, List<SResult> resultList) {
    double sum = 0.0;
    for (SResult a : resultList) {
      sum += (double) SResult.get(a, attribute);
    }
    return getNDForm(sum / resultList.size());
  }

  /**
   * Method of calculating variance value
   *
   * @param attribute  the attribute name that need to be calculated on
   * @param resultList an array list of result
   * @return variance value
   */
  public double getVariance(String attribute, List<SResult> resultList) {
    double mean = getMean(attribute, resultList);
    double temp = 0.0;
    double val;
    for (SResult a : resultList) {
      val = (Double) SResult.get(a, attribute);
      temp += (mean - val) * (mean - val);
    }

    return getNDForm(temp / resultList.size());
  }

  /**
   * Method of calculating standard variance
   *
   * @param attribute  the attribute name that need to be calculated on
   * @param resultList an array list of result
   * @return standard variance
   */
  public double getStdDev(String attribute, List<SResult> resultList) {
    return getNDForm(Math.sqrt(getVariance(attribute, resultList)));
  }

  /**
   * Method of calculating Z score
   *
   * @param val  the value of an attribute
   * @param mean the mean value of an attribute
   * @param std  the standard deviation of an attribute
   * @return Z score
   */
  public double getZscore(double val, double mean, double std) {
    if (!equalComp(std, 0)) {
      return getNDForm((val - mean) / std);
    } else {
      return 0;
    }
  }

  public boolean equalComp(double a, double b) {
    return Math.abs(a - b) < 0.0001;
  }
  
  /**
   * Get the first N decimals of a double value
   *
   * @param d double value that needs to be processed
   * @return processed double value
   */
  public double getNDForm(double d) {
    DecimalFormat ndForm = new DecimalFormat("#.###");
    return Double.valueOf(ndForm.format(d));
  }


}
