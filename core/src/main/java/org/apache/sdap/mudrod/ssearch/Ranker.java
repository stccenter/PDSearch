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
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.ssearch.ranking.Learner;
import org.apache.sdap.mudrod.ssearch.structure.SResult;
import org.apache.sdap.mudrod.utils.Stats;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

/**
 * Supports the ability to calculating ranking score
 */
public class Ranker extends MudrodAbstract implements Serializable {
  private static final long serialVersionUID = 1L;
  transient List<SResult> resultList = new ArrayList<>();
  Learner le = null;

  public Ranker(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    if("1".equals(props.getProperty(MudrodConstants.RANKING_ML)))
      le = new Learner(spark, props.getProperty(MudrodConstants.RANKING_MODEL));
  }

  /**
   * Method of comparing results based on final score
   */
  public class ResultComparator implements Comparator<SResult> {
    @Override
    public int compare(SResult o1, SResult o2) {
      return o2.below.compareTo(o1.below);
    }
  }

  /**
   * Method of ranking a list of result
   *
   * @param resultList result list
   * @return ranked result list
   */
  public List<SResult> rank(List<SResult> resultList) {
    if(le==null) return resultList;
    Stats stats = new Stats();
    
    for (int i = 0; i < resultList.size(); i++) {
      for (int m = 0; m < SResult.rlist.length; m++) {
        String att = SResult.rlist[m].split("_")[0];
        double val = SResult.get(resultList.get(i), att);
        double mean = stats.getMean(att, resultList);
        double std = stats.getStdDev(att, resultList);
        double score = stats.getZscore(val, mean, std);
        String scoreId = SResult.rlist[m];
        SResult.set(resultList.get(i), scoreId, score);
      }
    }

    // using collection.sort directly would cause an "not transitive" error
    // this is because the training model is not a overfitting model
    for (int j = 0; j < resultList.size(); j++) {
      for (int k = 0; k < resultList.size(); k++) {
        if (k != j) {
          resultList.get(j).below += comp(resultList.get(j), resultList.get(k));
        }
      }
    }

    Collections.sort(resultList, new ResultComparator());
    return resultList;
  }

  /**
   * Method of compare two search resutls
   *
   * @param o1 search result 1
   * @param o2 search result 2
   * @return 1 if o1 is greater than o2, 0 otherwise
   */
  public int comp(SResult o1, SResult o2) {
    List<Double> instList = new ArrayList<>();
    for (int i = 0; i < SResult.rlist.length; i++) {
      double o2Score = SResult.get(o2, SResult.rlist[i]);
      double o1Score = SResult.get(o1, SResult.rlist[i]);
      instList.add(o2Score - o1Score);
    }

    double[] ins = instList.stream().mapToDouble(i -> i).toArray();
    LabeledPoint insPoint = new LabeledPoint(99.0, Vectors.dense(ins));
    double prediction = le.classify(insPoint);
    Stats stats = new Stats();
    if (stats.equalComp(prediction, 1)) { //different from weka where the return value is 1 or 2
      return 0;
    } else {
      return 1;
    }
  }

}
