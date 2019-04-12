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
package org.apache.sdap.mudrod.ssearch.ranking;

import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.spark.SparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;

import org.deeplearning4j.nn.graph.ComputationGraph;
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport;
import org.deeplearning4j.nn.modelimport.keras.exceptions.InvalidKerasConfigurationException;
import org.deeplearning4j.nn.modelimport.keras.exceptions.UnsupportedKerasConfigurationException;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.io.ClassPathResource;

/**
 * Supports the ability to importing classifier into memory
 */
public class Learner implements Serializable {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	// SVMModel model = null;
	ComputationGraph model = null;
	// transient SparkContext sc = null;
	double[] query = null;

	/**
	 * Constructor to load in spark SVM classifier
	 *
	 * @param classifierName
	 *            classifier type
	 * @param skd
	 *            an instance of spark driver
	 * @param svmSgdModel
	 *            path to a trained model
	 */
	public Learner(SparkDriver skd, String svmSgdModel) {
		// sc = skd.sc.sc();
		// sc.addFile(svmSgdModel, true);
		// model = SVMModel.load(sc, svmSgdModel);
	}

	public Learner(String h5Model)
			throws IOException, InvalidKerasConfigurationException, UnsupportedKerasConfigurationException {
		System.out.println(h5Model);
		//String simpleMlp = new ClassPathResource(h5Model).getFile().getPath();
		if(h5Model.startsWith("file:/")){
			h5Model = h5Model.replace("file:/", "");
		}
		model = KerasModelImport.importKerasModelAndWeights(h5Model + "totalmodel.h5", false);

		//KerasModelImport.importKerasModelAndWeights(h5Model + "model.json", h5Model + "model.h5");
		//MultiLayerNetwork model = null;
	}

	/**
	 * Method of classifying instance
	 *
	 * @param p
	 *            the instance that needs to be classified
	 * @return the class id
	 */
	public double classify(LabeledPoint p) {
		// return model.predict(p.features());
		return 0;
	}

	public double classify(double[] docA, double[] docB) {
		
		
		int lenQuery = query.length;
		int lenDocA = docA.length;
		int lenDocB = docB.length;
		
		int inputs = 3000;
		INDArray features = Nd4j.zeros(inputs);
		for (int i=0; i<1000; i++) {
			if(i<lenQuery){
				features.putScalar(new int[] {i}, query[i]);
			}else{
				features.putScalar(new int[] {i}, 0);
			}
		}
		for (int i=0; i<1000; i++) {
			if(i<lenDocA){
				features.putScalar(new int[] {i}, docA[i]);
			}else{
				features.putScalar(new int[] {i}, 0);
			}
		}
		for (int i=0; i<1000; i++) {
			if(i<lenDocB){
				features.putScalar(new int[] {i}, docB[i]);
			}else{
				features.putScalar(new int[] {i}, 0);
			}
		}
		
		//double prediction = model.ev(features, false).getDouble(0);
		//Random r = new Random();
		//double prediction = -1 + 2 * r.nextDouble();
		
		/*if(prediction < 0){
			return -1;
		}else if (prediction > 0){
			return 1;
		}else{
			return 0;
		}*/
		//System.out.println(prediction);
		//return prediction;
		return 1.0;
	}
	
	public void setQuery(String strquery){
		query = new double[]{1,2,3,4,5};
	}
}
