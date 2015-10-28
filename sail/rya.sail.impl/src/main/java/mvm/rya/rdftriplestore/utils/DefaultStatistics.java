package mvm.rya.rdftriplestore.utils;

/*
 * #%L
 * mvm.rya.rya.sail.impl
 * %%
 * Copyright (C) 2014 Rya
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStatistics;

/**
 * Class DefaultStatistics
 * Date: Apr 12, 2011
 * Time: 1:31:05 PM
 */
public class DefaultStatistics extends EvaluationStatistics {

    public DefaultStatistics() {
    }

    @Override
    protected CardinalityCalculator createCardinalityCalculator() {
        return new DefaultCardinalityCalculator();
    }

    public class DefaultCardinalityCalculator extends CardinalityCalculator {

        double count = 0.0;

        @Override
        protected double getCardinality(StatementPattern sp) {
            //based on how many (subj, pred, obj) are set
//            int numSet = 3;
//            if (sp.getSubjectVar().hasValue()) numSet--;
//            if (sp.getPredicateVar().hasValue()) numSet--;
//            if (sp.getObjectVar().hasValue()) numSet--;
//            return numSet;
            return count++;
        }
    }

}
