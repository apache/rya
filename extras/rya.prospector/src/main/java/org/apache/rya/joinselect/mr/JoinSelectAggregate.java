package org.apache.rya.joinselect.mr;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */



import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.AUTHS;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.OUTPUTPATH;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.PROSPECTS_OUTPUTPATH;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.SPO_OUTPUTPATH;

import java.io.IOException;

import org.apache.rya.joinselect.mr.utils.CardList;
import org.apache.rya.joinselect.mr.utils.CardinalityType;
import org.apache.rya.joinselect.mr.utils.CompositeType;
import org.apache.rya.joinselect.mr.utils.JoinSelectStatsUtil;
import org.apache.rya.joinselect.mr.utils.TripleCard;
import org.apache.rya.joinselect.mr.utils.TripleEntry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

public class JoinSelectAggregate extends Configured implements Tool {

  public static class JoinSelectAggregateMapper extends Mapper<CompositeType,TripleCard,CompositeType,TripleCard> {

    public void map(CompositeType key, TripleCard value, Context context) throws IOException, InterruptedException {

      context.write(key, value);

    }

  }

  public static class JoinReducer extends Reducer<CompositeType,TripleCard,TripleEntry,CardList> {

    public void reduce(CompositeType key, Iterable<TripleCard> values, Context context) throws IOException, InterruptedException {

      CardinalityType card;
      TripleEntry triple;
      CardinalityType subjectCard = null;
      CardinalityType objectCard = null;
      CardinalityType predicateCard = null;
      CardinalityType spCard = null;
      CardinalityType soCard = null;
      CardinalityType poCard = null;
      CardList cList = new CardList((long) 0, (long) 0, (long) 0, (long) 0, (long) 0, (long) 0);
      boolean listEmpty = true;

      // System.out.println("********************************************************************");
      // System.out.println("Key is " + key );

      for (TripleCard val : values) {

        // System.out.println("Value in iterable is " + val);
        if (!val.isCardNull()) {
          card = val.getCard();

          if (card.getCardType().toString().equals("object")) {
            if (objectCard == null) {
              objectCard = new CardinalityType();
              objectCard.set(card);

            } else if (objectCard.compareTo(card) > 0) {
              // System.out.println(objectCard.compareTo(card));
              objectCard.set(card);

            }

          } else if (card.getCardType().toString().equals("predicate")) {
            // System.out.println("Coming in here?");
            if (predicateCard == null) {
              predicateCard = new CardinalityType();
              predicateCard.set(card);

            } else if (predicateCard.compareTo(card) > 0) {
              predicateCard.set(card);

            }
          } else if (card.getCardType().toString().equals("subject")) {
            if (subjectCard == null) {
              subjectCard = new CardinalityType();
              subjectCard.set(card);

            } else if (subjectCard.compareTo(card) > 0) {
              subjectCard.set(card);
            }

          } else if (card.getCardType().toString().equals("subjectpredicate")) {
            if (spCard == null) {
              spCard = new CardinalityType();
              spCard.set(card);

            } else if (spCard.compareTo(card) > 0) {
              spCard.set(card);

            }
          } else if (card.getCardType().toString().equals("subjectobject")) {
            if (soCard == null) {
              soCard = new CardinalityType();
              soCard.set(card);

            } else if (soCard.compareTo(card) > 0) {
              soCard.set(card);

            }
          } else if (card.getCardType().toString().equals("predicateobject")) {
            if (poCard == null) {
              poCard = new CardinalityType();
              poCard.set(card);

            } else if (poCard.compareTo(card) > 0) {
              poCard.set(card);

            }
          }

        } else {

          if (listEmpty) {
            if (subjectCard != null || predicateCard != null || objectCard != null) {

              if (subjectCard != null) {
                cList.setSCard(subjectCard.getCard().get());
              }
              if (predicateCard != null) {
                cList.setPCard(predicateCard.getCard().get());
              }
              if (objectCard != null) {
                cList.setOCard(objectCard.getCard().get());
              }

              listEmpty = false;

            } else if (spCard != null || poCard != null || soCard != null) {

              if (spCard != null) {
                cList.setSPCard(spCard.getCard().get());
              }
              if (poCard != null) {
                cList.setPOCard(poCard.getCard().get());
              }
              if (soCard != null) {
                cList.setSOCard(soCard.getCard().get());
              }

              listEmpty = false;
            }

            // System.out.println("Cardlist is " + cList);
            // System.out.println("Cards are " +
            // subjectCard.getCard() + "," + predicateCard.getCard()
            // +
            // "," + objectCard.getCard() + "," + spCard.getCard() +
            // "," + poCard.getCard() + "," + soCard.getCard());
            //
          }

          // only write record if cardList contains at least one
          // nonzero entry
          if (!val.isTeNull() && !listEmpty) {

            triple = (TripleEntry) val.getTE();

            context.write(triple, cList);
            // System.out.println("Triple is " + triple +
            // " and cardinality is " + cList);

          }

        }
      }

    }

  }

  public static class JoinSelectPartitioner extends Partitioner<CompositeType,TripleCard> {

    @Override
    public int getPartition(CompositeType key, TripleCard value, int numPartitions) {
      return Math.abs(key.getOldKey().hashCode() * 127) % numPartitions;
    }

  }

  public static class JoinSelectGroupComparator extends WritableComparator {

    protected JoinSelectGroupComparator() {
      super(CompositeType.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      CompositeType ct1 = (CompositeType) w1;
      CompositeType ct2 = (CompositeType) w2;
      return ct1.getOldKey().compareTo(ct2.getOldKey());
    }

  }

  public static class JoinSelectSortComparator extends WritableComparator {

    protected JoinSelectSortComparator() {
      super(CompositeType.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      CompositeType ct1 = (CompositeType) w1;
      CompositeType ct2 = (CompositeType) w2;
      return ct1.compareTo(ct2);
    }

  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String inPath1 = conf.get(PROSPECTS_OUTPUTPATH);
    String inPath2 = conf.get(SPO_OUTPUTPATH);
    String auths = conf.get(AUTHS);
    String outPath = conf.get(OUTPUTPATH);

    assert inPath1 != null && inPath2 != null && outPath != null;

    Job job = new Job(conf, this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
    job.setJarByClass(this.getClass());
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
    
    JoinSelectStatsUtil.initJoinMRJob(job, inPath1, inPath2, JoinSelectAggregateMapper.class, outPath, auths);

    job.setSortComparatorClass(JoinSelectSortComparator.class);
    job.setGroupingComparatorClass(JoinSelectGroupComparator.class);
    job.setPartitionerClass(JoinSelectPartitioner.class);
    job.setReducerClass(JoinReducer.class);
    job.setNumReduceTasks(32);
    job.waitForCompletion(true);

    return job.isSuccessful() ? 0 : 1;

  }

}
