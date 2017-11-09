<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License. 
-->

Benchmark Optimizations


## KafkaLatencyBenchmark

Several strategies for partitioning the rya_pcj_updater table.  If other tablet start hot spotting, they can be further split similar to how `STATEMENT_PATTERN_` is shown.
```
addsplits AGGREGATION_ JOIN_ PROJECTION_ QUERY_ STATEMENT_PATTERN_ urn -t rya_pcj_updater

# or
addsplits AGGREGATION_ JOIN_ PROJECTION_ QUERY_ STATEMENT_PATTERN_0 STATEMENT_PATTERN_4 STATEMENT_PATTERN_8 STATEMENT_PATTERN_c urn -t rya_pcj_updater

# or
addsplits AGGREGATION_ JOIN_ PROJECTION_ QUERY_ STATEMENT_PATTERN_0 STATEMENT_PATTERN_1 STATEMENT_PATTERN_2 STATEMENT_PATTERN_3 STATEMENT_PATTERN_4 STATEMENT_PATTERN_5 STATEMENT_PATTERN_6 STATEMENT_PATTERN_7 STATEMENT_PATTERN_8 STATEMENT_PATTERN_9 STATEMENT_PATTERN_a STATEMENT_PATTERN_b STATEMENT_PATTERN_c STATEMENT_PATTERN_d STATEMENT_PATTERN_e STATEMENT_PATTERN_f urn -t rya_pcj_updater

# then ensure the splits have been applied.
compact -t rya_pcj_updater
```

It is also possible to lower the table's split threshold to generate more tablets.
```
root@accumulo> config -t rya_pcj_updater -s table.split.threshold=100M
```

Identify which tablets are on what hosts and what particular data you might be 
hotspotting on.  Note that the tablet splits are ordered lexicographically, and 
the split point is exclusive.  So the tablet that contains AGGREGATION_ data is
 actually contained on the tablet with the split point label: 4qn;JOIN_.
```
root@accumulo> tables -l
...
rya_osp       =>       4qr
rya_pcj_updater =>       4qn
rya_po        =>       4qq
...

root@accumulo> scan -t accumulo.metadata -b 4qn; -e 4qn< -c loc
4qn;AGGREGATION_ loc:25e09c3a40b000e []    10.10.10.10:9997
4qn;JOIN_ loc:45e09c3a2260012 []    10.10.10.11:9997
4qn;PROJECTION_ loc:55e09c3a2cf0014 []    10.10.10.12:9997
4qn;QUERY_ loc:35e09c3a2080021 []    10.10.10.13:9997
4qn;STATEMENT_PATTERN_0 loc:16e09c3a436001d []    10.10.10.14:9997
4qn;STATEMENT_PATTERN_4 loc:17e09c3a436001d []    10.10.10.15:9997
4qn;STATEMENT_PATTERN_8 loc:18e09c3a436001d []    10.10.10.16:9997
4qn;STATEMENT_PATTERN_c loc:19e09c3a436001d []    10.10.10.17:9997
4qn;urn loc:15e09c3a4360019 []    10.10.10.18:9997
4qn< loc:55e09c3a2cf0012 []    10.10.10.19:9997
```

Use the `RegexGroupBalancer` to ensure all STATEMENT_PATTERN_x tablets are evenly distributed between all available tablet servers.  This distribution strategy will also apply to other groups that are specified in the regex.
```
root@accumulo> config -t rya_pcj_updater -s table.custom.balancer.group.regex.pattern=(AGGREGATION_|JOIN_|PROJECTION_|QUERY_|STATEMENT_PATTERN_|urn).*
#root@accumulo> config -t rya_pcj_updater -s table.custom.balancer.group.regex.default=AGGREGATION_
root@accumulo> config -t rya_pcj_updater -s table.balancer=org.apache.accumulo.server.master.balancer.RegexGroupBalancer
```
References:  
https://blogs.apache.org/accumulo/entry/balancing_groups_of_tablets  
https://reviews.apache.org/r/29230/diff/2#index_header
