/*
 * Copyright 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
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

package com.couchbase.connect.kafka.util;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.ArrayList;
import java.util.List;

public class Config {
    private final CouchbaseBucketConfig bucketConfig;

    public Config(final CouchbaseBucketConfig bucketConfig) {
        if (bucketConfig.numberOfPartitions() == 0) {
            throw new ConnectException("Cannot determine number of Couchbase partitions");
        }
        this.bucketConfig = bucketConfig;
    }

    public int numberOfPartitions() {
        return bucketConfig.numberOfPartitions();
    }

    public int numberOfNodes() {
        return bucketConfig.nodes().size();
    }

    public List<List<Short>> partitionsByNodes() {
        int numberOfNodes = numberOfNodes();
        List<List<Short>> groups = new ArrayList<List<Short>>(numberOfNodes);
        for (int nodeIdx = 0; nodeIdx < numberOfNodes; nodeIdx++) {
            groups.add(nodeIdx, new ArrayList<Short>());
        }
        int numberOfPartitions = numberOfPartitions();
        for (short partition = 0; partition < numberOfPartitions; partition++) {
            short nodeIdx = bucketConfig.nodeIndexForMaster(partition, false);
            groups.get(nodeIdx).add(partition);
        }
        return groups;
    }

    /**
     * Split partitions in groups to reduce number of server connections per task.
     *
     * @param numberOfGroups desired number of groups
     * @return List of partitions groups. Note that number of resulting groups
     * might be different than desired.
     */
    public List<List<Short>> groupGreedy(int numberOfGroups) {
        numberOfGroups = Math.min(numberOfGroups, numberOfPartitions());
        List<List<Short>> partitionsByNodes = partitionsByNodes();
        if (numberOfGroups == partitionsByNodes.size()) {
            return partitionsByNodes;
        } else if (numberOfGroups < partitionsByNodes.size()) {
            List<List<Short>> groups = partitionsByNodes.subList(0, numberOfGroups);
            int idx = numberOfGroups;
            while (idx < partitionsByNodes.size()) {
                groups.get(idx % numberOfGroups).addAll(partitionsByNodes.get(idx));
                idx++;
            }
            return groups;
        } else {
            int groupsPerNode = numberOfGroups / numberOfNodes();
            numberOfGroups = groupsPerNode * numberOfNodes();
            List<List<Short>> groups = new ArrayList<List<Short>>(numberOfGroups);
            int groupIdx = 0;
            for (List<Short> partitionGroup : partitionsByNodes) {
                int minimumPartitionsInGroup = partitionGroup.size() / groupsPerNode;
                int offset = 0;
                for (int i = 0; i < groupsPerNode; i++) {
                    int start = offset;
                    int end = offset + minimumPartitionsInGroup;
                    if (partitionGroup.size() - end < minimumPartitionsInGroup) {
                        end = partitionGroup.size();
                    }
                    groups.add(groupIdx, partitionGroup.subList(start, end));
                    offset = end;
                    groupIdx++;
                }
            }
            return groups;
        }
    }

    public List<List<String>> groupGreedyToString(int numberOfGroups) {
        List<List<Short>> groups = groupGreedy(numberOfGroups);
        List<List<String>> groupsString = new ArrayList<List<String>>(groups.size());
        for (List<Short> group : groups) {
            List<String> groupString = new ArrayList<String>(group.size());
            for (Short partition : group) {
                groupString.add(partition.toString());
            }
            groupsString.add(groupString);
        }
        return groupsString;
    }
}
