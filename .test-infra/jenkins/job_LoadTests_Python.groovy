/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import LoadTestsBuilder as loadTestsBuilder
import CommonJobProperties as commonJobProperties



def testsConfigurations = [
        [
                jobName           : 'beam_Python_LoadTests_SideInput_Direct_Small',
                jobDescription    : 'Runs GroupByKey load tests on direct runner small records 10b',
                itClass           : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                prCommitStatusName: 'Python SideInput Small Load Test Direct',
                prTriggerPhase    : 'Run Python SideInput Small Load Test Direct',
                runner            : loadTestsBuilder.Runner.DIRECT,
                jobProperties     : [
                        publishToBigQuery: true,
                        bigQueryDataset  : 'load_tests',
                        bigQueryTable    : 'direct_gbk_small',
                        input_options    : '{"num_records": 1,' +
                                '"key_size": 1,' +
                                '"value_size":1,' +
                                '"bundle_size_distribution_type": "const",' +
                                '"bundle_size_distribution_param": 1,' +
                                '"force_initial_num_bundles": 0' +
                                '}',
                        nmber_of_counter_operations       : 1,
                ]

        ],
]
for (testConfiguration in testsConfigurations) {
    create_load_test_job(testConfiguration)
}

private void create_load_test_job(testConfiguration) {

    // This job runs load test with Metrics API
    job(testConfiguration.jobName) {
        description(testConfiguration.jobDescription)

        // Set default Beam job properties.
        commonJobProperties.setTopLevelMainJobProperties(delegate)

        // Allows triggering this build against pull requests.
        commonJobProperties.enablePhraseTriggeringFromPullRequest(
                delegate,
                testConfiguration.prCommitStatusName,
                testConfiguration.prTriggerPhase)


        loadTestsBuilder.buildTest(delegate, testConfiguration.jobDescription, testConfiguration.runner, testConfiguration.jobProperties, null)
    }
}