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

import CommonJobProperties as commonJobProperties
import LoadTestsBuilder as loadTestsBuilder
import PhraseTriggeringPostCommitBuilder

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def testConfigurations = [
        [
                title        : 'Combine Python Load test: 2GB of 10B records',
                itClass      : 'apache_beam.testing.load_tests.combine_test:CombineTest.testCombine',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name            : 'load-tests-python-dataflow-batch-combine_1-' + now,
                        project             : 'apache-beam-testing',
                        temp_location       : 'gs://temp-storage-for-perf-tests/smoketests',
                        publish_to_big_query: true,
                        metrics_dataset     : 'load_test_SMOKE',
                        metrics_table       : 'python_dataflow_batch_combine_1',
                        input_options       : '\'{"num_records": 200000000,' +
                                '"key_size": 1,' +
                                '"value_size": 9}\'',
                        fanout              : 1,
                        maxNumWorkers       : 5,
                        numWorkers          : 5,
                        autoscalingAlgorithm: "NONE"
                ]
        ],
        [
                title        : 'Combine Python Load test: 2GB of 100B records',
                itClass      : 'apache_beam.testing.load_tests.combine_test:CombineTest.testCombine',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name            : 'load-tests-python-dataflow-batch-combine_2-' + now,
                        project             : 'apache-beam-testing',
                        temp_location       : 'gs://temp-storage-for-perf-tests/loadtests',
                        publish_to_big_query: true,
                        metrics_dataset     : 'load_test',
                        metrics_table       : 'python_dataflow_batch_combine_2',
                        input_options       : '\'{"num_records": 20000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90}\'',
                        fanout              : 1,
                        maxNumWorkers       : 5,
                        numWorkers          : 5,
                        autoscalingAlgorithm: "NONE"
                ]
        ],
        [
                title        : 'Combine Python Load test: 2GB of 100kB records',
                itClass      : 'apache_beam.testing.load_tests.combine_test:CombineTest.testCombine',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name            : 'load-tests-python-dataflow-batch-combine_3-' + now,
                        project             : 'apache-beam-testing',
                        temp_location       : 'gs://temp-storage-for-perf-tests/loadtests',
                        publish_to_big_query: true,
                        metrics_dataset     : 'load_test',
                        metrics_table       : 'python_dataflow_batch_combine_3',
                        input_options       : '\'{"num_records": 2000,' +
                                '"key_size": 100000,' +
                                '"value_size": 900000}\'',
                        fanout              : 1,
                        maxNumWorkers       : 5,
                        numWorkers          : 5,
                        autoscalingAlgorithm: "NONE"
                ]
        ],
        [
                title        : 'Combine Python Load test: fanout 4 times with 2GB 10-byte records total',
                itClass      : 'apache_beam.testing.load_tests.combine_test:CombineTest.testCombine',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name            : 'load-tests-python-dataflow-batch-combine_4-' + now,
                        project             : 'apache-beam-testing',
                        temp_location       : 'gs://temp-storage-for-perf-tests/loadtests',
                        publish_to_big_query: true,
                        metrics_dataset     : 'load_test',
                        metrics_table       : 'python_dataflow_batch_combine_4',
                        input_options       : '\'{"num_records": 5000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90}\'',
                        fanout              : 4,
                        maxNumWorkers       : 16,
                        numWorkers          : 16,
                        autoscalingAlgorithm: "NONE"
                ]
        ],
        [
                title        : 'Combine Python Load test: fanout 8 times with 2GB 10-byte records total',
                itClass      : 'apache_beam.testing.load_tests.combine_test:CombineTest.testCombine',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name            : 'load-tests-python-dataflow-batch-combine_5-' + now,
                        project             : 'apache-beam-testing',
                        temp_location       : 'gs://temp-storage-for-perf-tests/loadtests',
                        publish_to_big_query: true,
                        metrics_dataset     : 'load_test',
                        metrics_table       : 'python_dataflow_batch_combine_5',
                        input_options       : '\'{"num_records": 2500000,' +
                                '"key_size": 10,' +
                                '"value_size": 90}\'',
                        fanout              : 8,
                        maxNumWorkers       : 16,
                        numWorkers          : 16,
                        autoscalingAlgorithm: "NONE"
                ]
        ],
]

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_Python_LoadTests_Batch_Combine',
        'Run Python Load Tests Batch Combine',
        'Python Load Tests Batch Combine',
        this
) {
    description("Runs Python batch combine load tests")
    commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 120)

    for (testConfiguration in testConfigurations) {
        loadTestsBuilder.loadTest(delegate, testConfiguration.title, testConfiguration.runner,testConfiguration.sdk, testConfiguration.jobProperties, testConfiguration.itClass)
    }
}