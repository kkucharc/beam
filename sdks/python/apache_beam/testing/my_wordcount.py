#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
python -m apache_beam.testing.my_wordcount --test-pipeline-options="--input=kinglear.txt --output=counts.txt"

"""

from __future__ import absolute_import

import logging
import re
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline


def run():
  pipeline = TestPipeline()
  input = pipeline.get_option('input')
  output = pipeline.get_option('output')

  (pipeline
   | 'Read' >> beam.io.ReadFromText(input)
   | 'ExtractWords' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
   | 'Count' >> beam.combiners.Count.PerElement()
   | 'Map' >> beam.Map(lambda word_count: '%s: %s' % (word_count[0], word_count[1]))
   | 'Write' >> beam.io.WriteToText(output)
   )

  pipeline.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()


