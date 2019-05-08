from __future__ import absolute_import

import argparse
import logging

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.examples.wordcount import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io import WriteToText
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.trigger import AfterProcessingTime
from apache_beam.transforms.trigger import Repeatedly
from apache_beam.transforms.trigger import AfterCount
from apache_beam import window


def run(argv=None):
  """Build and run the pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output', required=True,
      help=('Output'
            'op.csv'))
  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument(
      '--input_topic',
      help=('Input PubSub topic of the form '
            '"projects/<PROJECT>/topics/<TOPIC>".'))
  group.add_argument(
      '--input_subscription',
      help=('Input PubSub subscription of the form '
            '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  pipeline_options.view_as(StandardOptions).streaming = True
  p = beam.Pipeline(options=pipeline_options)

  # Read from PubSub into a PCollection.
  if known_args.input_subscription:
    messages = (p
                | beam.io.ReadFromPubSub(
                    subscription=known_args.input_subscription)
                .with_output_types(bytes))
  else:
    messages = (p
                | beam.io.ReadFromPubSub(topic=known_args.input_topic)
                .with_output_types(bytes))

  lines = messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))

  # Count the occurrences of each word.
  def count_ones(word_ones):
    (word, ones) = word_ones
    return (word, sum(ones))

  def format_result(word_count):
      (word, count) = word_count
      return '%s: %d' % (word, count)

  counts = (lines
            | 'split' >> (beam.ParDo(WordExtractingDoFn())
                          .with_output_types(unicode))
            | 'pair_with_one' >> beam.Map(lambda x: (x, 1))

        #DISCARDING
            #| beam.WindowInto(window.SlidingWindows(30, 1))
            |'window' >> beam.WindowInto(window.FixedWindows(30),trigger=AfterProcessingTime(20))
            #,trigger=AfterProcessingTime(20),accumulation_mode=AccumulationMode.DISCARDING)
            #| 'window' >> beam.WindowInto(window.GlobalWindows(),trigger=Repeatedly(AfterCount(3)),accumulation_mode=AccumulationMode.ACCUMULATING)
            | 'group' >> beam.GroupByKey()
	    | 'count' >> beam.Map(count_ones)
            | 'format' >> beam.Map(format_result))

  
  counts | 'write' >> WriteToText(known_args.output)

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()