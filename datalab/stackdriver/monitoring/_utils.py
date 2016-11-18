# Copyright 2016 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.  See the License for the specific language governing permissions and limitations under
# the License.

"""Provides utility methods for the Monitoring API."""

from __future__ import absolute_import

import collections

import google.cloud.monitoring

import datalab.context


def make_client(project_id=None, context=None):
  context = context or datalab.context.Context.default()
  project_id = project_id or context.project_id
  client = google.cloud.monitoring.Client(
      project=project_id,
      credentials=context.credentials,
  )
  client._connection_class.USER_AGENT = 'pydatalab/v0'
  return client


def listify(value):
  """If value is a string, convert to a list of one element."""
  if value is None:
    return []
  elif isinstance(value, basestring):
    return [value]
  elif isinstance(value, collections.Iterable):
    return value
  else:
    raise TypeError('"value" must be a string or an iterable')
