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

"""Provides access to metric data as pandas dataframes."""

from __future__ import absolute_import

import datetime

import google.cloud.monitoring
import pandas

from . import _query_metadata
from . import _utils


class TimeInterval(object):
  """User friendly relative time intervals."""
  TODAY = 'TODAY'
  YESTERDAY = 'YESTERDAY'
  WEEK_TO_DATE = 'WEEK_TO_DATE'
  LAST_WEEK = 'LAST_WEEK'
  MONTH_TO_DATE = 'MONTH_TO_DATE'
  LAST_MONTH = 'LAST_MONTH'
  QUARTER_TO_DATE = 'QUARTER_TO_DATE'
  LAST_QUARTER = 'LAST_QUARTER'
  YEAR_TO_DATE = 'YEAR_TO_DATE'
  LAST_YEAR = 'LAST_YEAR'

  _FREQ_MAP = dict(TODAY='D', YESTERDAY='D', WEEK_TO_DATE='W', LAST_WEEK='W',
                   MONTH_TO_DATE='MS', LAST_MONTH='MS', QUARTER_TO_DATE='QS',
                   LAST_QUARTER='QS', YEAR_TO_DATE='AS', LAST_YEAR='AS')


class Query(google.cloud.monitoring.Query):
  """Query object for retrieving metric data."""

  def __init__(self,
               metric_type=google.cloud.monitoring.Query.DEFAULT_METRIC_TYPE,
               interval=None, end_time=None, days=0, hours=0, minutes=0,
               project_id=None, context=None):
    """Initializes the core query parameters.

    Only one of `interval` or the `end_time` with `days`/`hours`/`minutes` can
    be specified.

    Examples:
      query = Query('compute.googleapis.com/instance/uptime', interval='LAST_MONTH')
      query = Query('compute.googleapis.com/instance/uptime',
                    end_time=datetime.datetime(2016, 10, 1), days=30)

    Args:
      metric_type: The metric type name. The default value is
          :data:`Query.DEFAULT_METRIC_TYPE
          <google.cloud.monitoring.query.Query.DEFAULT_METRIC_TYPE>`, but
          please note that this default value is provided only for
          demonstration purposes and is subject to change.
      interval: Time interval for the time series. For example:
          TimeInterval.TODAY. Defaults to None.
      end_time: The end time (inclusive) of the time interval for which
          results should be returned, as a datetime object. The default
          is the start of the current minute.
      days: The number of days in the time interval.
      hours: The number of hours in the time interval.
      minutes: The number of minutes in the time interval.
      project_id: An optional project ID or number to override the one provided
          by the context.
      context: An optional Context object to use instead of the global default.

    Raises:
      ValueError: The time interval specified is not valid.
    """
    if interval is not None and (days or hours or minutes):
      raise ValueError('At most one of interval and days/hours/minutes '
                       'can be specified')

    client = _utils.make_client(project_id, context)
    super(Query, self).__init__(client, metric_type,
                                end_time=end_time,
                                days=days, hours=hours, minutes=minutes)
    if interval is not None:
      self._start_time, self._end_time = _get_timestamps(interval)

  def select_metric_type(self, metric_type):
    """Copy the query and update the metric type.

    Args:
      metric_type: The metric type name.

    Returns:
      The new query object.
    """
    new_query = self.copy()
    new_query._filter.metric_type = metric_type
    return new_query

  def metadata(self):
    """Retrieves the metadata for the query."""
    return _query_metadata.QueryMetadata(self)


def _get_utcnow():
  return datetime.datetime.utcnow().replace(second=0, microsecond=0)


def _get_timestamps(interval):
  """Returns the start and end datetime objects given the interval name."""
  interval = interval.upper()
  offset = TimeInterval._FREQ_MAP.get(interval)
  if offset is None:
    raise ValueError('"interval" does not have a valid value')

  now = _get_utcnow()
  today = now.replace(hour=0, minute=0)

  # Beginning of the current period with the provided frequency.
  curr_period_begin = _subtract_offset(today, offset)

  # When calculating an interval extending till now, set end_time to utcnow.
  ends_now = interval.endswith('_TO_DATE') or interval == TimeInterval.TODAY
  end_time = now if ends_now else None

  if interval == TimeInterval.TODAY:
    start_time = today
  elif interval == TimeInterval.YESTERDAY:
    end_time = today
    start_time = curr_period_begin
  else:
    if ends_now:
      start_time = curr_period_begin
    else:
      end_time = curr_period_begin
      start_time = _subtract_offset(end_time, offset)

  # Subtract one microsecond from start_time to include it in the interval.
  start_time -= datetime.timedelta(microseconds=1)
  return start_time, end_time


def _subtract_offset(end_time, offset):
  """Returns the result of substracting an offset from a datetime object"""
  start_time = end_time - pandas.tseries.frequencies.to_offset(offset)
  return start_time.to_datetime()
