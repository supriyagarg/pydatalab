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

"""Groups for the Google Monitoring API."""

from __future__ import absolute_import
from __future__ import unicode_literals
from builtins import object

import collections
import fnmatch

import pandas

import datalab.context

from . import _utils


class Groups(object):
  """Represents a list of Stackdriver groups for a project."""

  _DISPLAY_HEADERS = ('Group ID', 'Group name', 'Parent ID', 'Parent name',
                      'Is cluster', 'Filter')

  def __init__(self, project_id=None, context=None):
    """Initializes the Groups for a Stackdriver project.

    Args:
      project_id: An optional project ID or number to override the one provided
          by the context.
      context: An optional Context object to use instead of the global default.
    """
    self._context = context or datalab.context.Context.default()
    self._project_id = project_id or self._context.project_id
    self._client = _utils.make_client(project_id, context)
    self._group_dict = None

  def list(self, pattern='*'):
    """Returns a list of groups that match the filters.

    Args:
      pattern: An optional pattern to filter the groups based on their display
          name. This can include Unix shell-style wildcards. E.g.
          ``"Production*"``.

    Returns:
      A list of Group objects that match the filters.
    """
    if self._group_dict is None:
      self._group_dict = collections.OrderedDict(
          (group.id, group) for group in self._client.list_groups())

    return [group for group in self._group_dict.values()
            if fnmatch.fnmatch(group.display_name, pattern)]

  def as_dataframe(self, pattern='*', max_rows=None):
    """Creates a pandas dataframe from the groups that match the filters.

    Args:
      pattern: An optional pattern to further filter the groups. This can
          include Unix shell-style wildcards. E.g. ``"Production *"``,
          ``"*-backend"``.
      max_rows: The maximum number of groups to return. If None, return all.

    Returns:
      A pandas dataframe containing matching groups.
    """
    data = []
    for i, group in enumerate(self.list(pattern)):
      if max_rows is not None and i >= max_rows:
        break
      parent = self._group_dict.get(group.parent_id)
      parent_display_name = '' if parent is None else parent.display_name
      data.append([
          group.id, group.display_name, group.parent_id,
          parent_display_name, group.is_cluster, group.filter])

    return pandas.DataFrame(data, columns=self._DISPLAY_HEADERS)

  def _group_display_id(self, group_id):
    """Creates the ID to display for a group."""
    group = self._group_dict.get(group_id)
    if group is None:
      return group_id
    return '%s (%s)' % (group.display_name, group_id)

  def treemap(self, metric_type=None,
              interval=None, end_time=None, days=0, hours=0, minutes=0):
    """Draws a treemap with the group hierarchy in the project.

    When specifying the time interval for the metric data to read, you can
    specify only one of `interval` or the `end_time` with
    `days`/`hours`/`minutes`. By default, the last one hour is selected.

    Args:
      metric_type: An optional metric type that assigns color to the treemap
          cells. You can only specify metrics with numeric data.
      interval: Time interval over which to get the aggregate (mean) of the
        metric data. For example `TimeInterval.TODAY`. Defaults to None.
      end_time: The end time (inclusive) of the time interval as a datetime
        object. The default is the start of the current minute.
      days: The number of days in the time interval.
      hours: The number of hours in the time interval.
      minutes: The number of minutes in the time interval.

    Returns:
      The HTML rendering of a treemap.

    Raises:
      ValueError: The time interval specified is not valid.
    """
    import datalab.utils.commands

    if (interval is None and end_time is None and
        not (days or hours or minutes)):
      hours = 1

    dataframe = self._hierarchy(
        metric_type, interval=interval,
        end_time=end_time, days=days, hours=hours, minutes=minutes)
    dataframe = dataframe.fillna(0)
    properties = dict(height=500, maxPostDepth=1,
                      minColor='green', maxColor='red')
    if metric_type is not None:
      properties['midColor'] = 'yellow'

    prop = '\n'.join('%s: %s' % (key, value)
                     for key, value in properties.iteritems())
    return datalab.utils.commands._chart._chart_cell({
        'chart': 'treemap',
        'data': dataframe,
        'fields': ','.join(dataframe.columns)
    }, prop)

  def _hierarchy(self, metric_type=None,
                 interval=None, end_time=None, days=0, hours=0, minutes=0):
    """Returns a dataframe with the group hierarchy of the project. """
    _Node = collections.namedtuple(
        '_Node', ('entity_id', 'parent_id', 'size', 'metric'))

    query = None
    if metric_type is not None:
      query = self._build_query(
          metric_type, interval=interval,
          end_time=end_time, days=days, hours=hours, minutes=minutes)

    hierarchy_rows = [_Node(entity_id=self._project_id, parent_id=None,
                            size=0, metric=0)]
    for group in self.list():
      parent_id = self._group_display_id(group.parent_id) or self._project_id
      entity_id = self._group_display_id(group.id)
      if metric_type is not None:
        dataframe = query.select_group(group.id).as_dataframe()
        metric = dataframe.mean().mean()
      else:
        metric = 0
      hierarchy_rows.append(
          _Node(entity_id=entity_id, parent_id=parent_id, size=1,
                metric=metric))

    return pandas.DataFrame.from_records(hierarchy_rows, columns=_Node._fields)

  def _build_query(self, metric_type,
                   interval=None, end_time=None, days=0, hours=0, minutes=0):
    """Builds a query object based on the metric_type and time interval."""
    from . import _query

    descriptor = self._client.fetch_metric_descriptor(metric_type)
    if descriptor.value_type not in ['INT64', 'DOUBLE']:
      raise ValueError('Only numeric metric types are supported')

    if descriptor.metric_kind == 'CUMULATIVE':
      alignment_method = 'ALIGN_DELTA'
    else:
      alignment_method = 'ALIGN_MEAN'

    query = _query.Query(
        metric_type, interval=interval, end_time=end_time, days=days,
        hours=hours, minutes=minutes, project_id=self._project_id,
        context=self._context)
    duration = (query._end_time - query._start_time).total_seconds()
    query = query.align(alignment_method, seconds=int(duration))
    query = query.reduce('REDUCE_MEAN', 'resource.project_id')
    return query
