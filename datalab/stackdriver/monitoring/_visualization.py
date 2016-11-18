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

"""Visualization methods."""

from __future__ import absolute_import

import collections
import sys

import matplotlib.pyplot as plt
import pandas

# Internal imports
from . import _dataframe as dataframe_utils
from . import _utils


def _get_colorscale(colorscale=None, is_divergent=False, is_logscale=False,
                    val_range=None):
  """Returns a colorscale usable by the plotly heatmap method.

  Args:
    colorscale: A colorscale supported by matplotlib.
    is_divergent: boolean, specifies if the series is divergent.
    is_logscale: boolean. If True, then return a logarithmic colorscale.
    val_range: An optional value range.

  Returns:
    A colorscale compatible with plotly heatmap. We return a scale with 11
    colors to map the specified colorscale. The output is a 2-D array where
    each element is a list containing:
      a. A float between 0 and 1
      b. The rgb color as a string of the format: 'rgb(<red>, <green>, <blue)'
    An example based on the colorscale 'RdBu':
    [
      [0.0, 'rgb(103,0,31)'],
      [0.1, 'rgb(176,23,42)'],
      [0.2, 'rgb(214,96,77)'],
      [0.3, 'rgb(243,163,128)'],
      [0.4, 'rgb(253,219,199)'],
      [0.5, 'rgb(246,246,246)'],
      [0.6, 'rgb(209,229,240)'],
      [0.7, 'rgb(144,196,221)'],
      [0.8, 'rgb(67,147,195)'],
      [0.9, 'rgb(32,100,170)'],
      [1.0, 'rgb(5,48,97)']
   ]
  """
  if colorscale is None:
    if not is_divergent and val_range is not None:
      # If the colorscale is not marked as divergent, then decide based on the
      # range.
      df_min, df_max = val_range  # pylint: disable=unpacking-non-sequence

      # Maximum ratio allowed that marks a series as divergent.
      max_ratio = 5
      is_divergent = (df_min < 0 and df_max > 0 and
                      1.0/max_ratio <= abs(df_min)/float(df_max) <= max_ratio)

    if is_divergent:
      cmap = plt.get_cmap('RdBu')
    else:
      cmap = plt.get_cmap('GnBu')
  else:
    cmap = plt.get_cmap(colorscale)

  num_colors = 11
  num_partitions = num_colors - 1
  int_rgb = cmap([float(i)/num_partitions for i in xrange(num_colors)],
                 bytes=True)[:, :3]

  rgb_colormap = []
  for i, row in enumerate(int_rgb.astype(str)):
    if is_logscale:
      value = 10 ** -(num_partitions - i) if i > 0 else 0
    else:
      value = float(i)/num_partitions
    rgb_colormap.append([value, 'rgb(%s)' % ','.join(row)])

  return rgb_colormap


def heatmap(dataframe, levels=None, sort_legend=True, zrange=None,
            colorscale=None, is_logscale=False, is_divergent=False, **kwargs):
  """Draws a plotly heatmap for the specified dataframe.

  Args:
    dataframe: The pandas DataFrame object to draw as a heatmap.
    levels: A list of one or more levels of column header to pick.
    sort_legend: Iff True, the rows are sorted so that the annotation is
        lexicographically sorted.
    zrange: A list or tuple of length 2 numbers containing the range to use for
        the colormap. If not specified, it is calculated from the dataframe.
    colorscale: str, A colorscale supported by matplotlib. See:
        http://matplotlib.org/examples/color/colormaps_reference.html
    is_logscale: boolean, if True, then a logarithmic colorscale is used.
    is_divergent: boolean, specifies if the data has diverging values. If
        False, we check if the data diverges around 0, and use an appropriate
        default colormap. Ignored if you specify the colormap.
    **kwargs: Any arguments to pass in to the layout engine
        plotly.graph_objs.Layout().
  """
  import plotly.graph_objs as go
  import plotly.offline as py

  py.init_notebook_mode()

  if dataframe is None or dataframe.empty:
    return
  # For a square dataframe with identical index and columns, we need to extract
  # the single level names from both dimensions.
  if (len(dataframe.index.names) > 1 and
      dataframe.index.names == dataframe.columns.names):
    dataframe = dataframe_utils.extract_single_level(
        dataframe.T, levels).T

  dataframe = dataframe_utils.extract_single_level(
      dataframe, levels, sort_legend)

  # Reverse the columns since in heatmap the first column is at the bottom.
  dataframe = dataframe[dataframe.columns[::-1]]

  # Make the heatmap taller.
  if 'height' not in kwargs:
    kwargs['height'] = min(800, 200 + 25 * len(dataframe.columns))

  # Make the font smaller
  if 'font' not in kwargs:
    kwargs['font'] = {}
  if 'size' not in kwargs['font']:
    kwargs['font']['size'] = 10

  if 'margin' not in kwargs:
    kwargs['margin'] = {}
  # Adjust the right margin based on the size of the longest column label.
  if 'r' not in kwargs['margin']:
    kwargs['margin']['r'] = 0.7 * kwargs['font']['size'] * max([
        len(col) for col in dataframe.columns])
  # Make the top and bottom margins smaller.
  kwargs['margin']['t'] = kwargs['margin'].get('t') or 80
  kwargs['margin']['b'] = kwargs['margin'].get('b') or 20

  # Move the y-axis labels to the right, and the colorbar to the left.
  kwargs['yaxis'] = dict(side='right')

  zrange = zrange or (dataframe.min().min(), dataframe.max().max())
  colorscale = _get_colorscale(
      colorscale, is_divergent, is_logscale, zrange)
  heatmap_data = go.Heatmap(
      z=dataframe.T.values.tolist(), x=dataframe.index, y=dataframe.columns,
      colorscale=colorscale, zauto=False, zmin=zrange[0], zmax=zrange[1],
      colorbar=dict(x=-0.2))

  fig = go.Figure(data=[heatmap_data], layout=go.Layout(**kwargs))
  py.iplot(fig, show_link=False)


def linechart(dataframe, levels=None, sort_legend=True, **kwargs):
  """Draws a plotly linechart for the specified dataframe.

  Args:
    dataframe: The pandas DataFrame object to draw as a linechart.
    levels: A list of one or more levels of column header to pick.
    sort_legend: Iff True, the linechart legend is sorted by max of a
        timeseries.
    **kwargs: Any arguments to pass in to the layout engine
        plotly.graph_objs.Layout().
  """
  import plotly.graph_objs as go
  import plotly.offline as py

  py.init_notebook_mode()

  if dataframe is None or dataframe.empty:
    return

  dataframe = dataframe_utils.extract_single_level(dataframe, levels, False)

  # Re-order the columns in descending order by their max.
  if sort_legend:
    dataframe = dataframe.reindex_axis(
        dataframe.max().sort_values(ascending=False).index, axis=1)

  # Make the chart height, and top/bottom margins smaller.
  if 'height' not in kwargs:
    kwargs['height'] = 350
  if 'margin' not in kwargs:
    kwargs['margin'] = {}
  kwargs['margin']['t'] = kwargs['margin'].get('t') or 80
  kwargs['margin']['b'] = kwargs['margin'].get('b') or 20

  data = [go.Scatter(x=dataframe.index, y=dataframe[col], name=col)
          for col in dataframe.columns]
  fig = go.Figure(data=data, layout=go.Layout(**kwargs))
  py.iplot(fig, show_link=False)


_ALL_PLOT_KINDS = ('linechart', 'heatmap')


def plot_query_results(query_results, kind='linechart', partition_by=None,
                       annotate_by=None, sort_legend=True, **kwargs):
  """Draws a plotly chart for one or more QueryResults objects.

  Args:
    query_results: One or more QueryResults objects.
    kind: The kind of chart to draw. Defaults to "linechart".
    partition_by: One or more labels to partition the results into separate
      charts. It can be a string or a list/tuple.
    annotate_by: One or more labels to aggregate and annotate each chart by.
      It can be a string or a list/tuple.
    sort_legend: Iff True, the legend is sorted.
    **kwargs: Keyword arguments to pass in to the underlying visualization.

  Raises:
    ValueError: "kind", "partition_by" or "annotate_by" does not have a valid
      value.
  """
  if kind not in _ALL_PLOT_KINDS:
    raise ValueError('%r is not a valid plot kind' % kind)

  partition_by = _utils.listify(partition_by)
  annotate_by = _utils.listify(annotate_by)

  if isinstance(query_results, collections.Iterable):
    by_metric_type = False
    if 'metric_type' in partition_by:
      by_metric_type = True
      partition_by.remove('metric_type')
    elif 'metric_type' not in annotate_by:
      # If metric_type is in neither, it is automatically added to partition_by
      by_metric_type = True

    # Special case. No need to join the dataframe, and then split.
    if by_metric_type:
      for qr in query_results:
        plot_query_results(qr, kind, partition_by, annotate_by, **kwargs)
      return

    # First check the dataframes for compatibility.
    for i, qr in enumerate(query_results):
      if i > 0 and qr.label_keys != query_results[0].label_keys:
        raise ValueError('The specified QueryResults are not compatible for '
                         'viewing in the same chart.')

    # Now add the metric_type as an extra column header, and concatenate the
    # dataframes.
    all_dataframes = [
        dataframe_utils.add_level(qr._dataframe, qr.metric_type, 'metric_type')
        for qr in query_results]
    dataframe = pandas.concat(all_dataframes, axis=1)

  else:
    dataframe = query_results._dataframe

  if not partition_by:
    dataframe_iter = [(None, dataframe)]
  else:
    # When the dataframe has a single level, groupby does not accept a level
    # other than 0.
    level = 0 if len(dataframe.columns.names) == 1 else partition_by
    dataframe_iter = dataframe.groupby(level=level, axis=1)

  for name, dataframe in dataframe_iter:
    if not partition_by:
      if isinstance(query_results, collections.Iterable):
        title = 'All data'
      else:
        title = 'metric_type = %s' % query_results.metric_type
    else:
      title = ', '.join(['%s = %s' % (k, v)
                         for k, v in zip(partition_by, _utils.listify(name))])

    # Call the appropriate visualization function.
    thismodule = sys.modules[__name__]
    getattr(thismodule, kind)(dataframe, levels=annotate_by,
                              sort_legend=sort_legend, title=title, **kwargs)
