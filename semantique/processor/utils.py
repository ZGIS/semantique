import rioxarray

import numpy as np
import pandas as pd
import xarray as xr

from semantique.dimensions import TIME, SPACE, X, Y

def get_null(x):
  """Return the appropriate nodata value for an array.

  For arrays of datetime values NaT (not a time) is returned. For other arrays
  NaN (not a number) is returned.

  Parameters
  ----------
    x : :obj:`xarray.DataArray` or :obj:`numpy.array`
      The input array.

  """
  return np.datetime64("NaT") if x.dtype.kind == "M" else np.nan

def allnull(x, axis):
  """Test whether all elements along a given axis in an array are null.

  Parameters
  ----------
    x : :obj:`xarray.DataArray` or :obj:`numpy.array`
      The input array.
    axis : :obj:`int`
      Axis along which the tests are performed.

  Return
  -------
    :obj:`numpy.array`

  """
  return np.equal(np.sum(pd.notnull(x), axis = axis), 0)

def null_as_zero(x):
  """Convert all null values in an array to 0.

  Parameters
  -----------
    x : :obj:`xarray.DataArray` or :obj:`numpy.array`
      The input array.

  Return
  ------
    :obj:`numpy.array`

  """
  return np.where(pd.isnull(x), 0, x)

def inf_as_null(x):
  """Convert all infinite values in an array to null values.

  Parameters
  -----------
    x : :obj:`xarray.DataArray` or :obj:`numpy.array`
      The input array.

  Return
  ------
    :obj:`numpy.array`

  """
  try:
    is_inf = np.isinf(x)
  except TypeError:
    return x
  return np.where(is_inf, get_null(x), x)

def datetime64_as_unix(x):
  """Convert datetime64 values in an array to unix time values.

  Unix time values are the number of seconds since 1970-01-01.

  Parameters
  -----------
    x : :obj:`xarray.DataArray` or :obj:`numpy.array`
      The input array containing :obj:`numpy.datetime64` values.

  Return
  ------
    :obj:`numpy.array`

  """
  if not x.dtype.kind == "M":
    return x
  # Type conversion assigns a specific integer to null values.
  # We want them to be just null.
  unix = np.array(x).astype("<M8[s]").astype(int)
  return np.where(pd.notnull(x), unix, np.nan)

def convert_datetime64(obj, tz_from, tz_to, **kwargs):
  """Convert a numpy datetime object to a different timezone.

  Numpy datetime objects do not have native support for timezones anymore.
  Therefore pandas is used to convert between different timezones.

  Parameters
  ----------
    obj : :obj:`numpy.datetime64`
      Object to be converted.
    tz_from:
      Timezone of the object to be converted. Can be given as :obj:`str`
      referring to the name of a timezone in the tz database, or as instance
      of any class inheriting from :class:`datetime.tzinfo`.
    tz_to:
      Timezone the object should be converted to. Can be given as :obj:`str`
      referring to the name of a timezone in the tz database, or as instance
      of any class inheriting from :class:`datetime.tzinfo`.
    **kwargs:
      Additional keyword arguments passed on to
      :meth:`pandas.Timestamp.tz_convert`.

  Returns
  -------
    :obj:`numpy.datetime64`

  """
  obj_new = pd.Timestamp(obj).tz_localize(tz_from).tz_convert(tz_to, **kwargs)
  return np.datetime64(obj_new.tz_localize(None))

def parse_extent(spatial_extent, temporal_extent, spatial_resolution,
                 temporal_resolution = None, crs = None, tz = None, trim = True):
  """Parse the spatial and temporal extent into a spatio-temporal array.

  Internally the query processor uses a multi-dimensional array to represent
  the spatio-temporal extent of the query. This is an :obj:`xarray.DataArray`
  and forms the base template for all arrays that are retrieved from the
  mapping during query processing.

  Parameters
  -----------
    spatial_extent : SpatialExtent
      Spatial extent.
    temporal_extent : TemporalExtent
      Temporal extent.
    spatial_resolution : :obj:`list`
      Spatial resolution of the array. Should be given as a list in the format
      *[y, x]*, where y is the cell size along the y-axis, x is the cell size
      along the x-axis, and both are given as :obj:`int` or :obj:`float`
      value expressed in the units of the CRS. These values should include
      the direction of the axes. For most CRSs, the y-axis has a negative
      direction, and hence the cell size along the y-axis is given as a
      negative number.
    temporal_resolution : :obj:`str` or :obj:`pandas.tseries.offsets.DateOffset`
      Temporal resolution of the array. Can be given as offset alias as
      defined in pandas, e.g. "D" for a daily frequency. These aliases can
      have multiples, e.g. "5D". If :obj:`None`, only the start and end
      instants of the extent will be temporal coordinates in the array.
    crs : optional
      Coordinate reference system in which the spatial coordinates of the array
      should be expressed. Can be given as any object understood by the
      initializer of :class:`pyproj.crs.CRS`. This includes
      :obj:`pyproj.crs.CRS` objects themselves, as well as EPSG codes and WKT
      strings. If :obj:`None`, the CRS of the provided spatial extent is used.
    tz : optional
      Timezone in which the temporal coordinates of the array should be
      expressed. Can be given as :obj:`str` referring to the name of a time
      zone in the tz database, or as instance of any class inheriting from
      :class:`datetime.tzinfo`. If :obj:`None`, the timezone of the provided
      temporal extent is used.
    trim : :obj:`bool`
      Should the array be trimmed before returning?
      Trimming means that dimension coordinates for which all values are
      missing are removed from the array. The spatial dimension is treated
      differently, by trimming it only at the edges, and thus maintaining
      its regularity.

  Returns
  -------
    :obj:`xarray.DataArray`
      A two-dimensional array with a spatial and temporal dimension. The
      spatial dimension is a stacked dimension with each coordinate value
      being a tuple of the x and y coordinate of the corresponding cell.

  """
  # Rasterize spatial extent.
  space = spatial_extent.rasterize(spatial_resolution, crs)
  # Make sure X and Y dims have the correct names and value types.
  space = space.rename({space.rio.y_dim: Y, space.rio.x_dim: X})
  space[Y].sq.value_type = "numerical"
  space[X].sq.value_type = "numerical"
  # Add spatial feature indices as coordinates.
  space.coords["feature"] = ([Y, X], space.data)
  space["feature"].sq.value_type = space.sq.value_type
  space["feature"].sq.value_labels = space.sq.value_labels
  # Discretize temporal extent.
  time = temporal_extent.discretize(temporal_resolution, tz)
  # Combine rasterized spatial extent with discretized temporal extent.
  extent = space.expand_dims({TIME: time})
  extent[TIME].sq.value_type = "datetime"
  # Add temporal reference.
  extent = extent.sq.write_tz(time.sq.tz)
  # Trim the extent array if requested.
  # This means we drop all x, y and time slices for which all values are nan.
  if trim:
    extent = extent.sq.trim()
  return extent

def parse_datetime_component(name, obj):
  """Parse datetime accessor arrays.

  The `datetime accessors`_ of :obj:`xarray.DataArray` objects are treated in
  semantique as a component of the temporal dimension. Parsing them includes
  adding :attr:`value_type <semantique.processor.arrays.Array.value_type>`
  and :attr:`value_label <semantique.processor.arrays.Array.value_labels>`
  properties as well as in some cases re-organize the values in the array.

  Parameters
  -----------
    name : :obj:`str`
      Name of the datetime component.
    obj : :obj:`xarray.DataArray`
      Xarray datetime accessor.

  Returns
  --------
    :obj:`xarray.DataArray`
      Parsed datetime accessor.

  .. _datetime accessors:
    https://xarray.pydata.org/en/stable/user-guide/time-series.html#datetime-components

  """
  if name in ["dayofweek", "weekday"]:
    obj.sq.value_type = "ordinal"
    obj.sq.value_labels = {
      0: "Monday",
      1: "Tuesday",
      2: "Wednesday",
      3: "Thursday",
      4: "Friday",
      5: "Saturday",
      6: "Sunday"
    }
  elif name == "month":
    obj.sq.value_type = "ordinal"
    obj.sq.value_labels = {
      1: "January",
      2: "February",
      3: "March",
      4: "April",
      5: "May",
      6: "June",
      7: "July",
      8: "August",
      9: "September",
      10: "October",
      11: "November",
      12: "December"
    }
  elif name == "quarter":
    obj.sq.value_type = "ordinal"
    obj.sq.value_labels = {
      1: "January, February, March",
      2: "May, April, June",
      3: "July, August, September",
      4: "October, November, December"
    }
  elif name == "season":
    # In xarray seasons get stored as strings.
    # We want to store them as integers instead.
    for k, v in zip(["MAM", "JJA", "SON", "DJF"], [1, 2, 3, 4]):
      obj = obj.str.replace(k, str(v))
    obj = obj.astype(int)
    obj.sq.value_type = "ordinal"
    obj.sq.value_labels = {
      1: "March, April, May",
      2: "June, July, August",
      3: "September, October, November",
      4: "December, January, February"
    }
  else:
    obj.sq.value_type = "numerical"
  return obj