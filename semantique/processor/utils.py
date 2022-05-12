import numpy as np
import pandas as pd
import xarray as xr

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

def create_extent_cube(spatial_extent, temporal_extent, spatial_resolution,
                       temporal_resolution = None, crs = None, tz = None,
                       trim = True):
  """Create a spatio-temporal extent cube.

  Internally the query processor uses a multi-dimensional array to represent
  the spatio-temporal extent of the query. This is an :obj:`xarray.DataArray`
  and forms the base template for all cubes that are fetched from the factbase
  during query processing.

  Parameters
  -----------
    spatial_extent : SpatialExtent
      Spatial extent.
    temporal_extent : TemporalExtent
      Temporal extent.
    spatial_resolution : :obj:`list`
      Spatial resolution of the cube. Should be given as a list in the format
      `[y, x]`, where y is the cell size along the y-axis, x is the cell size
      along the x-axis, and both are given as :obj:`int` or :obj:`float`
      value expressed in the units of the CRS. These values should include
      the direction of the axes. For most CRSs, the y-axis has a negative
      direction, and hence the cell size along the y-axis is given as a
      negative number.
    temporal_resolution : :obj:`str` or :obj:`pandas.tseries.offsets.DateOffset`
        Temporal resolution of the cube. Can be given as offset alias as
        defined in pandas, e.g. "D" for a daily frequency. These aliases can
        have multiples, e.g. "5D". If :obj:`None`, only the start and end
        instants of the extent will be temporal coordinates in the cube.
    crs : optional
      Coordinate reference system in which the spatial coordinates of the cube
      should be expressed. Can be given as any object understood by the
      initializer of :class:`pyproj.crs.CRS`. This includes
      :obj:`pyproj.crs.CRS` objects themselves, as well as EPSG codes and WKT
      strings. If :obj:`None`, the CRS of the provided spatial extent is used.
    tz : optional
      Timezone in which the temporal coordinates of the cube should be
      expressed. Can be given as :obj:`str` referring to the name of a time
      zone in the tz database, or as instance of any class inheriting from
      :class:`datetime.tzinfo`. If :obj:`None`, the timezone of the provided
      temporal extent is used.
    trim : :obj:`bool`
      Should the cube be trimmed before returning? Trimming means that all
      coordinates for which all values are nodata, are dropped from the array.
      The spatial dimension (if present) is treated differently, by trimming
      it only at the edges, and thus maintaining the regularity of the spatial
      dimension.

  Returns
  -------
    :obj:`xarray.DataArray`
      A two-dimensional data cube with a spatial and temporal dimension. The
      spatial dimension is a stacked dimension with each coordinate value
      being a tuple of the x and y coordinate of the corresponding cell.

  """
  # Rasterize spatial extent.
  space = spatial_extent.rasterize(spatial_resolution, crs, stack = True)
  # Add spatial feature indices as coordinates.
  space.coords["feature"] = ("space", space.data)
  space["feature"].name = "feature"
  space["feature"].sq.value_type = space.sq.value_type
  space["feature"].sq.value_labels = space.sq.value_labels
  # Discretize temporal extent.
  time = temporal_extent.discretize(temporal_resolution, tz)
  # Combine rasterized spatial extent with discretized temporal extent.
  extent = space.expand_dims({"time": time})
  extent["time"].sq.value_type = "datetime"
  # Add temporal reference.
  extent = extent.sq.write_tz(time.sq.tz)
  # Trim the extent cube if requested.
  # This means we drop all x, y and time slices for which all values are nan.
  if trim:
    extent = extent.sq.trim()
  return extent

def parse_datetime_component(name, obj):
  """Parse datetime accessor arrays.

  The `datetime accessors`_ of :obj:`xarray.DataArray` objects are treated in
  semantique as a component of the temporal dimension. Parsing them includes
  adding :attr:`value_type <semantique.processor.structures.Cube.value_type>`
  and :attr:`value_label <semantique.processor.structures.Cube.value_labels>`
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
      "Monday": 0,
      "Tuesday": 1,
      "Wednesday": 2,
      "Thursday": 3,
      "Friday": 4,
      "Saturday": 5,
      "Sunday": 6
    }
  elif name == "quarter":
    obj.sq.value_type = "ordinal"
    obj.sq.value_labels = {
      "January, February, March": 1,
      "May, April, June": 2,
      "July, August, September": 3,
      "October, November, December": 4
    }
  elif name == "season":
    # In xarray seasons get stored as strings.
    # We want to store them as integers instead.
    for k, v in zip(["MAM", "JJA", "SON", "DJF"], [1, 2, 3, 4]):
      obj = obj.str.replace(k, str(v))
    obj = obj.astype(int)
    obj.sq.value_type = "ordinal"
    obj.sq.value_labels = {
      "March, April, May": 1,
      "June, July, August": 2,
      "September, October, November": 3,
      "December, January, February": 4
    }
  else:
    obj.sq.value_type = "numerical"
  return obj

def parse_coords_component(obj):
  """Parse spatial coordinate arrays.

  The spatial x and y coordinates of each pixel in a data cube are treated in
  semantique as a component of the spatial dimension. Parsing them includes
  adding a relevant
  :attr:`value_type <semantique.processor.structures.Cube.value_type>`
  property.

  Parameters
  -----------
    obj : :obj:`xarray.DataArray`
      Array containing spatial coordinates.

  Returns
  --------
    :obj:`xarray.DataArray`
      Parsed spatial coordinate array.

  """
  obj.sq.value_type = "numerical"
  return obj