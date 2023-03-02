import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr

import copy
import geocube.api.core
import pytz
import rioxarray

from pyproj.crs import CRS
from functools import partial
from geocube.rasterize import rasterize_image

from semantique import exceptions

class SpatialExtent(dict):
  """Dict-like representation of a spatial extent.

  Parameters
  ----------
    obj
      One or more spatial features that together form the boundaries of the
      spatial extent. Should be given as an object that can be read by the
      initializer of :class:`geopandas.GeoDataFrame`. This includes
      :obj:`geopandas.GeoDataFrame` objects themselves.
    **kwargs
      Additional keyword arguments forwarded to the initializer of
      :class:`geopandas.GeoDataFrame`.

  """

  def __init__(self, obj, **kwargs):
    if isinstance(obj, SpatialExtent):
      obj = obj.features
    geodf = gpd.GeoDataFrame(obj, **kwargs)
    geojs = geodf.__geo_interface__
    crs = geodf.crs
    if crs is None:
      crs = CRS.from_epsg(4326)
    geojs["crs"] = crs.to_string()
    self._features = geodf
    self._crs = crs
    super(SpatialExtent, self).__init__(geojs)

  @property
  def __geo_interface__(self):
    return self

  @property
  def features(self):
    """:obj:`geopandas.GeoDataFrame`: Spatial features comprising the spatial
    extent."""
    return self._features

  @property
  def crs(self):
    """:obj:`pyproj.crs.CRS`: Coordinate reference system in which the spatial
    coordinates are expressed."""
    return self._crs

  @classmethod
  def from_geojson(cls, obj, **kwargs):
    """Alternative initialization from any GeoJSON dictionary.

    Parameters
    ----------
      obj : :obj:`dict`
        Dictionary formatted according to GeoJSON standards.
      **kwargs
        Additional keyword arguments forwarded to
        :meth:`geopandas.GeoDataFrame.from_features`.

    """
    t = obj["type"]
    if t == "FeatureCollection":
      return cls.from_featurecollection(obj, **kwargs)
    elif t == "Feature":
      return cls.from_feature(obj, **kwargs)
    else:
      return cls.from_geometry(obj, **kwargs)

  @classmethod
  def from_featurecollection(cls, obj, **kwargs):
    """Alternative initialization from a GeoJSON FeatureCollection object.

    Parameters
    ----------
      obj : :obj:`dict`
        Dictionary formatted according to GeoJSON standards for
        FeatureCollection objects.
      **kwargs
        Additional keyword arguments forwarded to
        :meth:`geopandas.GeoDataFrame.from_features`.

    """
    geojs = copy.deepcopy(obj)
    def _assure_properties_key(x):
      if "properties" not in x:
        x["properties"] = {}
      return x
    feats = [_assure_properties_key(x) for x in geojs["features"]]
    geodf = gpd.GeoDataFrame.from_features(feats, **kwargs)
    return cls(geodf)

  @classmethod
  def from_feature(cls, obj, **kwargs):
    """Alternative initialization from a GeoJSON Feature object.

    Parameters
    ----------
      obj : :obj:`dict`
        Dictionary formatted according to GeoJSON standards for Feature
        objects.
      **kwargs
        Additional keyword arguments forwarded to
        :meth:`geopandas.GeoDataFrame.from_features`.

    """
    geojs = copy.deepcopy(obj)
    if "properties" not in geojs:
      geojs["properties"] = {}
    geodf = gpd.GeoDataFrame.from_features([geojs], **kwargs)
    return cls(geodf)

  @classmethod
  def from_geometry(cls, obj, **kwargs):
    """Alternative initialization from a GeoJSON Geometry object.

    Parameters
    ----------
      obj : :obj:`dict`
        Dictionary formatted according to GeoJSON standards for Geometry
        objects.
      **kwargs
        Additional keyword arguments forwarded to
        :meth:`geopandas.GeoDataFrame.from_features`.

    """
    geojs = {"type": "Feature", "geometry": obj, "properties": {}}
    geodf = gpd.GeoDataFrame.from_features([geojs], **kwargs)
    return cls(geodf)

  def rasterize(self, resolution, crs = None):
    """Rasterize the spatial extent into an array.

    Rasterizing the spatial extent creates a rectangular two-dimensional
    regular grid that covers the bounding box of the extent. Each grid cell
    corresponds to a spatial location within this bounding box. Cells whose
    centroid does not intersect with the extent itself gets assigned a value
    of 0. All other cells get assigned a positive integer, depending on which
    of the features in the extent their centroid intersects with, i.e. a 1 if
    it intersects with the first feature in the extent, a 2 if it intersects
    with the second feature in the extent, et cetera.

    Parameters
    ----------
      resolution : :obj:`list`
        Spatial resolution of the grid. Should be given as a list in the format
        `[y, x]`, where y is the cell size along the y-axis, x is the cell size
        along the x-axis, and both are given as :obj:`int` or :obj:`float`
        value expressed in the units of the CRS. These values should include
        the direction of the axes. For most CRSs, the y-axis has a negative
        direction, and hence the cell size along the y-axis is given as a
        negative number.
      crs : optional
        Coordinate reference system in which the grid should be created. Can be
        given as any object understood by the initializer of
        :class:`pyproj.crs.CRS`. This includes :obj:`pyproj.crs.CRS` objects
        themselves, as well as EPSG codes and WKT strings. If :obj:`None`, the
        CRS of the extent itself is used.

    Returns
    -------
      :obj:`xarray.DataArray`

    """
    # Use CRS of extent if no output CRS is given.
    if crs is None:
      crs = self.crs
    else:
      crs = CRS(crs)
    # Rasterize.
    vector_obj = self._features.reset_index()
    vector_obj["index"] = vector_obj["index"] + 1
    # select intersection method based on the geometry types
    # We want to use the touch intersection method for Polygons
    # and Multipolygons
    use_centroid_intersection = not any(
      gtype in ["Polygon", "MultiPolygon"] for gtype in vector_obj.geometry.geom_types.values
    )
    raster_obj = geocube.api.core.make_geocube(
      vector_data = vector_obj,
      measurements = ["index"],
      output_crs = crs,
      resolution = tuple(resolution),
      rasterize_function = partial(rasterize_image, all_touched=use_centroid_intersection),
    )["index"]
    # Update spatial reference.
    # CRS information was already automatically included during rasterizing.
    # In addition we add the GeoTransform attribute.
    # See https://gdal.org/tutorials/geotransforms_tut.html
    raster_obj = raster_obj.rio.write_transform()
    # Write semantique specific attributes.
    indices = vector_obj["index"]
    try:
      names = vector_obj["name"]
    except KeyError:
      names = ["feature_" + str(i) for i in indices]
    raster_obj.sq.value_type = "nominal"
    raster_obj.sq.value_labels = {k:v for k, v in zip(indices, names)}
    return raster_obj

class TemporalExtent(dict):
  """Dict-like representation of a temporal extent.

  Parameters
  ----------
    *bounds
      Boundaries of the temporal extent. Should be given as objects that can be
      read by the initializer of :class:`pandas.Timestamp`. This includes
      :obj:`pandas.Timestamp` objects themselves, as well as text
      representations of time instants in different formats. If the temporal
      extent is a single time instant, a single boundary may be given. If the
      temporal extent is a time interval, the boundaries should be provided
      as `(start, end)`. This interval is assumed to be closed at both sides.
    **kwargs
      Additional keyword arguments forwarded to the initializer of
      :class:`pandas.Timestamp`.

  Raises
  -------
    :obj:`exceptions.MixedTimeZonesError`
      If the given boundaries have differing timezone information attached.

  """

  def __init__(self, *bounds, **kwargs):
    interval = True # By default assume given bounds form an interval.
    # Parse bounds.
    if len(bounds) > 1:
      if isinstance(bounds[0], TemporalExtent):
        start = pd.Timestamp(bounds[0].start, **kwargs)
      else:
        start = pd.Timestamp(bounds[0], **kwargs)
      if isinstance(bounds[-1], TemporalExtent):
        end = pd.Timestamp(bounds[-1].end, **kwargs)
      else:
        end = pd.Timestamp(bounds[-1], **kwargs)
    else:
      if isinstance(bounds[0], TemporalExtent):
        start = pd.Timestamp(bounds[0].start, **kwargs)
        end = pd.Timestamp(bounds[0].end, **kwargs)
      else:
        start = pd.Timestamp(bounds[0], **kwargs)
        end = start
        interval = False
    # Check if timezones of bounds are matching.
    if interval and start.tz != end.tz:
        raise exceptions.MixedTimeZonesError(
          f"Time interval has bounds with differing timezones: "
          f"'{start.tz.zone}' and '{end.tz.zone}'"
        )
    # Assign default timezone UTC if timezone is not known.
    if start.tz is None:
      start = start.tz_localize(pytz.utc)
      end = end.tz_localize(pytz.utc)
    # Construct dict representation of the temporal extent.
    if interval:
      timejs = {
        "type": "Interval",
        "start": start.tz_localize(None).isoformat(),
        "end": end.tz_localize(None).isoformat(),
        "tz": start.tz.zone
      }
    else:
      timejs = {
        "type": "Instant",
        "start": start.tz_localize(None).isoformat(),
        "tz": start.tz.zone
      }
    self._start = start
    self._end = end
    self._tz = start.tz
    super(TemporalExtent, self).__init__(timejs)

  @property
  def tz(self):
    """:obj:`datetime.tzinfo`: Timezone of the time instants."""
    return self._tz

  @property
  def start(self):
    """:obj:`pandas.Timestamp`: Lower bound of the temporal extent."""
    return self._start

  @property
  def end(self):
    """:obj:`pandas.Timestamp`: Upper bound of the temporal extent."""
    return self._end

  def discretize(self, resolution = None, tz = None):
    """Discretize the temporal extent into an array.

    Discretizing the temporal extent creates a one-dimensional regular grid
    that covers the entire temporal extent. Each grid cell corresponds to a
    single time instant within this extent, and gets the :obj:`numpy.datetime64`
    object belonging to that instant assigned as value.

    Parameters
    ----------
      resolution : :obj:`str` or :obj:`pandas.tseries.offsets.DateOffset`
        Temporal resolution of the grid. Can be given as offset alias as
        defined in `pandas`_, e.g. "D" for a daily frequency. These aliases can
        have multiples, e.g. "5D". If :obj:`None`, only the bounds of the
        extent will be coordinates in the grid.
      tz : optional
        Timezone of the datetime values in the grid. Can be given as :obj:`str`
        referring to the name of a timezone in the tz database, or as instance
        of any class inheriting from :class:`datetime.tzinfo`. If :obj:`None`,
        the timezone of the extent is used.

    Returns
    -------
      :obj:`xarray.DataArray`

    .. _pandas:
      https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html

    """
    if resolution is None:
      range_obj = pd.date_range(self.start, self.end, periods = 2)
    else:
      range_obj = pd.date_range(self.start, self.end, freq = resolution)
    if tz is None:
      tz = self.tz
    range_obj = range_obj.tz_convert(tz)
    range_obj = [np.datetime64(x) for x in range_obj.tz_localize(None)]
    array_obj = xr.DataArray(range_obj, dims = ["time"], coords = [range_obj])
    array_obj.sq.value_type = "datetime"
    array_obj = array_obj.sq.write_tz(tz)
    return array_obj