import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr

import copy
import geocube.api.core
import pytz

from pyproj.crs import CRS

from semantique import exceptions

class SpatialExtent(dict):

  def __init__(self, obj, **kwargs):
    geodf = gpd.GeoDataFrame(obj, **kwargs)
    geojs = geodf.__geo_interface__
    crs = geodf.crs
    if crs is None:
      crs = CRS.from_epsg(4326)
    geojs["crs"] = crs.to_json_dict()
    self._geodf = geodf
    self._crs = crs
    super(SpatialExtent, self).__init__(geojs)

  @property
  def __geo_interface__(self):
    return self

  @property
  def crs(self):
    return self._crs

  @classmethod
  def from_geojson(cls, obj, **kwargs):
    t = obj["type"]
    if t == "FeatureCollection":
      return cls.from_featurecollection(obj, **kwargs)
    if t == "Feature":
      return cls.from_feature(obj, **kwargs)
    geom_types = [
      "Point",
      "MultiPoint",
      "LineString",
      "MultiLineString",
      "Polygon",
      "MultiPolygon",
      "GeometryCollection",
    ]
    if not t in geom_types:
      raise exception.UnknownGeometryTypeError(
        f"Geometry type '{t}' is not supported"
      )
    return cls.from_geometry(obj, **kwargs)

  @classmethod
  def from_featurecollection(cls, obj, **kwargs):
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
    geojs = copy.deepcopy(obj)
    if "properties" not in geojs:
      geojs["properties"] = {}
    geodf = gpd.GeoDataFrame.from_features([geojs], **kwargs)
    return cls(geodf)

  @classmethod
  def from_geometry(cls, obj, **kwargs):
    geojs = {"type": "Feature", "geometry": obj, "properties": {}}
    geodf = gpd.GeoDataFrame.from_features([geojs], **kwargs)
    return cls(geodf)

  def rasterize(self, crs, resolution):
    vector_obj = self._geodf.reset_index()
    vector_obj["index"] = vector_obj["index"] + 1
    raster_obj = geocube.api.core.make_geocube(
      vector_data = vector_obj,
      measurements = ["index"],
      output_crs = crs,
      resolution = resolution,
    )["index"]
    indices = vector_obj["index"]
    try:
      names = vector_obj["name"]
    except KeyError:
      names = ["feature_" + str(i) for i in indices]
    raster_obj.sq.value_type = "categorical"
    raster_obj.sq.categories = {k:v for k, v in zip(names, indices)}
    return raster_obj

class TemporalExtent(dict):

  def __init__(self, *bounds, **kwargs):
    start = pd.Timestamp(bounds[0], **kwargs)
    end = pd.Timestamp(bounds[-1], **kwargs)
    tz = start.tz
    if tz is None:
      tz = pytz.utc
      start = start.tz_localize(tz)
      end = end.tz_localize(tz)
    if start == end:
      timejs = {
        "type": "Instant",
        "datetime": start.tz_localize(None).isoformat(),
        "tz": tz.zone
      }
    else:
      if tz != end.tz:
        raise exceptions.MixedTimeZonesError(
          f"Time interval has bounds with differing time zones: "
          f"'{tz.zone}' and '{end.tz.zone}'"
        )
      timejs = {
        "type": "Interval",
        "start": start.tz_localize(None).isoformat(),
        "end": end.tz_localize(None).isoformat(),
        "tz": tz.zone
      }
    self._start = start
    self._end = end
    self._tz = tz
    super(TemporalExtent, self).__init__(timejs)

  @property
  def tz(self):
    return self._tz

  @property
  def start(self):
    return self._start

  @property
  def end(self):
    return self._end

  def discretize(self, tz, resolution):
    range_obj = pd.date_range(self._start, self._end, freq = resolution)
    range_obj = range_obj.tz_convert(tz)
    range_obj = [np.datetime64(x) for x in range_obj.tz_localize(None)]
    array_obj = xr.DataArray(range_obj, dims = ["time"], coords = [range_obj])
    array_obj.sq.value_type = "time"
    array_obj = array_obj.sq.write_tz(tz)
    return array_obj