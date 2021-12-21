import numpy as np
import pandas as pd
import xarray as xr

import copy
import pytz
import rasterio
import rioxarray
import warnings

from semantique import exceptions
from semantique.processor import utils

@xr.register_dataarray_accessor("sq")
class Cube():

  def __init__(self, xarray_obj):
    self._obj = xarray_obj

  @property
  def value_type(self):
    try:
      return self._obj.attrs["value_type"]
    except KeyError:
      return None

  @value_type.setter
  def value_type(self, value):
    self._obj.attrs["value_type"] = value

  @value_type.deleter
  def value_type(self):
    try:
      del self._obj.attrs["value_type"]
    except KeyError:
      pass

  @property
  def categories(self):
    try:
      return self._obj.attrs["categories"]
    except KeyError:
      return None

  @categories.setter
  def categories(self, value):
    self._obj.attrs["categories"] = value

  @categories.deleter
  def categories(self):
    try:
      del self._obj.attrs["categories"]
    except KeyError:
      pass

  @property
  def crs(self):
    return self._obj.rio.crs

  @property
  def spatial_resolution(self):
    return self._obj.sq.unstack_spatial_dims().rio.resolution()[::-1]

  @property
  def tz(self):
    try:
      return pytz.timezone(self._obj["temporal_ref"].attrs["zone"])
    except KeyError:
      return None

  @property
  def extent(self):
    time = self.temporal_dimension
    space = self.spatial_dimension
    if time is None:
      if space is None:
        out = xr.DataArray([])
      else:
        out = self._obj[space]["feature"]
    else:
      if space is None:
        out = self._obj[time]
        out.sq.value_type = "time"
      else:
        out = self._obj[space]["feature"].expand_dims({"time": self._obj[time]})
    return out

  @property
  def is_empty(self):
    return self._obj.values.size == 0 or self._obj.max() != self._obj.max()

  @ property
  def temporal_dimension(self):
    if "time" in self._obj.dims:
      return "time"
    else:
      return None

  @property
  def spatial_dimension(self):
    if "space" in self._obj.dims:
      return "space"
    else:
      return None

  @property
  def xy_dimensions(self):
    candidates = [
      ["x", "y"],
      ["X", "Y"],
      ["longitude", "latitude"],
      ["lon", "lat"],
    ]
    for xy in candidates:
      if all([dim in self._obj.unstack().dims for dim in xy]):
        return xy
    return None

  def evaluate(self, operator, y = None, type_promotion = None, **kwargs):
    operands = tuple([self._obj]) if y is None else tuple([self._obj, y])
    out = operator(*operands, **kwargs)
    if type_promotion is not None:
      func = operator.__name__[:-1]
      out.sq._promote_types(*operands, manual = type_promotion, name = func)
    return out

  def extract(self, dimension, component = None, **kwargs):
    try:
      coords = self._obj[dimension]
    except KeyError:
      raise exceptions.MissingDimensionError(
        f"Dimension '{dimension}' is not defined"
      )
    if component is None:
      out = coords
    else:
      try:
        out = coords[component]
      except KeyError:
        try:
          out = getattr(coords.dt, component)
        except AttributeError:
          raise exceptions.UndefinedDimensionComponentError(
            f"Component '{component}' "
            f"is not defined for dimension '{dimension}'"
          )
        else:
          out = self._parse_datetime_component(out, component)
      else:
        if component in self.xy_dimensions:
          out.sq.value_type = "numerical"
    return out

  def filter(self, filterer, trim = True, **kwargs):
    out = self._obj.where(filterer.sq.align_with(self._obj))
    if trim:
      out = out.sq.trim()
    return out

  def groupby(self, grouper, **kwargs):
    self._validate_grouper(grouper, self._obj)
    if isinstance(grouper, list):
      idx = pd.MultiIndex.from_arrays([x.data for x in grouper])
      dim = grouper[0].dims
      partition = list(self._obj.groupby(xr.IndexVariable(dim, idx)))
    else:
      partition = list(self._obj.groupby(grouper))
    out = CubeCollection([i[1].sq.label(i[0]) for i in partition])
    return out

  def label(self, label, **kwargs):
    out = self._obj.rename(label)
    return out

  def reduce(self, dimension, reducer, type_promotion = None, **kwargs):
    out = reducer(self._obj, dimension, **kwargs)
    if type_promotion is not None:
      func = reducer.__name__[:-1]
      out.sq._promote_types(self._obj, manual = type_promotion, name = func)
    return out

  def replace(self, y, type_promotion = None, **kwargs):
    try:
      y = y.sq.align_with(self._obj)
    except AttributeError:
      y = np.array(y)
    nodata = np.datetime64("NaT") if y.dtype.kind == "M" else np.nan
    out = xr.where(np.isfinite(self._obj), y, nodata)
    if type_promotion is not None:
      func = "replace"
      out.sq._promote_types(self._obj, y, manual = type_promotion, name = func)
    return out

  def align_with(self, other):
    out = xr.align(other, self._obj, join = "left")[1].broadcast_like(other)
    if not out.shape == other.shape:
      raise exceptions.AlignmentError(
        f"Cube '{other.name if other.name is not None else 'y'}' "
        f"cannot be aligned with "
        f"input cube '{self._obj.name if self._obj.name is not None else 'y'}'"
      )
    return out

  def trim(self):
    out = self._obj
    all_dims = out.dims
    for dim in all_dims:
      other_dims = [d for d in all_dims if d != dim]
      out = out.isel({dim: out.count(other_dims) > 0})
    return out

  def regularize(self):
    space = self.spatial_dimension
    if space is None:
      return self._obj
    obj = self._obj.unstack(space)
    res = self.spatial_resolution
    # Extract x and y dimensions.
    xydims = self.xy_dimensions
    yname = xydims[1]
    ycoords = obj[yname]
    xname = xydims[0]
    xcoords = obj[xname]
    # Update x and y dimensions.
    ycoords = np.arange(ycoords[0], ycoords[-1] + res[0], res[0])
    xcoords = np.arange(xcoords[0], xcoords[-1] + res[1], res[1])
    out = obj.reindex({yname: ycoords, xname: xcoords})
    return out.stack({space: [yname, xname]})

  def reproject(self, crs, **kwargs):
    obj = self.unstack_spatial_dims()
    obj = obj.sq.drop_non_dimension_coords(keep = ["spatial_ref"])
    out = obj.rio.reproject(crs, **kwargs).sq.stack_spatial_dims()
    return out.sq.write_tz(self.tz)

  def tz_convert(self, tz, **kwargs):
    dim = self.temporal_dimension
    src = self._obj[dim].data
    trg = [utils.convert_datetime64(x, self.tz, tz, **kwargs) for x in src]
    out = self._obj.assign_coords({dim: trg}).sq.write_tz(tz)
    return out

  def write_crs(self, crs, inplace = False):
    return self._obj.rio.write_crs(crs, inplace = inplace)

  def write_tz(self, tz, inplace = False):
    obj = self._obj if inplace else copy.deepcopy(self._obj)
    try:
      zone = tz.zone
    except AttributeError:
      zone = pytz.timezone(tz).zone
    obj["temporal_ref"] = 0
    obj["temporal_ref"].attrs["zone"] = zone
    return obj

  def stack_spatial_dims(self):
    xy_dims = self.xy_dimensions
    if xy_dims is None:
      return self._obj
    return self._obj.stack(space = xy_dims[::-1])

  def unstack_spatial_dims(self):
    dim = self.spatial_dimension
    if dim is None:
      return self._obj
    return self._obj.unstack(dim)

  def drop_non_dimension_coords(self, keep = None):
    if keep is None:
      drop = set(self._obj.coords) - set(self._obj.dims)
    else:
      drop = set(self._obj.coords) - set(self._obj.dims) - set(keep)
    return self._obj.reset_coords(drop, drop = True)

  def to_csv(self, file, **kwargs):
    obj = self.drop_non_dimension_coords()
    # to_dataframe method does not work for zero-dimensional arrays.
    if len(self.dims) == 0:
      pd.DataFrame([obj.values]).to_csv(file, header = False, index = False)
    else:
      obj.to_dataframe().to_csv(file)
    return file

  def to_geotiff(self, file, cloud_optimized = True, compress = True,
                 output_crs = None, **kwargs):
    # Make sure spatial dimensions are present.
    if self.spatial_dimension is None:
      raise exceptions.MissingDimensionError(
        "GeoTIFF export requires a spatial dimension"
      )
    obj = self.unstack_spatial_dims()
    # Remove non-dimension coordinates but not 'spatial_ref'.
    # That one is needed by rioxarray to determine the CRS of the data.
    obj = obj.sq.drop_non_dimension_coords(keep = ["spatial_ref"])
    # Reproject data if requested.
    if output_crs is not None:
      obj = obj.rio.reproject(output_crs)
    # Initialize GDAL configuration parameters.
    config = {}
    # GDAL has limited support for numpy dtypes.
    # Therefore dtype conversion might be needed in some cases.
    dtype = obj.dtype
    if not rasterio.dtypes.check_dtype(dtype):
      dtype = rasterio.dtypes.get_minimum_dtype(obj)
    config["dtype"] = dtype
    # GDAL support COG export only since version 3.1.
    if cloud_optimized:
      if rasterio.__gdal_version__ < "3.1":
        config["driver"] = "GTiff"
        warnings.warn(
          f"Cloud optimized export is not supported by GDAL version "
          f"{rasterio.__gdal_version__}, written to regular GeoTIFF instead"
        )
      else:
        config["driver"] = "COG"
    else:
      config["driver"] = "GTiff"
    # Use LZW compression if compression is requested by user.
    if compress:
      config["compress"] = "LZW"
    # Write data to file.
    try:
      obj.rio.to_raster(file, **config)
    except rioxarray.exceptions.TooManyDimensions:
      ndims = len(obj.dims) - 2
      raise exceptions.TooManyDimensionsError(
        f"GeoTIFF export is only supported for 2D or 3D arrays, not {ndims}D"
      )
    return file

  def _promote_types(self, *vars, manual, name, inplace = True):
    out = self._obj if inplace else copy.deepcopy(self._obj)
    def _get_type(x):
      try:
        vtype = x.sq.value_type
      except AttributeError:
        vtype = None
      return np.array(x).dtype.kind if vtype is None else vtype
    intypes = [_get_type(x) for x in vars]
    outtype = manual # Initialize before scanning.
    for x in intypes:
      try:
        outtype = outtype[x]
      except KeyError:
        raise exceptions.InvalidTypePromotionError(
          f"Unsupported operand value type(s) for "
          f"'{name}': '{intypes}'"
        )
    out.sq.value_type = outtype
    if outtype in ["nominal", "ordinal"]:
      out.sq.categories = self.categories
    else:
      del out.sq.categories
    return out

  @staticmethod
  def _validate_grouper(grouper, obj):
    if not isinstance(grouper, list):
      grouper = [grouper]
    try:
      dims = [x.dims for x in grouper]
    except AttributeError:
      raise TypeError(
        "Groupers must be arrays"
      )
    if not all([len(x) == 1 for x in dims]):
      raise exceptions.TooManyDimensionsError(
        "Groupers must be one-dimensional"
      )
    if not all([x == dims[0] for x in dims]):
      raise exceptions.UnmatchingDimensionsError(
        "Dimensions of grouper arrays do not match"
      )
    if not dims[0][0] in obj.dims:
      raise exceptions.MissingDimensionError(
        f"Grouper dimension '{dims[0]}' does not exist in the input object"
      )

  @staticmethod
  def _parse_datetime_component(obj, name):
    if name in ["dayofweek", "weekday"]:
      obj.sq.value_type = "ordinal"
      obj.sq.categories = {
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
      obj.sq.categories = {
        "JFM": 1,
        "AMJ": 2,
        "JAS": 3,
        "OND": 4
      }
    elif name == "season":
      categories = {
        "DJF": 1,
        "MAM": 2,
        "JJA": 3,
        "SON": 4
      }
      for k, v in categories.items():
        obj = obj.str.replace(k, str(v))
      obj = obj.astype(int)
      obj.sq.value_type = "ordinal"
      obj.sq.categories = categories
    else:
      obj.sq.value_type = "numerical"
    return obj

class CubeCollection(list):

  def __init__(self, list_obj):
    super(CubeCollection, self).__init__(list_obj)

  @property
  def extent(self):
    named = [xr.DataArray(self[i], name = i) for i in range(len(self))]
    merged = xr.merge(named, compat = "override", join = "outer")
    return merged[0].sq.extent

  @property
  def is_empty(self):
    return all([x.sq.is_empty for x in self])

  def compose(self, track_types = True, **kwargs):
    if track_types:
      value_types = [x.sq.value_type for x in self]
      if not all([x == "binary" for x in value_types]):
        raise exceptions.InvalidTypePromotionError(
          f"Unsupported operand value type(s) for 'compose': "
          f"{np.unique(value_types).tolist()} "
          f"Should all be binary"
        )
    def index_(idx, obj):
      return xr.where(obj, idx + 1, np.nan).where(obj.notnull())
    enumerated = enumerate(self)
    indexed = [index_(i, x) for i, x in enumerated]
    out = indexed[0]
    for x in indexed[1:]:
      out = out.combine_first(x)
    if track_types:
      out.sq.value_type = "nominal"
      names = [x.name for x in self]
      idxs = range(1, len(names) + 1)
      out.sq.categories = {k:v for k, v in zip(names, idxs)}
    return out

  def concatenate(self, dimension, track_types = True, **kwargs):
    if track_types:
      value_types = [x.sq.value_type for x in self]
      if not all([x == value_types[0] for x in value_types]):
        raise exceptions.MixedValueTypesError(
          f"Cubes to be concatenated have differing value types: "
          f"{np.unique(value_types).tolist()}"
        )
    if dimension in self[0].dims:
      raw = xr.concat([x for x in self], dimension)
      coords = raw.get_index(dimension)
      clean = raw.isel({dimension: np.invert(coords.duplicated())})
      out = clean.sortby(dimension)
    else:
      labels = [x.name for x in self]
      coords = pd.Index(labels, name = dimension, tupleize_cols = False)
      out = xr.concat([x for x in self], coords)
      if track_types:
        out[dimension].sq.value_type = "nominal" # Thematic dimension.
        out[dimension].sq.categories = {x:x for x in labels}
    if track_types and out.sq.value_type == "nominal":
      out.sq.categories = None
    return out

  def merge(self, reducer, track_types = True, type_promotion = None, **kwargs):
    concatenated = self.concatenate("__sq__", track_types = track_types)
    out = concatenated.sq.reduce("__sq__", reducer, type_promotion, **kwargs)
    return out

  def evaluate(self, operator, y = None, type_promotion = None, **kwargs):
    out = copy.deepcopy(self)
    out[:] = [x.sq.evaluate(operator, y, type_promotion, **kwargs) for x in out]
    return out

  def filter(self, filterer, trim = True, **kwargs):
    out = copy.deepcopy(self)
    out[:] = [x.sq.filter(filterer, trim, **kwargs) for x in out]
    return out

  def groupby(self, grouper, **kwargs):
    out = copy.deepcopy(self)
    out[:] = [x.sq.groupby(grouper, **kwargs) for x in out]
    return out

  def reduce(self, dimension, reducer, type_promotion = None, **kwargs):
    out = copy.deepcopy(self)
    out[:] = [x.sq.reduce(dimension, reducer, type_promotion, **kwargs) for x in out]
    return out

  def replace(self, y, type_promotion = None, **kwargs):
    out = copy.deepcopy(self)
    out[:] = [x.sq.replace(y, type_promotion, **kwargs) for x in out]
    return out

  def extract(self, dimension, component = None, **kwargs):
    out = copy.deepcopy(self)
    out[:] = [x.sq.extract(dimension, component, **kwargs) for x in out]
    return out

  def regularize(self):
    out = copy.deepcopy(self)
    out[:] = [x.sq.regularize() for x in out]
    return out