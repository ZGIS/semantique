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
  def value_labels(self):
    try:
      return self._obj.attrs["value_labels"]
    except KeyError:
      return None

  @value_labels.setter
  def value_labels(self, value):
    self._obj.attrs["value_labels"] = value

  @value_labels.deleter
  def value_labels(self):
    try:
      del self._obj.attrs["value_labels"]
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

  def evaluate(self, operator, y = None, track_types = False, **kwargs):
    operands = tuple([self._obj]) if y is None else tuple([self._obj, y])
    out = operator(*operands, track_types = track_types, **kwargs)
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
          raise exceptions.UnknownComponentError(
            f"Component '{component}' "
            f"is not defined for dimension '{dimension}'"
          )
        else:
          out = utils.parse_datetime_component(component, out)
      else:
        if component in self.xy_dimensions:
          out.sq.value_type = "numerical"
    return out

  def filter(self, filterer, trim = True, track_types = False, **kwargs):
    if track_types:
      vtype = filterer.sq.value_type
      if vtype is not None and vtype != "binary":
        raise exceptions.InvalidValueTypeError(
          f"Filterer must be of value type 'binary', not '{vtype}'"
        )
    out = self._obj.where(filterer.sq.align_with(self._obj))
    if trim:
      out = out.sq.trim()
    return out

  def groupby(self, grouper, **kwargs):
    # Validate grouper.
    dims = [x.dims for x in list(grouper)]
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
    # Split input object into groups.
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

  def reduce(self, dimension, reducer, track_types = False, **kwargs):
    out = reducer(self._obj, dimension, track_types = track_types, **kwargs)
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

  def promote_value_type(self, *operands, func, manual, inplace = True):
    out = self._obj if inplace else copy.deepcopy(self._obj)
    if manual is None:
      out.sq.value_type = None
      out.sq.value_labels = None
      return out
    # Update value type.
    # Based on value types of the operands and the type promotion manual.
    def _get_type(x):
      try:
        return x.attrs["value_type"]
      except AttributeError:
        return np.array(x).dtype.kind
      except KeyError:
        return x.dtype.kind
    intypes = [_get_type(x) for x in operands]
    outtype = manual # Initialize before scanning.
    for x in intypes:
      try:
        outtype = outtype[x]
      except KeyError:
        raise exceptions.InvalidValueTypeError(
          f"Unsupported operand value type(s) for '{func}': '{intypes}'"
        )
    out.sq.value_type = outtype
    # Update value labels.
    # The manual has a special key to define if labels should be preserved.
    # If True the labels of the first operand should be preserved.
    try:
      preserve_labels = manual["__preserve_labels__"]
    except KeyError:
      preserve_labels = False
    if preserve_labels:
      out.sq.value_labels = operands[0].sq.value_labels
    else:
      del out.sq.value_labels
    return out

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

  def compose(self, track_types = False, **kwargs):
    if track_types:
      value_types = [x.sq.value_type for x in self]
      if not all([x is None or x == "binary" for x in value_types]):
        raise exceptions.InvalidValueTypeError(
          f"Element value types for 'compose' should all be 'binary', "
          f"not {np.unique(value_types).tolist()} "
        )
    def index_(idx, obj):
      return xr.where(obj, idx + 1, np.nan).where(obj.notnull())
    enumerated = enumerate(self)
    indexed = [index_(i, x) for i, x in enumerated]
    out = indexed[0]
    for x in indexed[1:]:
      out = out.combine_first(x)
    labels = [x.name for x in self]
    idxs = range(1, len(labels) + 1)
    out.sq.value_type = "nominal"
    out.sq.value_labels = {k:v for k, v in zip(labels, idxs)}
    return out

  def concatenate(self, dimension, track_types = False,
                  vtype = "nominal", **kwargs):
    if track_types:
      value_types = [x.sq.value_type for x in self]
      if not all([x is None or x == value_types[0] for x in value_types]):
        raise exceptions.InvalidValueTypeError(
          f"Element value types for 'concatenate' should all be the same, "
          f"not {np.unique(value_types).tolist()} "
        )
    if dimension in self[0].dims:
      # Concatenate over existing dimension.
      raw = xr.concat([x for x in self], dimension)
      coords = raw.get_index(dimension)
      clean = raw.isel({dimension: np.invert(coords.duplicated())})
      out = clean.sortby(dimension)
    else:
      # Concatenate over new dimension.
      labels = [x.name for x in self]
      coords = pd.Index(labels, name = dimension, tupleize_cols = False)
      out = xr.concat([x for x in self], coords)
      out[dimension].sq.value_type = vtype
      out[dimension].sq.value_labels = {x:x for x in labels}
    # PROVISIONAL FIX: Always drop value labels of the concatenated cube.
    # Value labels are preserved from the first element in the collection.
    # These may not be accurate anymore for the concatenated cube.
    # TODO: Find a way to merge value labels. But how to handle duplicates?
    del out.sq.value_labels
    return out

  def merge(self, reducer, track_types = False, **kwargs):
    dim = "__sq__" # Temporary dimension.
    concat = self.concatenate(dim, track_types)
    out = concat.sq.reduce(dim, reducer, track_types, **kwargs)
    return out

  def evaluate(self, operator, y = None, track_types = False, **kwargs):
    args = tuple([operator, y, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sq.evaluate(*args, **kwargs) for x in out]
    return out

  def extract(self, dimension, component = None, **kwargs):
    args = tuple([dimension, component])
    out = copy.deepcopy(self)
    out[:] = [x.sq.extract(*args, **kwargs) for x in out]
    return out

  def filter(self, filterer, trim = True, track_types = False, **kwargs):
    args = tuple([filterer, trim, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sq.filter(*args, **kwargs) for x in out]
    return out

  def label(self, label, **kwargs):
    out = copy.deepcopy(self)
    out[:] = [x.sq.label(label, **kwargs) for x in out]
    return out

  def reduce(self, dimension, reducer, track_types = False, **kwargs):
    args = tuple([dimension, reducer, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sq.reduce(*args, **kwargs) for x in out]
    return out

  def regularize(self):
    out = copy.deepcopy(self)
    out[:] = [x.sq.regularize() for x in out]
    return out