import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr

import copy
import pytz
import rasterio
import rioxarray
import warnings

from scipy import ndimage

from semantique import exceptions, components
from semantique.processor import operators, reducers, utils
from semantique.dimensions import TIME, SPACE, X, Y

@xr.register_dataarray_accessor("sq")
class Array():
  """Internal representation of a multi-dimensional array.
  To be used in conjunction with the `semantique.processor.QueryProcessor` or
  `semantique.processor.FakeProcessor`.

  This data structure is modelled as an accessor of :class:`xarray.DataArray`.
  Using accessors instead of the common class inheritance is recommended by the
  developers of xarray, see `here`_. In practice, this means that each method
  of this class can be called as method of :obj:`xarray.DataArray` objects by
  using the ``.sq`` prefix: ::

    xarray_obj.sq.method

  Parameters
  ----------
    xarray_obj : :obj:`xarray.DataArray`
      The content of the array.

  .. _here:
    https://xarray.pydata.org/en/stable/internals/extending-xarray.html

  """

  def __init__(self, xarray_obj):
    self._obj = xarray_obj

  #
  # PROPERTIES
  #

  @property
  def value_type(self):
    """:obj:`str`: The value type of the array."""
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
    """:obj:`dict`: Character labels of data values."""
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
    """:obj:`pyproj.crs.CRS`: Coordinate reference system in which the spatial
    coordinates of the array are expressed."""
    return self._obj.rio.crs

  @property
  def spatial_resolution(self):
    """:obj:`list`: Spatial resolution [x,y] of the array in units of the CRS."""
    try:
      return [self._obj[Y].attrs["resolution"], self._obj[X].attrs["resolution"]]
    except KeyError:
      return None

  @property
  def tz(self):
    """:obj:`datetime.tzinfo`: Time zone in which the temporal coordinates of
    the array are expressed."""
    try:
      return pytz.timezone(self._obj["temporal_ref"].attrs["zone"])
    except KeyError:
      return None

  @property
  def is_empty(self):
    """:obj:`bool`: Is the array empty."""
    try:
      return self._obj.values.size == 0 or not np.any(np.isfinite(self._obj))
    except TypeError:
      # For some value types np.isfinite cannot be applied.
      # This is the case e.g. for spatial coordinate tuples.
      # However, given that these occur it already means the array is not empty.
      return False

  @property
  def grid_points(self):
    """:obj:`geopandas.GeoSeries`: Spatial grid points of the array."""
    if X not in self._obj.dims or Y not in self._obj.dims:
      return self._obj
    # Extract coordinates of spatial pixels.
    cells = self.stack_spatial_dims()[SPACE]
    xcoords = cells[X]
    ycoords = cells[Y]
    # Convert to point geometries.
    points = gpd.points_from_xy(xcoords, ycoords)
    return gpd.GeoSeries(points, crs = self.crs)

  #
  # VERBS
  #

  def evaluate(self, operator, y = None, track_types = True, **kwargs):
    """Apply the evaluate verb to the array.

    The evaluate verb evaluates an expression for each pixel in an array.

    Parameters
    ----------
    operator : :obj:`callable`
      Operator function to be used in the expression.
    y : optional
      Right-hand side of the expression. May be a constant, meaning that the
      same value is used in each expression. May also be another array
      which can be aligned to the same shape as the input array. In the latter
      case, when evaluating the expression for a pixel in the input array the
      second operand is the value of the pixel in array ``y`` that has the same
      dimension coordinates. Ignored when the operator is univariate.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type(s) of the operand(s)?
    **kwargs:
      Additional keyword arguments passed on to the operator function.

    Returns
    --------
      :obj:`xarray.DataArray`

    """
    operands = tuple([self._obj]) if y is None else tuple([self._obj, y])
    out = operator(*operands, track_types = track_types, meta = False, **kwargs)
    return out

  def extract(self, dimension, component = None, **kwargs):
    """Apply the extract verb to the array.

    The extract verb extracts coordinate labels of a dimension as a new
    array.

    Parameters
    -----------
      dimension : :obj:`str`
        Name of the dimension to be extracted.
      component : :obj:`str`, optional
        Name of a specific component of the dimension coordinates to be
        extracted, e.g. *year*, *month* or *day* for temporal dimension
        coordinates.
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    -------
      :obj:`exceptions.UnknownDimensionError`
        If a dimension with the given name is not present in the array.
      :obj:`exceptions.UnknownComponentError`
        If the given dimension does not contain the given component.

    """
    # Get array.
    obj = self._obj
    # Extract spatial or temporal dimension(s).
    if dimension == TIME:
      return self._extract_time(obj, component)
    if dimension == SPACE:
      return self._extract_space(obj, component)
    # Extract any other dimension.
    try:
      out = obj[dimension]
    except KeyError:
      raise exceptions.UnknownDimensionError(
        f"Dimension '{dimension}' is not present in the array"
      )
    if component is not None:
      try:
        out = out[component]
      except KeyError:
        raise exceptions.UnknownComponentError(
          f"Component '{component}' is not defined for dimension '{dimension}'"
        )
    return out

  @staticmethod
  def _extract_space(obj, component = None):
    if component is None:
      try:
        out = obj.sq.stack_spatial_dims()[SPACE]
      except KeyError:
        raise exceptions.UnknownDimensionError(
          f"Spatial dimensions '{X}' and '{Y}' are not present in the array"
        )
      out._variable = out._variable.to_base_variable()
      out = out.sq.unstack_spatial_dims()
      out.sq.value_type = "coords"
    else:
      # Component FEATURE should extract spatial feature indices.
      if component == components.space.FEATURE:
        cname = "spatial_feats"
      else:
        cname = component
      try:
        out = obj[cname]
      except KeyError:
        raise exceptions.UnknownComponentError(
          f"Component '{cname}' is not defined for dimension '{SPACE}'"
        )
    return out

  @staticmethod
  def _extract_time(obj, component = None):
    try:
      out = obj[TIME]
    except KeyError:
      raise exceptions.UnknownDimensionError(
        f"Dimension '{TIME}' is not present in the array"
      )
    if component is not None:
      try:
        out = out[component]
      except KeyError:
        aliases = {
          "day_of_week": "dayofweek",
          "day_of_year": "dayofyear"
        }
        try:
          component = aliases[component]
        except KeyError:
          pass
        try:
          out = getattr(out.dt, component)
        except AttributeError:
          raise exceptions.UnknownComponentError(
            f"Component '{component}' is not defined for dimension '{TIME}'"
          )
        else:
          out = utils.parse_datetime_component(component, out)
    return out

  def filter(self, filterer, track_types = True, **kwargs):
    """Apply the filter verb to the array.

    The filter verb filters the values in an array.

    Parameters
    -----------
      filterer : :obj:`xarray.DataArray`
        Binary array which can be aligned to the same shape as the input array.
        Each pixel in the input array will be kept if the pixel in the filterer
        with the same dimension coordinates is true, and dropped otherwise
        (i.e. assigned a nodata value).
      track_types : :obj:`bool`
        Should it be checked that the filterer has value type *binary*?
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    -------
      :obj:`exceptions.InvalidValueTypeError`
        If ``track_types = True`` and the value type of ``filterer`` is not
        *binary*.

    """
    if track_types:
      vtype = filterer.sq.value_type
      if vtype is not None and vtype != "binary":
        raise exceptions.InvalidValueTypeError(
          f"Filterer must be of value type 'binary', not '{vtype}'"
        )
    # Update filterer.
    # Xarray treats null values as True but they should not pass the filter.
    filterer.values = utils.null_as_zero(filterer)
    # Apply filter.
    out = self._obj.where(filterer.sq.align_with(self._obj))
    return out

  def assign(self, y, at = None, track_types = True, **kwargs):
    """Apply the assign verb to the array.

    The assign verb assigns new values to the pixels in an array, without any
    computation. Unless specifically stated through the ``at`` argument, it
    does not assign new values to missing pixels (i.e. those pixels having a
    missing value).

    Parameters
    ----------
    y :
      Value(s) to be assigned. May be a constant, meaning that the same value
      is assigned to every pixel. May also be another array which can be
      aligned to the same shape as the input array. In the latter case, the
      value assigned to a pixel in the input array is the value of the pixel in
      array ``y`` that has the same dimension coordinates.
    at : :obj:`xarray.DataArray`, optional
      Binary array which can be aligned to the same shape as the input array.
      To be used for conditional assignment, in which a pixel in the input will
      only be assigned a new value if the if the pixel in ``at`` with the same
      dimension coordinates is true.
    track_types : :obj:`bool`
      Should the value type of the output object be promoted, and should it be
      checked that ``at`` has value type *binary*?
    **kwargs:
      Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    -------
      :obj:`exceptions.InvalidValueTypeError`
        If ``track_types = True`` and the value type of ``at`` is not *binary*.

    """
    if at is None:
      out = operators.assign_(self._obj, y, track_types = track_types)
    else:
      if track_types:
        vtype = at.sq.value_type
        if vtype is not None and vtype != "binary":
          raise exceptions.InvalidValueTypeError(
            f"Array 'at' must be of value type 'binary', not '{vtype}'"
          )
      out = operators.assign_at_(self._obj, y, at, track_types = track_types)
    return out

  def groupby(self, grouper, labels_as_names = True, **kwargs):
    """Apply the groupby verb to the array.

    The groupby verb groups the values in an array.

    Parameters
    -----------
      grouper : :obj:`xarray.DataArray` or :obj:`Collection`
        Array which can be aligned to the same shape as the input array. Pixels
        in the input array that have equal values in the grouper will be
        grouped together. Alternatively, it may be a collection of such arrays.
        Then, pixels in the input array that have equal values in all of the
        grouper arrays will be grouped together.
      labels_as_names : :obj:`bool`
        If value labels are defined, should they be used as group names instead
        of the values themselves?
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`Collection`

    Raises
    -------
      :obj:`exceptions.MissingDimensionError`
        If the grouper is zero-dimensional.
      :obj:`exceptions.UnknownDimensionError`
        If the grouper contains dimensions that are not present in the input.
      :obj:`exceptions.MixedDimensionsError`
        If the grouper is a collection and its elements don't all have the same
        dimensions.

    """
    # Get dimensions of the input.
    obj = self._obj
    odims = obj.dims
    # Get dimensions of the grouper(s).
    if isinstance(grouper, list):
      is_list = True
      gdims = [x.dims for x in grouper]
      if not all([x == gdims[0] for x in gdims]):
        raise exceptions.MixedDimensionsError(
          "Dimensions of grouper arrays do not match"
        )
    else:
      is_list = False
      gdims = [grouper.dims]
      grouper = [grouper]
    # Parse grouper.
    # When grouper is multi-dimensional, dimensions should be stacked.
    if len(gdims[0]) == 0:
      raise exceptions.MissingDimensionError(
        "Cannot group with a zero-dimensional grouper"
      )
    elif len(gdims[0]) == 1:
      is_spatial = False
      is_multidim = False
      if not gdims[0][0] in odims:
        raise exceptions.UnknownDimensionError(
          f"Grouper dimension '{gdims[0][0]}' is not present in the array"
        )
    elif len(gdims[0]) == 2 and X in gdims[0] and Y in gdims[0]:
      is_spatial = True
      is_multidim = False
      grouper = [x.sq.stack_spatial_dims() for x in grouper]
      try:
        obj = obj.sq.stack_spatial_dims()
      except KeyError:
        raise exceptions.UnknownDimensionError(
          f"Spatial dimensions '{X}' and '{Y}' are not present in the array"
        )
    else:
      is_spatial = False
      is_multidim = True
      if not all(x in odims for x in gdims[0]):
        raise exceptions.UnknownDimensionError(
            "Not all grouper dimensions are present in the array"
          )
      grouper = [x.sq.align_with(obj).sq.stack_all_dims() for x in grouper]
      obj = obj.sq.stack_all_dims()
    # Split input into groups based on unique grouper values.
    if is_list:
      idx = pd.MultiIndex.from_arrays([x.data for x in grouper])
      dim = grouper[0].dims
      partition = list(obj.groupby(xr.IndexVariable(dim, idx), squeeze=False))
      # Use value labels as group names if defined.
      if labels_as_names:
        labs = [x.sq.value_labels for x in grouper]
        names = [i[0] for i in partition]
        for i, x in enumerate(labs):
          if x is None:
            pass
          else:
            for j, y in enumerate(names):
              y = list(y)
              y[i] = x[y[i]]
              names[j] = tuple(y)
        groups = [i[1].rename(j) for i, j in zip(partition, names)]
      else:
        groups = [i[1].rename(i[0]) for i in partition]
    else:
      partition = list(obj.groupby(grouper[0], squeeze=False))
      # Use value labels as group names if defined.
      if labels_as_names:
        labs = grouper[0].sq.value_labels
        if labs is not None:
          groups = [i[1].rename(labs[i[0]]) for i in partition]
        else:
          groups = [i[1].rename(i[0]) for i in partition]
      else:
        groups = [i[1].rename(i[0]) for i in partition]
    # Post-process.
    # Stacked arrays must be unstacked again.
    if is_spatial:
      groups = [x.sq.unstack_spatial_dims() for x in groups]
    elif is_multidim:
      # Multi-dimensional grouping may create irregular spatial dimensions.
      # Therefore besides unstacking we also need to regularize the arrays.
      groups = [x.sq.unstack_all_dims().sq.regularize() for x in groups]
      # Stacking messes up the spatial feature indices coordinate.
      # We need to re-create this coordinate for each group array.
      if "spatial_feats" in self._obj.coords:
        def fix(x, y):
          x["spatial_feats"] = y["spatial_feats"].reindex_like(x)
          return x
        groups = [fix(x, self._obj) for x in groups]
    # Collect and return.
    out = Collection(groups)
    return out

  def reduce(
      self,
      reducer,
      dimension = None,
      track_types = True,
      keep_attrs = True,
      **kwargs
    ):
    """Apply the reduce verb to the array.

    The reduce verb reduces the dimensionality of an array.

    Parameters
    -----------
      reducer : :obj:`callable`
        The reducer function to be applied.
      dimension : :obj:`str`
        Name of the dimension to apply the reduction function to. If
        :obj:`None`, all dimensions are reduced.
      track_types : :obj:`bool`
        Should the reducer promote the value type of the output object, based
        on the value type of the input object?
      keep_attrs: :obj:`bool`
        Should the variable's attributes (attrs) be copied from the
        original object to the new one?
      **kwargs:
        Additional keyword arguments passed on to the reducer function. These
        should not include a keyword argument "dim", which is reserved for
        specifying the dimension to reduce over.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    ------
      :obj:`exceptions.UnknownDimensionError`
        If a dimension with the given name is not present in the array.

    """
    # Get array and set reduction dimension.
    obj = self._obj
    if dimension is not None:
      if dimension == SPACE:
        if X not in obj.dims or Y not in obj.dims:
          raise exceptions.UnknownDimensionError(
            f"Spatial dimensions '{X}' and '{Y}' are not present in the array"
          )
        obj = self.stack_spatial_dims()
      else:
        if dimension not in obj.dims:
          raise exceptions.UnknownDimensionError(
            f"Dimension '{dimension}' is not present in the array"
          )
      kwargs["dim"] = dimension
    # Reduce.
    out = reducer(obj, track_types = track_types, keep_attrs = keep_attrs, **kwargs)
    return out

  def shift(self, dimension, steps, **kwargs):
    """Apply the shift verb to the array.

    The shift verb shifts the values in an array a given amount of steps along
    a dimension.

    Parameters
    -----------
      dimension : :obj:`str`
        Name of the dimension to shift along.
      steps : :obj:`int`
        Amount of steps each value should be shifted. A negative integer will
        result in a shift to the left, while a positive integer will result in
        a shift to the right. A shift along the spatial dimension follows the
        pixel order defined by the CRS, e.g. starting in the top-left and
        moving down each column.
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    ------
      :obj:`exceptions.UnknownDimensionError`
        If a dimension with the given name is not present in the array.

    """
    # Get array.
    obj = self._obj
    if dimension == SPACE:
      if X not in obj.dims or Y not in obj.dims:
        raise exceptions.UnknownDimensionError(
          f"Spatial dimensions '{X}' and '{Y}' are not present in the array"
        )
      obj = self.stack_spatial_dims()
      stacked = True
    else:
      if dimension not in obj.dims:
        raise exceptions.UnknownDimensionError(
          f"Dimension '{dimension}' is not present in the array"
        )
      stacked = False
    # Shift values.
    out = self._obj.shift({dimension: steps})
    # Post-process.
    if stacked:
      out = out.sq.unstack_spatial_dims()
    return out

  def smooth(self, reducer, dimension, size, limit = 2, fill = False,
             track_types = True, **kwargs):
    """Apply the smooth verb to the array.

    The smooth verb smoothes the values in an array by applying a reducer
    function to a rolling window along a dimension.

    Parameters
    -----------
      reducer : :obj:`callable`
        The reducer function to be applied to the rolling window.
      dimension : :obj:`str`
        Name of the dimension to smooth along.
      size : :obj:`int`
        Size k defining the extent of the rolling window. The pixel being
        smoothed will always be in the center of the window, with k pixels at
        its left and k pixels at its right. If the dimension to smooth over is
        the spatial dimension, the size will be used for both the X and Y
        dimension, forming a square window with the smoothed pixel in the
        middle.
      limit : :obj:`int`
        Minimum number of valid data values inside a window. If the window
        contains less than this number of data values (excluding nodata) the
        smoothed value will be nodata.
      fill : :obj:`bool`
        Should pixels with a nodata value also be smoothed?
      track_types : :obj:`bool`
        Should the reducer promote the value type of the output object, based
        on the value type of the input object?
      **kwargs:
        Additional keyword arguments passed on to the reducer function. These
        should not include a keyword argument "dim", which is reserved for
        specifying the dimension to reduce over.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    ------
      :obj:`exceptions.UnknownDimensionError`
        If a dimension with the given name is not present in the array.

    """
    # Get array.
    obj = self._obj
    # Check dimension presence.
    if dimension == SPACE:
      if X not in obj.dims or Y not in obj.dims:
        raise exceptions.UnknownDimensionError(
          f"Spatial dimensions '{X}' and '{Y}' are not present in the array"
        )
    else:
      if dimension not in obj.dims:
        raise exceptions.UnknownDimensionError(
          f"Dimension '{dimension}' is not present in the array"
        )
    # Parse size.
    # Size parameter defines neighborhood size at each side of the pixel.
    size = size * 2 + 1
    # Create the rolling window object.
    if dimension == SPACE:
      obj = obj.rolling({X: size, Y: size}, center = True, min_periods = limit)
    else:
      obj = obj.rolling({dimension: size}, center = True, min_periods = limit)
    # Apply the reducer to each window.
    out = reducer(obj, track_types = track_types, **kwargs)
    # Post-process.
    if not fill:
      out = out.where(pd.notnull(self._obj)) # Preserve nan.
    return out

  def trim(self, dimension = None, **kwargs):
    """Apply the trim verb to the array.

    The trim verb trims the dimensions of an array, meaning that all dimension
    coordinates for which all values are missing are removed from the array.
    The spatial dimensions are only trimmed at their edges, to preserve their
    regularity.

    Parameters
    ----------
      dimension : :obj:`str`
        Name of the dimension to be trimmed. If :obj:`None`, all dimensions
        will be trimmed.

    Returns
    -------
      :obj:`xarray.DataArray`

    Raises
    ------
      :obj:`exceptions.UnknownDimensionError`
        If a dimension with the given name is not present in the array.

    """
    obj = self._obj
    dims = obj.dims
    if dimension is None:
      if X in dims and Y in dims:
        regular_dims = [d for d in dims if d not in [X, Y]]
        out = self._trim_space(self._trim(obj, regular_dims))
      else:
        out = self._trim(obj, dims)
    else:
      if dimension == SPACE:
        if X not in dims or Y not in dims:
          raise exceptions.UnknownDimensionError(
            f"Spatial dimensions '{X}' and '{Y}' are not present in the array"
          )
        out = self._trim_space(obj)
      else:
        if dimension not in obj.dims:
          raise exceptions.UnknownDimensionError(
            f"Dimension '{dimension}' is not present in the array"
          )
        out = self._trim(obj, [dimension])
    return out

  @staticmethod
  def _trim(obj, dimensions):
    for dim in dimensions:
      other_dims = [d for d in obj.dims if d != dim]
      out = obj.isel({dim: obj.count(other_dims) > 0})
    return out

  @staticmethod
  def _trim_space(obj):
    # Find the smallest and largest spatial coords containing valid values.
    y_idxs = np.nonzero(obj.count(list(set(obj.dims) - set([Y]))).data)[0]
    x_idxs = np.nonzero(obj.count(list(set(obj.dims) - set([X]))).data)[0]
    y_slice = slice(y_idxs.min(), y_idxs.max() + 1)
    x_slice = slice(x_idxs.min(), x_idxs.max() + 1)
    # Limit the x and y coordinates to only those ranges.
    out = obj.isel({Y: y_slice, X: x_slice})
    return out

  def delineate(self, track_types = True, **kwargs):
    """Apply the delineate verb to the array.

    The delineate verb deliniates spatio-temporal objects in a binary array.

    Parameters
    -----------
      track_types : :obj:`bool`
        Should the value type of the output object be promoted, and should it be
        checked that the input has value type *binary*?
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    -------
      :obj:`exceptions.InvalidValueTypeError`
        If ``track_types = True`` and the array is not binary.
      :obj:`exceptions.TooManyDimensionsError`
        If the array has more dimensions besides the spatial and/or temporal.
      :obj:`exceptions.MissingDimensionError`
        If the array has neither spatial nor temporal dimensions.

    """
    # Get and check array.
    obj = xr.apply_ufunc(utils.null_as_zero, self._obj)
    if track_types:
      vtype = obj.sq.value_type
      if vtype is not None and vtype != "binary":
        raise exceptions.InvalidValueTypeError(
          f"Array to be delineated must be of value type 'binary', not '{vtype}'"
        )
    # Inspect dimensions.
    dims = obj.dims
    is_spatial = X in dims and Y in dims
    is_temporal = TIME in dims
    # Define neighborhood matrix.
    if is_spatial and is_temporal:
      if len(dims) > 3:
        raise exceptions.TooManyDimensionsError(
          f"Delineate is only supported for arrays with dimension '{TIME}' "
          f"and/or '[{Y},{X}]', not: {list(dims)}"
        )
      obj = obj.transpose(TIME, Y, X) # Make sure dimension order is correct.
      nb = np.array([
        [[0,0,0],[0,1,0],[0,0,0]],
        [[1,1,1],[1,1,1],[1,1,1]],
        [[0,0,0],[0,1,0],[0,0,0]]
      ])
    elif is_spatial:
      if len(dims) > 2:
        raise exceptions.TooManyDimensionsError(
          f"Delineate is only supported for arrays with dimension '{TIME}' "
          f"and/or '[{Y},{X}]', not: {list(dims)}"
        )
      nb = np.array([
        [1,1,1],
        [1,1,1],
        [1,1,1]
      ])
    elif is_temporal:
      if len(dims) > 1:
        raise exceptions.TooManyDimensionsError(
          f"Delineate is only supported for arrays with dimension '{TIME}' "
          f"and/or '[{Y},{X}]', not: {list(dims)}"
        )
      nb = np.array([1,1,1])
    else:
      raise exceptions.MissingDimensionError(
        f"Delineate is only supported for arrays with dimension '{TIME}' "
        f"and/or '{SPACE}', not: {list(dims)}"
      )
    # Delineate.
    out = xr.apply_ufunc(lambda x, y: ndimage.label(x, y)[0], obj, nb)
    # Post-process.
    out = out.where(pd.notnull(self._obj)) # Preserve nan.
    if track_types:
      out.sq.value_type = "ordinal"
    return out

  def fill(self, dimension, method, extrapolate = True, track_types = True,
           **kwargs):
    """Apply the fill verb to the array.

    The fill verb fills nodata values by interpolating valid data values.

    Parameters
    -----------
      dimension : :obj:`str`
        Name of the dimension along which to interpolate.
      method : :obj:`str`
        Interpolation method to use. One of nearest, linear or cubic. When
        interpolation along the stacked space dimensions, the two-dimensional
        versions of these interpolation methods are used, i.e. 2D nearest
        neighbour, bilinear and bicubic.
      extrapolate : :obj:`bool`
        Should nodata values at the edge be extrapolated? Only applied to
        one-dimensional interpolation.
      track_types : :obj:`bool`
        Should the value type(s) of the input(s) be checked, and the value
        type of the output be promoted, whenever applicable?
      **kwargs"
        Additional keyword arguments passed on to the interpolation function.
        When interpolating along a single dimension, the interpolation function
        is :meth:`xarray.DataArray.interpolate_na`.
        When interpolation along the stacked space dimension, the interpolation
        funtion is :meth:`rioxarray.raster_array.RasterArray.interpolate_na`.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    ------
      :obj:`exceptions.UnknownDimensionError`
        If a dimension with the given name is not present in the array.
      :obj:`exceptions.InvalidValueTypeError`
        If ``track_types = True`` and the value type of the array is not
        supported by the chosen interpolation method.

    """
    obj = self._obj
    # Check if arrays value type is supported by the interpolation method.
    if track_types:
      if method != "nearest":
        vtype = obj.sq.value_type
        if vtype is not None and vtype not in ["continuous", "discrete"]:
          raise exceptions.InvalidValueTypeError(
            f"Unsupported value type for interpolation method '{method}': '{vtype}'"
          )
    # Interpolate missing values.
    # --> 2D interpolation for the stacked space dimension.
    # --> 1D interpolation for any regular dimension.
    if dimension == SPACE:
      if X not in obj.dims or Y not in obj.dims:
        raise exceptions.UnknownDimensionError(
          f"Spatial dimensions '{X}' and '{Y}' are not present in the array"
        )
      if obj.rio.nodata is None:
        nodata = utils.get_null(obj)
        out = obj.rio.write_nodata(nodata).rio.interpolate_na(method, **kwargs)
      else:
        out = obj.rio.interpolate_na(method, **kwargs)
    else:
      if dimension not in obj.dims:
        raise exceptions.UnknownDimensionError(
          f"Dimension '{dimension}' is not present in the array"
        )
      if extrapolate:
        kwargs.update({"fill_value": "extrapolate"})
      out = obj.interpolate_na(dimension, method = method, **kwargs)
    return out

  def name(self, value, **kwargs):
    """Apply the name verb to the array.

    The name verb assigns a name to an array.

    Parameters
    -----------
      value : :obj:`str`
        Character sting to be assigned as name to the input array.
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    """
    out = self._obj.rename(value)
    return out

  def apply_custom(self, verb, track_types = True, **kwargs):
    """Apply a user-defined verb to the array.

    Parameters
    -----------
      verb : :obj:`callable`
        Implementation of the custom verb which will be provided to
        :meth:`xarray.DataArray.pipe`.
      track_types : :obj:`bool`
        Should the value type(s) of the input(s) be checked, and the value
        type of the output be promoted, whenever applicable?
      **kwargs:
        Additional keyword arguments passed on to the verb function.

    Returns
    --------
      :obj:`xarray.DataArray`

    """
    out = self._obj.pipe(verb, track_types = track_types, **kwargs)
    return out

  #
  # INTERNAL PROCESSING
  #

  def align_with(self, other):
    """Align the array to the shape of another array.

    An input array is alinged to another array if the pixel at position *i* in
    the input array has the same coordinates as the pixel at position *i* in the
    other array. Aligning can be done in several ways:

    * Consider the case where the input array has exactly the same dimensions
      and coordinates as the other array, but the order of them is different.
      In that case, the input array is simply re-ordered to match the other
      array.

    * Consider the case where the input array has the same dimensions as the
      other array, but not all coordinates match. In that case, the coordinates
      that are in the input array but not in the other array are removed from the
      input array, and at the same time the coordinates that are in the other
      array but not in the input array are added to the input array, with nodata
      values assigned.

    * Consider the case where all dimensions of the input array are also present
      in the other array, but not all dimensions of the other array are present
      in the input array. In that case, the pixels of the input array are
      duplicated along those dimensions that are missing.

    Alignment may also be a combination of more than one of these ways.

    Parameters
    -----------
      other : :obj:`xarray.DataArray`
        Array to which the input array should be aligned.

    Returns
    --------
      :obj:`xarray.DataArray`
        The aligned input array.

    Raises
    -------
      :obj:`exceptions.AlignmentError`
        If the input array cannot be aligned to the other array, for example when
        the two arrays have no dimensions in common at all, or when the input
        array has dimensions that are not present in the other array.

    """
    out = xr.align(other, self._obj, join = "left")[1].broadcast_like(other)
    if not out.shape == other.shape:
      raise exceptions.AlignmentError(
        f"Array '{other.name if other.name is not None else 'y'}' "
        f"cannot be aligned with "
        f"input array '{self._obj.name if self._obj.name is not None else 'x'}'"
      )
    return out

  def regularize(self):
    """Regularize the spatial dimension of the array.

    Regularizing makes sure that the steps between subsequent coordinates of
    the spatial dimensions are always equal to the resolution of that
    dimensions.

    Returns
    -------
      :obj:`xarray.DataArray`
        The regularized input array.

    """
    # Extract spatial coordinates.
    try:
      xcoords = self._obj[X]
      ycoords = self._obj[Y]
    except KeyError:
      return self._obj
    # Update spatial coordinates.
    res = self.spatial_resolution
    if res is None:
      res = self.write_spatial_resolution(inplace = True).sq.spatial_resolution
    xcoords = np.arange(xcoords[0], xcoords[-1] + res[1], res[1])
    ycoords = np.arange(ycoords[0], ycoords[-1] + res[0], res[0])
    # Reindex array.
    out = self._obj.reindex({Y: ycoords, X: xcoords})
    return out

  def reproject(self, crs, **kwargs):
    """Reproject the spatial coordinates of the array into a different CRS.

    Parameters
    ----------
      crs:
        Target coordinate reference system. Can be given as any object
        understood by the initializer of :class:`pyproj.crs.CRS`. This includes
        :obj:`pyproj.crs.CRS` objects themselves, as well as EPSG codes and WKT
        strings.
      **kwargs:
        Additional keyword arguments passed on to
        :meth:`rioxarray.rioxarray.XRasterBase.reproject`.

    Returns
    --------
      :obj:`xarray.DataArray`
        The input array with reprojected spatial coordinates.

    """
    if X not in self._obj.dims or Y not in self._obj.dims:
      return self._obj
    # To make rioxarray work:
    # Array can have only the spatial_ref non-dimension coordinate.
    obj = self.drop_non_dimension_coords(keep = ["spatial_ref"])
    # Reproject.
    out = obj.rio.reproject(crs, **kwargs)
    # Recover other non-dimension coordinates.
    out = out.sq.write_tz(self.tz)
    return out

  def tz_convert(self, tz, **kwargs):
    """Convert the temporal coordinates of the array into a different timezone.

    Parameters
    ----------
      tz:
        Target timezone. Can be given as :obj:`str` referring to the name of a
        timezone in the tz database, or as instance of any class inheriting
        from :class:`datetime.tzinfo`.
      **kwargs:
        Additional keyword arguments passed on to
        :func:`convert_datetime64 <semantique.processor.utils.convert_datetime64>`.

    Returns
    -------
      :obj:`xarray.DataArray`
        The input array with converted temporal coordinates.

    """
    if TIME not in self._obj.dims:
      return self._obj
    src = self._obj[TIME].data
    trg = [utils.convert_datetime64(x, self.tz, tz, **kwargs) for x in src]
    out = self._obj.assign_coords({TIME: trg}).sq.write_tz(tz)
    return out

  def write_crs(self, crs, inplace = False):
    """Store the CRS of the array as non-dimension coordinate.

    The coordinate reference system of the spatial coordinates is stored as
    attribute of a specific non-dimension coordinate named "spatial_ref".
    Storing this inside a non-dimension coordinate rather than as direct
    attribute of the array guarantees that this information is preserved
    during any kind of operation. The coordinate itself serves merely as a
    placeholder.

    Parameters
    ----------
      crs:
        The spatial coordinate reference system to store. Can be given as any
        object understood by the initializer of :class:`pyproj.crs.CRS`. This
        includes :obj:`pyproj.crs.CRS` objects themselves, as well as EPSG
        codes and WKT strings.
      inplace : :obj:`bool`
        Should the array be modified inplace?

    Returns
    -------
      :obj:`xarray.DataArray`
        The input array with the CRS stored in a non-dimension coordinate.

    """
    return self._obj.rio.write_crs(crs, inplace = inplace)

  def write_tz(self, tz, inplace = False):
    """Store the timezone of the array as non-dimension coordinate.

    The timezone of the temporal coordinates is stored as attribute of a
    specific non-dimension coordinate named "temporal_ref". Storing this inside
    a non-dimension coordinate rather than as direct attribute of the array
    guarantees that this information is preserved during any kind of operation.
    The coordinate itself serves merely as a placeholder.

    Parameters
    ----------
      tz:
        The timezone to store. Can be given as :obj:`str` referring to the name
        of a timezone in the tz database, or as instance of any class
        inheriting from :class:`datetime.tzinfo`.
      inplace : :obj:`bool`
        Should the array be modified inplace?

    Returns
    -------
      :obj:`xarray.DataArray`
        The input array with the timezone stored in a non-dimension coordinate.

    """
    obj = self._obj if inplace else copy.deepcopy(self._obj)
    try:
      zone = tz.tzname(None)
    except AttributeError:
      zone = pytz.timezone(tz).tzname(None)
    obj["temporal_ref"] = 0
    obj["temporal_ref"].attrs["zone"] = zone
    return obj

  def write_spatial_resolution(self, resolution = None, recompute = False,
                               inplace = False):
    """Store the spatial resolution of the array as dimension attributes.

    The resolution of the spatial coordinates is stored as attribute of the
    respective spatial dimensions.

    Parameters
    ----------
      resolution:
        The spatial resolution to store. Should be given as a list in the format
        *[y, x]*, where y is the cell size along the y-axis, x is the cell size
        along the x-axis, and both are given as :obj:`int` or :obj:`float`
        value expressed in the units of the CRS. These values should include
        the direction of the axes. For most CRSs, the y-axis has a negative
        direction, and hence the cell size along the y-axis is given as a
        negative number. If :obj:`None`, the spatial resolution will be
        automatically inferred from the array.
      recompute : :obj:`bool`
        If the resolution is automatically inferred, should it be recomputed
        based on the coordinate values, or read from the transform attribute if
        present?
      inplace : :obj:`bool`
        Should the array be modified inplace?

    Returns
    -------
      :obj:`xarray.DataArray`
        The input array with the spatial resolution stored as dimension
        attributes.

    """
    obj = self._obj if inplace else copy.deepcopy(self._obj)
    if resolution is None:
      resolution = obj.rio.resolution(recalc = recompute)[::-1]
    try:
      obj[Y].attrs["resolution"] = resolution[0]
      obj[X].attrs["resolution"] = resolution[1]
    except KeyError:
      raise exceptions.MissingDimensionError(
        "Cannot write spatial resolution to an array without spatial dimensions"
      )
    return obj

  def stack_spatial_dims(self):
    """Stack spatial X and Y dimensions into a single spatial dimension.

    Returns
    --------
      :obj:`xarray.DataArray`
        The input array with a multi-index spatial dimension.

    """
    out = self._obj.stack({SPACE: [Y, X]})
    out[SPACE].sq.value_type = "coords"
    return out

  def unstack_spatial_dims(self):
    """Unstack the spatial dimension into separate X and Y dimensions.

    Returns
    --------
      :obj:`xarray.DataArray`
        The input array with unstacked spatial dimensions.

    """
    out = self._obj.unstack(SPACE)
    out[Y].sq.value_type = "continuous"
    out[X].sq.value_type = "continuous"
    return out

  def stack_all_dims(self):
    """Stack all dimensions into a single multi-indexed dimension.

    Returns
    --------
      :obj:`xarray.DataArray`
        The input array with a single multi-index dimension named "__all__".

    """
    dimnames = self._obj.dims
    dimtypes = [self._obj[x].sq.value_type for x in dimnames]
    out = self._obj.stack(__all__ = dimnames)
    out.attrs["dim_value_types"] = {k:v for k,v in zip(dimnames, dimtypes)}
    return out

  def unstack_all_dims(self):
    """Unstack the single multi-indexed dimension back into separate dimensions.

    Returns
    --------
      :obj:`xarray.DataArray`
        The input array with unstacked dimensions.

    """
    out = self._obj.unstack()
    if "dim_value_types" in self._obj.attrs:
      for k, v in self._obj.attrs["dim_value_types"].items():
        out[k].sq.value_type = v
      del out.attrs["dim_value_types"]
    return out

  def rename_dims(self, mapping):
    """Rename one or more dimensions in the array.

    Parameters
    -----------
      mapping : :obj:`dict`
        Mapping from current to new dimension names.

    Returns
    --------
      :obj:`xarray.DataArray`
        The input array without renamed dimensions

    """
    new_indices = {v:k for k, v in mapping.items()}
    return self._obj.swap_dims(mapping).set_index(new_indices)

  def drop_non_dimension_coords(self, keep = None):
    """Drop non-dimension coordinates from the array.

    Non-dimension coordinates are coordinates that are used for e.g. auxiliary
    labeling or metadata storage. See the `xarray documentation`_.

    Parameters
    -----------
      keep : :obj:`list`, optional
        List of non-dimension coordinate names that should not be dropped.

    Returns
    --------
      :obj:`xarray.DataArray`
        The input array without non-dimension coordinates

    .. _xarray documentation:
      https://xarray.pydata.org/en/stable/user-guide/terminology.html#term-Non-dimension-coordinate

    """
    if keep is None:
      drop = set(self._obj.coords) - set(self._obj.dims)
    else:
      drop = set(self._obj.coords) - set(self._obj.dims) - set(keep)
    return self._obj.reset_coords(drop, drop = True)

  #
  # CONVERTERS
  #

  def to_dataframe(self):
    """Convert the array to a pandas DataFrame.

    The data frame will contain one column per dimension, and a column
    containing the data values.

    Returns
    -------
      :obj:`pandas.DataFrame`
        The converted input array

    """
    obj = self.drop_non_dimension_coords()
    # to_dataframe method does not work for zero-dimensional arrays.
    if len(self._obj.dims) == 0:
      out = pd.DataFrame([obj.values])
    else:
      out = obj.to_dataframe()
    return out

  def to_geodataframe(self, output_crs = None):
    """Convert the array to a geopandas GeoDataFrame.

    The data frame will contain one column per dimension, a column
    containing the data values, and a geometry column containing coordinates of
    geospatial points that represent the centroids of the pixels in the array.

    Parameters
    ----------
      output_crs : optional
        Spatial coordinate reference system of the GeoDataFrame. Can be
        given as any object understood by the initializer of
        :class:`pyproj.crs.CRS`. This includes :obj:`pyproj.crs.CRS` objects
        themselves, as well as EPSG codes and WKT strings. If :obj:`None`, the
        CRS of the array itself is used.

    Returns
    -------
      :obj:`geopandas.GeoDataFrame`
        The converted input array

    """
    # Convert to dataframe.
    df = self.to_dataframe().reset_index()
    # Create geometries.
    geoms = gpd.points_from_xy(df[X], df[Y])
    # Convert to geodataframe
    gdf = gpd.GeoDataFrame(df, geometry = geoms, crs = self.crs)
    # Reproject if needed.
    if output_crs is not None:
      gdf = gdf.to_crs(output_crs)
    return gdf

  def to_csv(self, file):
    """Write the content of the array to a CSV file on disk.

    The CSV file will contain one column per dimension, and a column containing
    the data values.

    Parameters
    ----------
      file : :obj:`str`
        Path to the CSV file to be written.

    Returns
    --------
      :obj:`str`
        Path to the written CSV file.

    """
    df = self.to_dataframe()
    if len(self._obj.dims) == 0:
      df.to_csv(file, header = False, index = False)
    else:
      df.to_csv(file)
    return file

  def to_geotiff(self, file, cloud_optimized = True, compress = True,
                 output_crs = None):
    """Write the content of the array to a GeoTIFF file on disk.

    Parameters
    ----------
      file : :obj:`str`
        Path to the GeoTIFF file to be written.
      cloud_optimized : :obj:`bool`
        Should the written file be a Cloud Optimized GeoTIFF (COG) instead of a
        regular GeoTIFF? Ignored when a GDAL version < 3.1 is used. These GDAL
        versions do not have a COG driver, and the written GeoTIFF will always
        be a regular GeoTIFF.
      compress : :obj:`bool`
        Should the written file be compressed? If :obj:`True`, LZW compression
        is used.
      output_crs : optional
        Spatial coordinate reference system of the written GeoTIFF. Can be
        given as any object understood by the initializer of
        :class:`pyproj.crs.CRS`. This includes :obj:`pyproj.crs.CRS` objects
        themselves, as well as EPSG codes and WKT strings. If :obj:`None`, the
        CRS of the array itself is used.

    Returns
    -------
      :obj:`str`
        Path to the written GeoTIFF file.

    Raises
    ------
      :obj:`exceptions.InvalidValueTypeError`
        If the value type of the array is not supported by the GeoTIFF exporter.
      :obj:`exceptions.TooManyDimensionsError`
        If the array has more than three dimensions, including the two unstacked
        spatial dimensions. More than three dimensions is currently not
        supported by the export functionality of rasterio.

    """
    # Get array to export.
    obj = self._obj
    # Initialize GDAL configuration parameters.
    config = {}
    # Remove non-dimension coordinates but not 'spatial_ref'.
    # That one is needed by rioxarray to determine the CRS of the data.
    obj = obj.sq.drop_non_dimension_coords(keep = ["spatial_ref"])
    # Reproject data if requested.
    if output_crs is not None:
      obj = obj.rio.reproject(output_crs)
    # Many GeoTIFF visualizers cannot handle Inf values.
    # Therefore we convert all Inf values to NaN values.
    obj.values = utils.inf_as_null(obj)
    # The GeoTIFF exporter cannot handle datetime values.
    # Therefore we convert all datetime values to numeric values (unix time).
    obj.values = utils.datetime64_as_unix(obj)
    # GDAL has limited support for numpy dtypes.
    # Therefore dtype conversion might be needed in some cases.
    dtype = obj.dtype
    if not rasterio.dtypes.check_dtype(dtype):
      try:
        dtype = rasterio.dtypes.get_minimum_dtype(obj)
      except TypeError:
        vtype = obj.sq.value_type
        raise exceptions.InvalidValueTypeError(
          f"GeoTIFF exporter has no support for value type '{vtype}'"
        )
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
    # If the array has an additional dimension next to the spatial ones:
    # --> Convert array to dataset with the third dimension as variable.
    # --> This will make rioxarray export dimension coords as band names.
    ndims = len(obj.dims)
    if ndims == 3:
      non_spatial_dim = [x for x in obj.dims if x not in [X, Y]][0]
      newobj = xr.Dataset()
      for x in obj[non_spatial_dim].values:
        newobj[str(x)] = obj.sel({non_spatial_dim: x}, drop = True)
      obj = newobj
    # Write data to file.
    try:
      obj.rio.to_raster(file, **config)
    except rioxarray.exceptions.TooManyDimensions:
      raise exceptions.TooManyDimensionsError(
        f"GeoTIFF export is only supported for 2D or 3D arrays, not {ndims}D"
      )
    return file

@xr.register_dataarray_accessor("sqm")
class MetaArray(Array):
  """Internal representation of a multi-dimensional meta array.
  To be used in conjunction with the `semantique.processor.FilterProcessor`.

  Note: Track-type attributes are kept to have a compatible structure with the
  `semantique.processor.Array` but are effectively ignored.

  This data structure is modelled as an accessor of :class:`xarray.DataArray`.
  Using accessors instead of the common class inheritance is recommended by the
  developers of xarray, see `here`_. In practice, this means that each method
  of this class can be called as method of :obj:`xarray.DataArray` objects by
  using the ``.sqm`` prefix: ::

    xarray_obj.sqm.method

  Parameters
  ----------
    xarray_obj : :obj:`xarray.DataArray`
      The content of the array.

  .. _here:
    https://xarray.pydata.org/en/stable/internals/extending-xarray.html

  """

  def __init__(self, xarray_obj):
    super(MetaArray, self).__init__(xarray_obj)

  @property
  def active(self):
    """:obj:`str`: Active evaluation object."""
    try:
      return self._obj.attrs["active"]
    except KeyError:
      return None

  @active.setter
  def active(self, value):
    self._obj.attrs["active"] = value

  @active.deleter
  def active(self):
    """:obj:`bool`: Is the current object actively monitored?
    This guides how the object is evaluated in some operations
    Active objects should be those that contain data from the
    current `watch_layer` (processor.core.FilterProcessor).
    """
    try:
      del self._obj.attrs["active"]
    except KeyError:
      pass

  @property
  def locked(self):
    """:obj:`bool`: Is the evaluation of filter operations restricted?
    If locked, filter operations will be ignored."""
    try:
      return self._obj.attrs["locked"]
    except KeyError:
      return None

  @locked.setter
  def locked(self, value):
    self._obj.attrs["locked"] = value

  @locked.deleter
  def locked(self):
    try:
      del self._obj.attrs["locked"]
    except KeyError:
      pass

  @property
  def vault(self):
    """:obj:`list`: Tresor which can be used to store timestamps
    if the time dimension in the array itself has been reduced away."""
    try:
      return self._obj.attrs["vault"]
    except KeyError:
      return None

  @vault.setter
  def vault(self, value):
    self._obj.attrs["vault"] = value

  @vault.deleter
  def vault(self):
    try:
      del self._obj.attrs["vault"]
    except KeyError:
      pass

  @property
  def value_type(self):
    """:obj:`str`: The value type of the array."""
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

  #
  # VERBS
  #

  def evaluate(self, operator, y = None, track_types = False, **kwargs):
    """Apply the evaluate verb to the array.

    The evaluate verb evaluates an expression for each pixel in an array.

    Parameters
    ----------
    operator : :obj:`callable`
      Operator function to be used in the expression.
    y : optional
      Right-hand side of the expression. May be a constant, meaning that the
      same value is used in each expression. May also be another array
      which can be aligned to the same shape as the input array. In the latter
      case, when evaluating the expression for a pixel in the input array the
      second operand is the value of the pixel in array ``y`` that has the same
      dimension coordinates. Ignored when the operator is univariate.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type(s) of the operand(s)?
    **kwargs:
      Additional keyword arguments passed on to the operator function.

    Returns
    --------
      :obj:`xarray.DataArray`

    """
    # once issue 54 (https://github.com/ZGIS/semantique/issues/54) is solved,
    # the code can be simplified by omitting the twoway_ops differentiations

    operands = tuple([self._obj]) if y is None else tuple([self._obj, y])
    # Temporal operations should always be evaluated.
    if operands[0].dims == ('time',):
      out = operator(*operands, track_types = track_types, meta = True, **kwargs)
      out.sqm.active = self.active
      out.sqm.locked = False
      out.sqm.vault = self.vault
    # If y is another array, the need to evaluate depends on the operator and
    # the active status of the array.
    elif y is not None and hasattr(y, "sqm"):
      # Currently only some operators are operating in two ways.
      # Two-way operation mode -> Filtering (= passing of NaNs) regardless if y is active.
      twoway_ops = [
        "add_",
        "subtract_",
        "multiply_",
        "divide_",
        "power_",
        "normalized_difference_"
      ]
      if y.sqm.active:
        if self.active or operator.__name__ in twoway_ops:
          out = operator(*operands, track_types = track_types, meta = True, **kwargs)
          out.sqm.vault = self._merge_arrays_vaults(list(operands))
        else:
          out = y
          out.sqm.vault = y.sqm.vault
        out.sqm.active = True
        out.sqm.locked = True if any([self.locked, y.sqm.locked]) else False
      else:
        if operator.__name__ in twoway_ops:
          out = operator(*operands, track_types = track_types, meta = True, **kwargs)
          if self.active:
            out.sqm.active = True
          else:
            out.sqm.active = False
          out.sqm.locked = True if any([self.locked, y.sqm.locked]) else False
          out.sqm.vault = self._merge_arrays_vaults(list(operands))
        else:
          out = self._obj
      # Only NaN values should be passed through the filter.
      # Others are result of content-based ops and should be reset to 1.
      out.values = np.where(~np.isnan(out.values), 1, np.nan)
    else:
      # Missing ops invert the filter result and should be evaluated.
      if operator.__name__ in ["is_missing_", "not_missing_"]:
        out = operator(*operands, track_types = track_types, meta = True, **kwargs)
        out.sqm.active = self.active
        out.sqm.locked = self.locked
        out.sqm.vault = self.vault
      else:
        out = self._obj
    return out


  def extract(self, dimension, component = None, **kwargs):
    """Apply the extract verb to the array.

    The extract verb extracts coordinate labels of a dimension as a new
    array.

    Parameters
    -----------
      dimension : :obj:`str`
        Name of the dimension to be extracted.
      component : :obj:`str`, optional
        Name of a specific component of the dimension coordinates to be
        extracted, e.g. *year*, *month* or *day* for temporal dimension
        coordinates.
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    -------
      :obj:`exceptions.UnknownDimensionError`
        If a dimension with the given name is not present in the array.
      :obj:`exceptions.UnknownComponentError`
        If the given dimension does not contain the given component.

    """
    # Get array.
    obj = self._obj
    # Extract spatial or temporal dimension(s).
    if dimension == TIME:
      out = self._extract_time(obj, component)
      out.sqm.active = self.active
      out.sqm.locked = self.locked
      out.sqm.vault = self.vault
      return out
    if dimension == SPACE:
      out = self._extract_space(obj, component)
      out.sqm.active = self.active
      out.sqm.locked = self.locked
      out.sqm.vault = self.vault
      return out
    # Extract any other dimension.
    try:
      out = obj[dimension]
    except KeyError:
      raise exceptions.UnknownDimensionError(
        f"Dimension '{dimension}' is not present in the array"
      )
    if component is not None:
      try:
        out = out[component]
      except KeyError:
        raise exceptions.UnknownComponentError(
          f"Component '{component}' is not defined for dimension '{dimension}'"
        )
    out.sqm.active = self.active
    out.sqm.locked = self.locked
    out.sqm.vault = self.vault
    return out

  @staticmethod
  def _extract_space(obj, component = None):
    if component is None:
      try:
        out = obj.sqm.stack_spatial_dims()[SPACE]
      except KeyError:
        raise exceptions.UnknownDimensionError(
          f"Spatial dimensions '{X}' and '{Y}' are not present in the array"
        )
      out._variable = out._variable.to_base_variable()
      out = out.sqm.unstack_spatial_dims()
      out.sqm.value_type = "coords"
    else:
      # Component FEATURE should extract spatial feature indices.
      if component == components.space.FEATURE:
        cname = "spatial_feats"
      else:
        cname = component
      try:
        out = obj[cname]
      except KeyError:
        raise exceptions.UnknownComponentError(
          f"Component '{cname}' is not defined for dimension '{SPACE}'"
        )
    return out

  @staticmethod
  def _extract_time(obj, component = None):
    try:
      out = obj[TIME]
    except KeyError:
      raise exceptions.UnknownDimensionError(
        f"Dimension '{TIME}' is not present in the array"
      )
    if component is not None:
      try:
        out = out[component]
      except KeyError:
        aliases = {
          "day_of_week": "dayofweek",
          "day_of_year": "dayofyear"
        }
        try:
          component = aliases[component]
        except KeyError:
          pass
        try:
          out = getattr(out.dt, component)
        except AttributeError:
          raise exceptions.UnknownComponentError(
            f"Component '{component}' is not defined for dimension '{TIME}'"
          )
        else:
          out = utils.parse_datetime_component(component, out)
    return out

  def filter(self, filterer, track_types = False, **kwargs):
    """Apply the filter verb to the array.

    The filter verb filters the values in an array.

    Parameters
    -----------
      filterer : :obj:`xarray.DataArray`
        Binary array which can be aligned to the same shape as the input array.
        Each pixel in the input array will be kept if the pixel in the filterer
        with the same dimension coordinates is true, and dropped otherwise
        (i.e. assigned a nodata value).
      track_types : :obj:`bool`
        Should it be checked that the filterer has value type *binary*?
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    """
    # Xarray treats null values as True but they should not pass the filter.
    filterer.values = utils.null_as_zero(filterer)
    # Apply filter only if not locked.
    if any([self.locked, filterer.sqm.locked]):
      out = self._obj
    else:
      out = self._obj.where(filterer.sqm.align_with(self._obj))
      out.sqm.active = any([self.active, filterer.sqm.active])
      out.sqm.locked = False
      out.sqm.vault = self._merge_arrays_vaults([self._obj, filterer])
    return out

  def assign(self, y, at = None, track_types = False, **kwargs):
    """Apply the assign verb to the array. This method is structured the same 
    way as it is structured for Array objects but effectively muted, i.e. the
    current object is simply handed back.

    Parameters
    ----------
    y :
      Value(s) to be assigned. May be a constant, meaning that the same value
      is assigned to every pixel. May also be another array which can be
      aligned to the same shape as the input array. In the latter case, the
      value assigned to a pixel in the input array is the value of the pixel in
      array ``y`` that has the same dimension coordinates.
    at : :obj:`xarray.DataArray`, optional
      Binary array which can be aligned to the same shape as the input array.
      To be used for conditional assignment, in which a pixel in the input will
      only be assigned a new value if the if the pixel in ``at`` with the same
      dimension coordinates is true.
    track_types : :obj:`bool`
      Should the value type of the output object be promoted, and should it be
      checked that ``at`` has value type *binary*?
    **kwargs:
      Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    """
    out = self._obj
    return out

  def groupby(self, grouper, labels_as_names = True, **kwargs):
    """Apply the groupby verb to the array.

    The groupby verb groups the values in an array.

    Parameters
    -----------
      grouper : :obj:`xarray.DataArray` or :obj:`Collection`
        Array which can be aligned to the same shape as the input array. Pixels
        in the input array that have equal values in the grouper will be
        grouped together. Alternatively, it may be a collection of such arrays.
        Then, pixels in the input array that have equal values in all of the
        grouper arrays will be grouped together.
      labels_as_names : :obj:`bool`
        If value labels are defined, should they be used as group names instead
        of the values themselves?
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`Collection`

    Raises
    -------
      :obj:`exceptions.MissingDimensionError`
        If the grouper is zero-dimensional.
      :obj:`exceptions.UnknownDimensionError`
        If the grouper contains dimensions that are not present in the input.
      :obj:`exceptions.MixedDimensionsError`
        If the grouper is a collection and its elements don't all have the same
        dimensions.

    """
    # Get dimensions of the input.
    obj = self._obj
    odims = obj.dims
    # Get dimensions of the grouper(s).
    if isinstance(grouper, list):
      is_list = True
      gdims = [x.dims for x in grouper]
      if not all([x == gdims[0] for x in gdims]):
        raise exceptions.MixedDimensionsError(
          "Dimensions of grouper arrays do not match"
        )
    else:
      is_list = False
      gdims = [grouper.dims]
      grouper = [grouper]
    # Parse grouper.
    # When grouper is multi-dimensional, dimensions should be stacked.
    if len(gdims[0]) == 0:
      raise exceptions.MissingDimensionError(
        "Cannot group with a zero-dimensional grouper"
      )
    elif len(gdims[0]) == 1:
      is_spatial = False
      is_multidim = False
      if not gdims[0][0] in odims:
        raise exceptions.UnknownDimensionError(
          f"Grouper dimension '{gdims[0][0]}' is not present in the array"
        )
    elif len(gdims[0]) == 2 and X in gdims[0] and Y in gdims[0]:
      is_spatial = True
      is_multidim = False
      grouper = [x.sqm.stack_spatial_dims() for x in grouper]
      try:
        obj = obj.sqm.stack_spatial_dims()
      except KeyError:
        raise exceptions.UnknownDimensionError(
          f"Spatial dimensions '{X}' and '{Y}' are not present in the array"
        )
    else:
      is_spatial = False
      is_multidim = True
      if not all(x in odims for x in gdims[0]):
        raise exceptions.UnknownDimensionError(
            "Not all grouper dimensions are present in the array"
          )
      grouper = [x.sqm.align_with(obj).sqm.stack_all_dims() for x in grouper]
      obj = obj.sqm.stack_all_dims()
    # Split input into groups based on unique grouper values.
    if is_list:
      idx = pd.MultiIndex.from_arrays([x.data for x in grouper])
      dim = grouper[0].dims
      partition = list(obj.groupby(xr.IndexVariable(dim, idx), squeeze=False))
      # Use value labels as group names if defined.
      if labels_as_names:
        labs = [x.sqm.value_labels for x in grouper]
        names = [i[0] for i in partition]
        for i, x in enumerate(labs):
          if x is None:
            pass
          else:
            for j, y in enumerate(names):
              y = list(y)
              y[i] = x[y[i]]
              names[j] = tuple(y)
        groups = [i[1].rename(j) for i, j in zip(partition, names)]
      else:
        groups = [i[1].rename(i[0]) for i in partition]
    else:
      partition = list(obj.groupby(grouper[0], squeeze=False))
      # Use value labels as group names if defined.
      if labels_as_names:
        labs = grouper[0].sqm.value_labels
        if labs is not None:
          groups = [i[1].rename(labs[i[0]]) for i in partition]
        else:
          groups = [i[1].rename(i[0]) for i in partition]
      else:
        groups = [i[1].rename(i[0]) for i in partition]
    # Post-process.
    # Stacked arrays must be unstacked again.
    if is_spatial:
      groups = [x.sqm.unstack_spatial_dims() for x in groups]
    elif is_multidim:
      # Multi-dimensional grouping may create irregular spatial dimensions.
      # Therefore besides unstacking we also need to regularize the arrays.
      groups = [x.sqm.unstack_all_dims().sqm.regularize() for x in groups]
      # Stacking messes up the spatial feature indices coordinate.
      # We need to re-create this coordinate for each group array.
      if "spatial_feats" in self._obj.coords:
        def fix(x, y):
          x["spatial_feats"] = y["spatial_feats"].reindex_like(x)
          return x
        groups = [fix(x, self._obj) for x in groups]
    # Collect and return.
    out = MetaCollection(groups)
    out.active = True if any([x.sqm.active for x in groups]) else False
    out.locked = True if any([x.sqm.locked for x in groups]) else False
    out.vault = self._merge_arrays_vaults(groups)
    return out

  def reduce(
      self,
      reducer,
      dimension = None,
      track_types = False,
      keep_attrs = True,
      **kwargs
    ):
    """Apply the reduce verb to the array.

    The reduce verb reduces the dimensionality of an array.

    Parameters
    -----------
      reducer : :obj:`callable`
        The reducer function to be applied.
      dimension : :obj:`str`
        Name of the dimension to apply the reduction function to. If
        :obj:`None`, all dimensions are reduced.
      track_types : :obj:`bool`
        Should the reducer promote the value type of the output object, based
        on the value type of the input object?
      keep_attrs: :obj:`bool`
        Should the variable's attributes (attrs) be copied from the
        original object to the new one?
      **kwargs:
        Additional keyword arguments passed on to the reducer function. These
        should not include a keyword argument "dim", which is reserved for
        specifying the dimension to reduce over.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    ------
      :obj:`exceptions.UnknownDimensionError`
        If a dimension with the given name is not present in the array.

    """
    # Get array and set reduction dimension.
    obj = self._obj
    if dimension is not None:
      if dimension == SPACE:
        if X not in obj.dims or Y not in obj.dims:
          raise exceptions.UnknownDimensionError(
            f"Spatial dimensions '{X}' and '{Y}' are not present in the array"
          )
        obj = self.stack_spatial_dims()
      else:
        if dimension not in obj.dims:
          raise exceptions.UnknownDimensionError(
            f"Dimension '{dimension}' is not present in the array"
          )
      kwargs["dim"] = dimension
    # If reduce-over-time persist temporal indices in vault.
    # Rationale: temporal indices are lost when reducing over time but
    # are needed as part of the response object.
    if (
        self.active and 
        (dimension == TIME or (dimension is None and TIME in obj.dims))
      ):
      # Reduce additional dimensions first.
      if obj.ndim > 3:
          dim = [x for x in obj.dims if x not in [TIME, X, Y]]
          assert len(dim) == 1, "Only one dimension can be reduced"
          obj_to_save = obj.sqm.reduce(
            reducers.any_,
            dim[0],
            track_types = track_types
          )
      else:
          obj_to_save = obj
      # Extract valid temporal indices & set as attrs.
      reduce_dims = [x for x in obj_to_save.dims if x != TIME]
      t_idxs = obj_to_save.time[obj_to_save.isnull().sum(reduce_dims) == 0]
      obj.sqm.vault = t_idxs
    # Reduce.
    out = reducer(
      obj,
      track_types = track_types,
      keep_attrs = keep_attrs,
      **kwargs
    )
    return out

  def shift(self, dimension, steps, **kwargs):
    """Apply the shift verb to the array. This method is structured the same way 
    as it is structured for Array objects but effectively muted,
    i.e. the current object is simply handed back.

    The shift verb shifts the values in an array a given amount of steps along
    a dimension.

    Parameters
    -----------
      dimension : :obj:`str`
        Name of the dimension to shift along.
      steps : :obj:`int`
        Amount of steps each value should be shifted. A negative integer will
        result in a shift to the left, while a positive integer will result in
        a shift to the right. A shift along the spatial dimension follows the
        pixel order defined by the CRS, e.g. starting in the top-left and
        moving down each column.
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    """
    out = self._obj
    out.sqm.locked = True
    return out

  def smooth(self, reducer, dimension, size, limit = 2, fill = False,
             track_types = False, **kwargs):
    """Apply the smooth verb to the array. This method is structured the same way 
    as it is structured for Array objects but effectively muted,
    i.e. the current object is simply handed back.

    The smooth verb smoothes the values in an array by applying a reducer
    function to a rolling window along a dimension.

    Parameters
    -----------
      reducer : :obj:`callable`
        The reducer function to be applied to the rolling window.
      dimension : :obj:`str`
        Name of the dimension to smooth along.
      size : :obj:`int`
        Size k defining the extent of the rolling window. The pixel being
        smoothed will always be in the center of the window, with k pixels at
        its left and k pixels at its right. If the dimension to smooth over is
        the spatial dimension, the size will be used for both the X and Y
        dimension, forming a sqmuare window with the smoothed pixel in the
        middle.
      limit : :obj:`int`
        Minimum number of valid data values inside a window. If the window
        contains less than this number of data values (excluding nodata) the
        smoothed value will be nodata.
      fill : :obj:`bool`
        Should pixels with a nodata value also be smoothed?
      track_types : :obj:`bool`
        Should the reducer promote the value type of the output object, based
        on the value type of the input object?
      **kwargs:
        Additional keyword arguments passed on to the reducer function. These
        should not include a keyword argument "dim", which is reserved for
        specifying the dimension to reduce over.

    Returns
    --------
      :obj:`xarray.DataArray`

    """
    out = self._obj
    out.sqm.locked = True
    return out

  def trim(self, dimension = None, **kwargs):
    """Apply the trim verb to the array.

    The trim verb trims the dimensions of an array, meaning that all dimension
    coordinates for which all values are missing are removed from the array.
    The spatial dimensions are only trimmed at their edges, to preserve their
    regularity.

    Parameters
    ----------
      dimension : :obj:`str`
        Name of the dimension to be trimmed. If :obj:`None`, all dimensions
        will be trimmed.

    Returns
    -------
      :obj:`xarray.DataArray`

    Raises
    ------
      :obj:`exceptions.UnknownDimensionError`
        If a dimension with the given name is not present in the array.

    """
    obj = self._obj
    dims = obj.dims
    if dimension is None:
      if X in dims and Y in dims:
        regular_dims = [d for d in dims if d not in [X, Y]]
        out = self._trim_space(self._trim(obj, regular_dims))
      else:
        out = self._trim(obj, dims)
    else:
      if dimension == SPACE:
        if X not in dims or Y not in dims:
          raise exceptions.UnknownDimensionError(
            f"Spatial dimensions '{X}' and '{Y}' are not present in the array"
          )
        out = self._trim_space(obj)
      else:
        if dimension not in obj.dims:
          raise exceptions.UnknownDimensionError(
            f"Dimension '{dimension}' is not present in the array"
          )
        out = self._trim(obj, [dimension])
    return out

  def delineate(self, track_types = False, **kwargs):
    """Apply the delineate verb to the array. This method is structured the same way 
    as it is structured for Array objects but effectively muted,
    i.e. the current object is simply handed back.

    The delineate verb deliniates spatio-temporal objects in a binary array.

    Parameters
    -----------
      track_types : :obj:`bool`
        Should the value type of the output object be promoted, and should it be
        checked that the input has value type *binary*?
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    """
    return self._obj

  def fill(self, dimension, method, extrapolate = True, track_types = False,
           **kwargs):
    """Apply the fill verb to the array. This method is structured the same way 
    as it is structured for Array objects but effectively muted,
    i.e. the current object is simply handed back.

    The fill verb fills nodata values by interpolating valid data values.

    Parameters
    -----------
      dimension : :obj:`str`
        Name of the dimension along which to interpolate.
      method : :obj:`str`
        Interpolation method to use. One of nearest, linear or cubic. When
        interpolation along the stacked space dimensions, the two-dimensional
        versions of these interpolation methods are used, i.e. 2D nearest
        neighbour, bilinear and bicubic.
      extrapolate : :obj:`bool`
        Should nodata values at the edge be extrapolated? Only applied to
        one-dimensional interpolation.
      track_types : :obj:`bool`
        Should the value type(s) of the input(s) be checked, and the value
        type of the output be promoted, whenever applicable?
      **kwargs"
        Additional keyword arguments passed on to the interpolation function.
        When interpolating along a single dimension, the interpolation function
        is :meth:`xarray.DataArray.interpolate_na`.
        When interpolation along the stacked space dimension, the interpolation
        funtion is :meth:`rioxarray.raster_array.RasterArray.interpolate_na`.

    Returns
    --------
      :obj:`xarray.DataArray`

    """
    out = self._obj
    out.sqm.locked = True
    return out

  def name(self, value, **kwargs):
    """Apply the name verb to the array.

    The name verb assigns a name to an array.

    Parameters
    -----------
      value : :obj:`str`
        Character sting to be assigned as name to the input array.
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    """
    out = self._obj.rename(value)
    return out

  def apply_custom(self, verb, track_types = False, **kwargs):
    """Apply a user-defined verb to the array. This method is structured the 
    same way as it is structured for Array objects but effectively muted,
    i.e. the current object is simply handed back.

    Parameters
    -----------
      verb : :obj:`callable`
        Implementation of the custom verb which will be provided to
        :meth:`xarray.DataArray.pipe`.
      track_types : :obj:`bool`
        Should the value type(s) of the input(s) be checked, and the value
        type of the output be promoted, whenever applicable?
      **kwargs:
        Additional keyword arguments passed on to the verb function.

    Returns
    --------
      :obj:`xarray.DataArray`

    """
    out = self._obj
    return out

  def align_with(self, other):
    """Align the array to the shape of another array.

    An input array is alinged to another array if the pixel at position *i* in
    the input array has the same coordinates as the pixel at position *i* in the
    other array. Aligning can be done in several ways:

    * Consider the case where the input array has exactly the same dimensions
      and coordinates as the other array, but the order of them is different.
      In that case, the input array is simply re-ordered to match the other
      array.

    * Consider the case where the input array has the same dimensions as the
      other array, but not all coordinates match. In that case, the coordinates
      that are in the input array but not in the other array are removed from the
      input array, and at the same time the coordinates that are in the other
      array but not in the input array are added to the input array, with nodata
      values assigned.

    * Consider the case where all dimensions of the input array are also present
      in the other array, but not all dimensions of the other array are present
      in the input array. In that case, the pixels of the input array are
      duplicated along those dimensions that are missing.

    Alignment may also be a combination of more than one of these ways.

    Parameters
    -----------
      other : :obj:`xarray.DataArray`
        Array to which the input array should be aligned.

    Returns
    --------
      :obj:`xarray.DataArray`
        The aligned input array.

    Raises
    -------
      :obj:`exceptions.AlignmentError`
        If the input array cannot be aligned to the other array, for example when
        the two arrays have no dimensions in common at all, or when the input
        array has dimensions that are not present in the other array.

    """
    # Reintroduce time dimension if it was removed.
    if (
      TIME in other.dims and 
      TIME not in self._obj.dims and
      self._obj.sqm.vault is not None
    ):
      self._obj = self._obj.expand_dims({'time': self._obj.sqm.vault})
    # Carry out alignment.
    out = xr.align(other, self._obj, join = "left")[1].broadcast_like(other)
    if not out.shape == other.shape:
      raise exceptions.AlignmentError(
        f"Array '{other.name if other.name is not None else 'y'}' "
        f"cannot be aligned with "
        f"input array '{self._obj.name if self._obj.name is not None else 'x'}'"
      )
    return out

  def _merge_arrays_vaults(self, arrays):
    """Merge vaults of arrays by taking the union of their elements."""
    if any([x.sqm.active and x.sqm.vault is not None for x in arrays]):
      vaults = [x.sqm.vault for x in arrays if (x.sqm.active and x.sqm.vault is not None)]
      vault = np.unique(np.concatenate(vaults))
      out = xr.DataArray(vault, dims=TIME, name=TIME)
    else:
      out = None
    return out


class Collection(list):
  """Internal representation of a collection of multiple arrays.
  To be used in conjunction with the `semantique.processor.QueryProcessor` or
  `semantique.processor.FakeProcessor`.

  Parameters
  ----------
    list_obj : :obj:`list` of :obj:`xarray.DataArray`
      The elements of the collection stored in a list.

  """

  def __init__(self, list_obj):
    super(Collection, self).__init__(list_obj)

  @property
  def sq(self):
    """self: Semantique accessor.

    This is merely provided to ensure compatible behaviour with
    :obj:`Array <semantique.processor.arrays.Array>` objects, which are
    modelled as an accessor to :obj:`xarray.DataArray` objects. It allows to
    call all other properties and methods through the prefix ``.sq``.

    """
    return self

  @property
  def is_empty(self):
    """:obj:`bool`: Are all elements of the collection empty arrays."""
    return all([x.sq.is_empty for x in self])

  def compose(self, track_types = True, **kwargs):
    """Apply the compose verb to the collection.

    The compose verb creates a categorical composition from the arrays in the
    collection.

    Parameters
    -----------
      track_types : :obj:`bool`
        Should it be checked if all arrays in the collection have value type
        *binary*?
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    ------
      :obj:`exceptions.InvalidValueTypeError`
        If ``track_types = True`` and the value type of at least one of the
        arrays in the collection is not *binary*.

    """
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
    out.sq.value_labels = {k:v for k, v in zip(idxs, labels)}
    return out

  def concatenate(self, dimension, track_types = True,
                  vtype = "nominal", **kwargs):
    """Apply the concatenate verb to the collection.

    The concatenate verb concatenates the arrays in the collection along a new
    or existing dimension.

    Parameters
    -----------
      dimension : :obj:`str`
        Name of the dimension to concatenate along. To concatenate along an
        existing dimension, it should be a dimension that exists in all
        collection members. To concatenate along a new dimension, it should be
        a dimension that does not exist in any of the collection members.
      track_types : :obj:`bool`
        Should it be checked if all arrays in the collection have the same value
        type?
      vtype : :obj:`str`:
        If the arrays are concatenated along a new dimension, what should the
        value type of its dimension coordinates be? Valid options are
        "continuous", "discrete", "nominal", "ordinal" and "binary".
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    ------
      :obj:`exceptions.InvalidValueTypeError`
        If ``track_types = True`` and the value types of the arrays in the
        collection are not all equal to each other.

      :obj:`exceptions.MissingDimensionError`
        If the dimension to concatenate along exists in some but not all
        arrays in the collection.

      :obj:`exceptions.ReservedDimensionError`
        If the new dimension to concatenate along has one of the names that
        semantique reserves for the temporal dimension or spatial dimensions.

    """
    # Check value types.
    if track_types:
      value_types = [x.sq.value_type for x in self]
      if not all([x is None or x == value_types[0] for x in value_types]):
        raise exceptions.InvalidValueTypeError(
          f"Element value types for 'concatenate' should all be the same, "
          f"not {np.unique(value_types).tolist()} "
        )
    # Concatenate.
    has_dim = [dimension in x.dims for x in self]
    if any(has_dim):
      if all(has_dim):
        # Concatenate over existing dimension.
        raw = xr.concat([x for x in self], dimension)
        has_duplicated_coords = any(raw.get_index(dimension).duplicated())
        if has_duplicated_coords:
          # If arrays have overlapping coordinates for this dimension:
          # --> Choose the first non-missing value to be in the output.
          # Do this by:
          # --> Creating groups for each coordinate value.
          # --> Merge these groups using the "first" reducer.
          # --> Concatenate the merged groups back together.
          def _merge_dups(obj):
            coords = obj.get_index(dimension).values
            if len(coords) > 1:
              dups = [obj.isel({dimension: i}) for i, x in enumerate(coords)]
              return Collection(dups).sq.merge(reducers.first_)
            else:
              return obj
          groups = list(raw.groupby(dimension, squeeze=False))
          clean = xr.concat([_merge_dups(x[1]) for x in groups], dimension)
        else:
          clean = raw
        out = clean.sortby(dimension)
      else:
        raise exceptions.MissingDimensionError(
          f"Concatenation dimension '{dimension}' exists in some but not all "
          "arrays in the collection"
        )
    else:
      # Concatenate over new dimension.
      if dimension in [TIME, SPACE, X, Y]:
        raise exceptions.ReservedDimensionError(
          f"Dimension name '{dimension}' is reserved and should not be used "
          "as a new dimension name"
        )
      names = [x.name for x in self]
      coords = pd.Index(names, name = dimension, tupleize_cols = False)
      out = xr.concat([x for x in self], coords)
      out[dimension].sq.value_type = vtype
      out[dimension].sq.value_labels = {x:x for x in names}
    # Update value labels.
    if track_types:
      orig_labs = [x.sq.value_labels for x in self]
      if None not in orig_labs:
        # If keys are duplicated first array should be prioritized.
        # Therefore we first reverse the list of value label dictionaries.
        orig_labs.reverse()
        new_labs = {k:v for x in orig_labs for k,v in x.items()}
        out.sq.value_labels = new_labs
      else:
        del out.sq.value_labels
    else:
      del out.sq.value_labels
    # Return.
    return out

  def merge(self, reducer, track_types = True, **kwargs):
    """Apply the merge verb to the collection.

    The merge verb merges the pixel values of all arrays in the collection into
    a single value per pixel.

    Parameters
    -----------
      reducer : :obj:`str`
        Name of the reducer function to be applied in order to reduce multiple
        values per pixel into a single value. Should either be one of the
        built-in reducers of semantique, or a user-defined reducer which will
        be provided to the query processor when executing the query recipe.
      track_types : :obj:`bool`
        Should it be checked if all arrays in the collection have the same value
        type, and should the reducer promote the value type of the output
        object, based on the value type of the input objects?
      **kwargs:
        Additional keyword arguments passed on to the reducer function.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    ------
      :obj:`exceptions.InvalidValueTypeError`
        If ``track_types = True`` and the value types of the arrays in the
        collection are not all equal to each other.

    """
    if track_types:
      value_types = [x.sq.value_type for x in self]
      if not all([x is None or x == value_types[0] for x in value_types]):
        raise exceptions.InvalidValueTypeError(
          f"Element value types for 'merge' should all be the same, "
          f"not {np.unique(value_types).tolist()} "
        )
    dim = "__sq__" # Temporary dimension.
    concat = self.concatenate(dim, track_types = False)
    out = concat.sq.reduce(reducer, dim, track_types, **kwargs)
    return out

  def evaluate(self, operator, y = None, track_types = True, **kwargs):
    """Apply the evaluate verb to all arrays in the collection.

    See :meth:`Array.evaluate`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([operator, y, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sq.evaluate(*args, **kwargs) for x in out]
    return out

  def extract(self, dimension, component = None, **kwargs):
    """Apply the extract verb to all arrays in the collection.

    See :meth:`Array.extract`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([dimension, component])
    out = copy.deepcopy(self)
    out[:] = [x.sq.extract(*args, **kwargs) for x in out]
    return out

  def filter(self, filterer, track_types = True, **kwargs):
    """Apply the filter verb to all arrays in the collection.

    See :meth:`Array.filter`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([filterer, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sq.filter(*args, **kwargs) for x in out]
    return out

  def assign(self, y, at = None, track_types = True, **kwargs):
    """Apply the assign verb to all arrays in the collection.

    See :meth:`Array.assign`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([y, at, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sq.assign(*args, **kwargs) for x in out]
    return out

  def reduce(self, reducer, dimension = None, track_types = True, **kwargs):
    """Apply the reduce verb to all arrays in the collection.

    See :meth:`Array.reduce`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([reducer, dimension, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sq.reduce(*args, **kwargs) for x in out]
    return out

  def shift(self, dimension, steps, **kwargs):
    """Apply the shift verb to all arrays in the collection.

    See :meth:`Array.shift`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([dimension, steps])
    out = copy.deepcopy(self)
    out[:] = [x.sq.shift(*args, **kwargs) for x in out]
    return out

  def smooth(self, reducer, dimension, size, limit = 2, fill = False,
             track_types = True, **kwargs):
    """Apply the smooth verb to all arrays in the collection.

    See :meth:`Array.smooth`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([reducer, dimension, size, limit, fill, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sq.smooth(*args, **kwargs) for x in out]
    return out

  def trim(self, dimension = None, **kwargs):
    """Apply the trim verb to all arrays in the collection.

    See :meth:`Array.trim`

    Returns
    -------
      :obj:`Collection`

    """
    out = copy.deepcopy(self)
    out[:] = [x.sq.trim(dimension, **kwargs) for x in out]
    return out

  def delineate(self, track_types = True, **kwargs):
    """Apply the delineate verb to all arrays in the collection.

    See :meth:`Array.delineate`

    Returns
    -------
      :obj:`Collection`

    """
    out = copy.deepcopy(self)
    out[:] = [x.sq.delineate(track_types, **kwargs) for x in out]
    return out

  def fill(self, dimension, method, track_types = True, **kwargs):
    """Apply the fill verb to all arrays in the collection.

    See :meth:`Array.fill`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([dimension, method, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sq.fill(*args, **kwargs) for x in out]
    return out

  def name(self, value, **kwargs):
    """Apply the name verb to all arrays in the collection.

    See :meth:`Array.name`

    Returns
    -------
      :obj:`Collection`

    """
    out = copy.deepcopy(self)
    out[:] = [x.sq.name(value, **kwargs) for x in out]
    return out

  def apply_custom(self, verb, track_types = True, **kwargs):
    """Apply a user-defined verb to all arrays in the collection.

    See :meth:`Array.apply_custom`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([verb, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sq.apply_custom(*args, **kwargs) for x in out]
    return out

  def regularize(self):
    """Regularize the spatial dimension of all arrays in the collection.

    See :meth:`Array.regularize`

    Returns
    -------
      :obj:`Collection`

    """
    out = copy.deepcopy(self)
    out[:] = [x.sq.regularize() for x in out]
    return out

  def stack_spatial_dims(self):
    """Stack the spatial dimensions for all arrays in the collection.

    See :meth:`Array.stack_spatial_dims`

    Returns
    -------
      :obj:`Collection`

    """
    out = copy.deepcopy(self)
    out[:] = [x.sq.stack_spatial_dims() for x in out]
    return out

  def unstack_spatial_dims(self):
    """Unstack the spatial dimensions for all arrays in the collection.

    See :meth:`Array.unstack_spatial_dims`

    Returns
    -------
      :obj:`Collection`

    """
    out = copy.deepcopy(self)
    out[:] = [x.sq.unstack_spatial_dims() for x in out]
    return out

class MetaCollection(Collection):
  """Internal representation of a collection of multiple arrays.
  To be used in conjunction with the `semantique.processor.FilterProcessor`.

  Note: Track-type attributes are kept to have a compatible structure with the
  `semantique.processor.Collection` but are effectively ignored.

  Parameters
  ----------
    list_obj : :obj:`list` of :obj:`xarray.DataArray`
      The elements of the collection stored in a list.

  """

  def __init__(self, list_obj):
    # Only active objects should be given any weight when processing MetaCollections
    # Init merges active objects & sets values of inactive obj to extent of active obj
    if any([x.sqm.active for x in list_obj]):
      # Create empty response obj.
      tidxs = []
      for arr in list_obj:
        if arr.sqm.active:
          # Reduce arrays to 3 dimensions.
          if arr.ndim > 3:
            dim = [x for x in arr.dims if x not in [TIME, "x", "y"]]
            assert len(dim) == 1, "Only one dimension can be reduced"
            arr = arr.sqm.reduce(reducers.any_, dim[0], track_types=False)
          # Extract temporal indices from array itself.
          if TIME in arr.dims:
            reduce_dims = [x for x in arr.dims if x != TIME]
            tidxs.append(arr.time[arr.isnull().sum(reduce_dims) == 0])
          # Extract saved results from vault.
          if arr.sqm.vault is not None:
            tidxs.append(arr.sqm.vault)
      # Combine results.
      if len(tidxs) > 1:
        tidxs = np.concatenate(tidxs)
      tidxs = xr.DataArray(np.unique(tidxs), dims=TIME, name=TIME)
      # Broadcast to equal time dimension.
      if (
        any([TIME in x.dims for x in list_obj]) and 
        not all([TIME in x.dims for x in list_obj])
      ):
        for i,arr in enumerate(list_obj):
          if TIME not in arr.dims:
            list_obj[i] = arr.expand_dims({'time': tidxs})
      # Fill inactive arrays with ones at coords where tidxs are given, else NaN.
      if any([TIME in x.dims for x in list_obj]):
        for x in list_obj:
          if not x.sqm.active:
            cond = x.time.isin(tidxs).broadcast_like(x)
            x.values = xr.where(cond, xr.ones_like(x), xr.full_like(x, np.NaN))
      # Create response object.
      super(MetaCollection, self).__init__(list_obj)
      self._active = True
      self._vault = self._merge_arrays_vaults(list_obj)
    else:
      # Create response object.
      super(MetaCollection, self).__init__(list_obj)
      self._active = False
      self._vault = None
    self._locked = True if any([x.sqm.locked for x in list_obj]) else False

  @property
  def sqm(self):
    """self: Semantique accessor. This is merely provided to ensure compatible
    behaviour with :obj:`Array <semantique.processor.arrays.MetaArray>` objects."""
    return self

  @property
  def is_empty(self):
    """:obj:`bool`: Are all elements of the collection empty arrays."""
    return all([x.sqm.is_empty for x in self])

  @property
  def active(self):
    """:obj:`bool`: Is the current object actively monitored?
    This guides how the object is evaluated in some operations
    Active objects should be those that contain data from the
    current `watch_layer` (processor.core.FilterProcessor).
    """
    return self._active

  @active.setter
  def active(self, value):
    self._active = value

  @active.deleter
  def active(self):
    self._active = None

  @property
  def locked(self):
    """:obj:`bool`: Is the evaluation of filter operations restricted?
    If locked, filter operations will be ignored."""
    return self._locked

  @locked.setter
  def locked(self, value):
    self._locked = value

  @locked.deleter
  def locked(self):
    self._locked = None

  @property
  def vault(self):
    """:obj:`list`: Tresor which can be used to store timestamps
    if the time dimension in the array itself has been reduced away."""
    return self._vault

  @vault.setter
  def vault(self, value):
    self._vault = value

  @vault.deleter
  def vault(self):
    self._vault = None
      
  def compose(self, track_types = False, **kwargs):
    """Apply the compose verb to the collection.

    The compose verb creates a categorical composition from the arrays in the
    collection.

    Parameters
    -----------
      track_types : :obj:`bool`
        Should it be checked if all arrays in the collection have value type
        *binary*?
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    """
    out = super(MetaCollection, self).compose(
      track_types=track_types,
      **kwargs
    )
    out.sqm.active = self.active
    out.sqm.locked = self.locked
    out.sqm.vault = self.vault
    return out

  def concatenate(self, dimension, track_types = False,
                  vtype = "nominal", **kwargs):
    """Apply the concatenate verb to the collection.

    The concatenate verb concatenates the arrays in the collection along a new
    or existing dimension.

    Parameters
    -----------
      dimension : :obj:`str`
        Name of the dimension to concatenate along. To concatenate along an
        existing dimension, it should be a dimension that exists in all
        collection members. To concatenate along a new dimension, it should be
        a dimension that does not exist in any of the collection members.
      track_types : :obj:`bool`
        Should it be checked if all arrays in the collection have the same value
        type?
      vtype : :obj:`str`:
        If the arrays are concatenated along a new dimension, what should the
        value type of its dimension coordinates be? Valid options are
        "continuous", "discrete", "nominal", "ordinal" and "binary".
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    Raises
    ------
      :obj:`exceptions.MissingDimensionError`
        If the dimension to concatenate along exists in some but not all
        arrays in the collection.

      :obj:`exceptions.ReservedDimensionError`
        If the new dimension to concatenate along has one of the names that
        semantique reserves for the temporal dimension or spatial dimensions.

    """
    out = super(MetaCollection, self).concatenate(
      dimension,
      track_types=track_types,
      vtype=vtype,
      **kwargs
    )
    out.sqm.active = self.active
    out.sqm.locked = self.locked
    out.sqm.vault = self.vault
    return out

  def merge(self, reducer, track_types = False, **kwargs):
    """Apply the merge verb to the collection.

    The merge verb merges the pixel values of all arrays in the collection into
    a single value per pixel.

    Parameters
    -----------
      reducer : :obj:`str`
        Name of the reducer function to be applied in order to reduce multiple
        values per pixel into a single value. Should either be one of the
        built-in reducers of semantique, or a user-defined reducer which will
        be provided to the query processor when executing the query recipe.
      track_types : :obj:`bool`
        Should it be checked if all arrays in the collection have the same value
        type, and should the reducer promote the value type of the output
        object, based on the value type of the input objects?
      **kwargs:
        Additional keyword arguments passed on to the reducer function.

    Returns
    --------
      :obj:`xarray.DataArray`

    """
    dim = "__sqm__" # Temporary dimension.
    concat = self.concatenate(dim, track_types = track_types)
    out = concat.sqm.reduce(reducer, dim, track_types, **kwargs)
    return out

  def evaluate(self, operator, y = None, track_types = False, **kwargs):
    """Apply the evaluate verb to all arrays in the collection.

    See :meth:`Array.evaluate`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([operator, y, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sqm.evaluate(*args, **kwargs) for x in out]
    return out

  def extract(self, dimension, component = None, **kwargs):
    """Apply the extract verb to all arrays in the collection.

    See :meth:`Array.extract`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([dimension, component])
    out = copy.deepcopy(self)
    out[:] = [x.sqm.extract(*args, **kwargs) for x in out]
    return out

  def filter(self, filterer, track_types = False, **kwargs):
    """Apply the filter verb to all arrays in the collection.

    See :meth:`Array.filter`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([filterer, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sqm.filter(*args, **kwargs) for x in out]
    return out

  def assign(self, y, at = None, track_types = False, **kwargs):
    """Apply the assign verb to all arrays in the collection.

    See :meth:`Array.assign`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([y, at, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sqm.assign(*args, **kwargs) for x in out]
    return out

  def reduce(self, reducer, dimension = None, track_types = False, **kwargs):
    """Apply the reduce verb to all arrays in the collection.

    See :meth:`Array.reduce`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([reducer, dimension, track_types])
    out = copy.deepcopy(self)
    list_obj = [x.sqm.reduce(*args, **kwargs) for x in out]
    out = MetaCollection(list_obj)
    return out

  def shift(self, dimension, steps, **kwargs):
    """Apply the shift verb to all arrays in the collection.

    See :meth:`Array.shift`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([dimension, steps])
    out = copy.deepcopy(self)
    out[:] = [x.sqm.shift(*args, **kwargs) for x in out]
    return out

  def smooth(self, reducer, dimension, size, limit = 2, fill = False,
             track_types = False, **kwargs):
    """Apply the smooth verb to all arrays in the collection.

    See :meth:`Array.smooth`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([reducer, dimension, size, limit, fill, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sqm.smooth(*args, **kwargs) for x in out]
    return out

  def trim(self, dimension = None, **kwargs):
    """Apply the trim verb to all arrays in the collection.

    See :meth:`Array.trim`

    Returns
    -------
      :obj:`Collection`

    """
    out = copy.deepcopy(self)
    out[:] = [x.sqm.trim(dimension, **kwargs) for x in out]
    return out

  def delineate(self, track_types = False, **kwargs):
    """Apply the delineate verb to all arrays in the collection.

    See :meth:`Array.delineate`

    Returns
    -------
      :obj:`Collection`

    """
    out = copy.deepcopy(self)
    out[:] = [x.sqm.delineate(track_types, **kwargs) for x in out]
    return out

  def fill(self, dimension, method, track_types = False, **kwargs):
    """Apply the fill verb to all arrays in the collection.

    See :meth:`Array.fill`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([dimension, method, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sqm.fill(*args, **kwargs) for x in out]
    return out

  def name(self, value, **kwargs):
    """Apply the name verb to all arrays in the collection.

    See :meth:`Array.name`

    Returns
    -------
      :obj:`Collection`

    """
    out = copy.deepcopy(self)
    out[:] = [x.sqm.name(value, **kwargs) for x in out]
    return out

  def apply_custom(self, verb, track_types = False, **kwargs):
    """Apply a user-defined verb to all arrays in the collection.

    See :meth:`Array.apply_custom`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([verb, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sqm.apply_custom(*args, **kwargs) for x in out]
    return out

  def regularize(self):
    """Regularize the spatial dimension of all arrays in the collection.

    See :meth:`Array.regularize`

    Returns
    -------
      :obj:`Collection`

    """
    out = copy.deepcopy(self)
    out[:] = [x.sqm.regularize() for x in out]
    return out

  def stack_spatial_dims(self):
    """Stack the spatial dimensions for all arrays in the collection.

    See :meth:`Array.stack_spatial_dims`

    Returns
    -------
      :obj:`Collection`

    """
    out = copy.deepcopy(self)
    out[:] = [x.sqm.stack_spatial_dims() for x in out]
    return out

  def unstack_spatial_dims(self):
    """Unstack the spatial dimensions for all arrays in the collection.

    See :meth:`Array.unstack_spatial_dims`

    Returns
    -------
      :obj:`Collection`

    """
    out = copy.deepcopy(self)
    out[:] = [x.sqm.unstack_spatial_dims() for x in out]
    return out

  def _merge_arrays_vaults(self, arrays):
    """Merge vaults of arrays by taking the union of their elements."""
    if any([x.sqm.active and x.sqm.vault is not None for x in arrays]):
      vaults = [x.sqm.vault for x in arrays if (x.sqm.active and x.sqm.vault is not None)]
      vault = np.unique(np.concatenate(vaults))
      out = xr.DataArray(vault, dims=TIME, name=TIME)
    else:
      out = None
    return out