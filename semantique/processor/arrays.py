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

from semantique import exceptions
from semantique.processor import operators, utils

TIME = "time"
""":obj:`str` : Reserved name for the temporal dimension."""

SPACE = "space"
""":obj:`str` : Reserved name for the stacked spatial (X, Y) dimension."""

X = "x"
""":obj:`str` : Reserved name for the spatial X dimension."""

Y = "y"
""":obj:`str` : Reserved name for the spatial Y dimension."""

@xr.register_dataarray_accessor("sq")
class Array():
  """Internal representation of a multi-dimensional array.

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
    """:obj:`str`: The value type of the array.

    Valid options are: ::

      'numerical', 'nominal', 'ordinal', 'boolean'

    """
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
    """:obj:`list`: Spatial resolution of the array in units of the CRS."""
    return self._obj.sq.unstack_spatial_dims().rio.resolution()[::-1]

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
      return False

  @property
  def grid_points(self):
    """:obj:`geopandas.GeoSeries`: Spatial grid points of the array."""
    if SPACE not in self._obj.dims:
      return None
    # Extract spatial coordinates.
    xcoords = self._obj[SPACE][X]
    ycoords = self._obj[SPACE][Y]
    # Return grid points as geometries.
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
    out = operator(*operands, track_types = track_types, **kwargs)
    return out

  def extract(self, dimension, component = None, track_types = True, **kwargs):
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
      track_types : :obj:`bool`
        Should the extracted object get a value type assigned?
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
    # Extract dimension coordinates.
    try:
      coords = self._obj[dimension]
    except KeyError:
      raise exceptions.UnknownDimensionError(
        f"Dimension '{dimension}' is not present in the input object"
      )
    # Extract component of dimensions coordinates if specified.
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
        if dimension == SPACE and component in [X, Y]:
          out = utils.parse_coords_component(out)
    out._variable = out._variable.to_base_variable()
    if not track_types:
      del out.sq.value_type
      del out.sq.value_labels
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
        (i.e. assigned a missing data value).
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
    computation. It only assigns to non-missing pixels. Hence, pixels with
    missing values in the input are always preserved in the output.

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
        Data array containing a single dimension that is also present in the
        input array. The group to which each pixel in the input array will be
        assigned depends on the value of the grouper that has the same
        coordinate for that dimension. Alternatively it may be a array
        collection in which each array meets the requirements above. In that
        case, groups are defined by the unique combinations of corresponding
        values in all collection members.
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
        If the grouper dimension is not present in the input object.
      :obj:`exceptions.TooManyDimensionsError`
        If the grouper has more than one dimension.
      :obj:`exceptions.MixedDimensionsError`
        If the grouper is a collection and its elements don't all have the same
        dimensions.

    """
    # Validate grouper.
    if isinstance(grouper, list):
      multiple = True
      dims = [x.dims for x in grouper]
    else:
      multiple = False
      dims = [grouper.dims]
    if not all([len(x) == 1 for x in dims]):
      raise exceptions.TooManyDimensionsError(
        "Groupers must be one-dimensional"
      )
    if not all([x == dims[0] for x in dims]):
      raise exceptions.MixedDimensionsError(
        "Dimensions of grouper arrays do not match"
      )
    if not dims[0][0] in self._obj.dims:
      raise exceptions.MissingDimensionError(
        f"Grouper dimension '{dims[0]}' does not exist in the input object"
      )
    # Split input object into groups.
    if multiple:
      idx = pd.MultiIndex.from_arrays([x.data for x in grouper])
      dim = grouper[0].dims
      partition = list(self._obj.groupby(xr.IndexVariable(dim, idx)))
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
      partition = list(self._obj.groupby(grouper))
      # Use value labels as group names if defined.
      if labels_as_names:
        labs = grouper.sq.value_labels
        if labs is not None:
          groups = [i[1].rename(labs[i[0]]) for i in partition]
        else:
          groups = [i[1].rename(i[0]) for i in partition]
      else:
        groups = [i[1].rename(i[0]) for i in partition]
    out = Collection(groups)
    return out

  def reduce(self, reducer, dimension = None, track_types = True, **kwargs):
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
    if dimension is not None:
      if dimension not in self._obj.dims:
        raise exceptions.UnknownDimensionError(
          f"Dimension '{dimension}' is not present in the input object"
        )
      kwargs["dim"] = dimension
    out = reducer(self._obj, track_types = track_types, **kwargs)
    return out

  def shift(self, dimension, steps, coord = None, **kwargs):
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
        a shift to the right.
      coord : :obj:`str`
        Either "x" or "y", specifying if values should be shifted in
        respectively the X or Y direction of the spatial dimension. If
        :obj:`None`, a shift along the spatial dimension follows the pixel
        order defined by the CRS, e.g. starting in the top-left and moving down
        each column. Ignored when ``dimension`` is not the spatial dimension.
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
    # Check dimension presence.
    if dimension not in self._obj.dims:
      raise exceptions.UnknownDimensionError(
        f"Dimension '{dimension}' is not present in the input object"
      )
    # Shift values.
    # Spatial dimension needs special treatment if coord is specified.
    if dimension == SPACE and coord is not None:
      obj = self._obj.unstack(dimension)
      if coord not in obj.dims:
        raise exceptions.UnknownComponentError(
          f"Spatial dimension does not have coordinate '{coord}'"
        )
      out = obj.shift({coord: steps}).sq.stack_spatial_dims()
    else:
      out = self._obj.shift({dimension: steps})
    return out

  def smooth(self, reducer, dimension, size, coord = None, track_types = True,
             **kwargs):
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
        the spatial dimension and ``coord`` is not specified, the size will be
        used for both the X and Y dimension, forming a square window with the
        smoothed pixel in the middle.
      coord : :obj:`str`
        Either "x" or "y", specifying if the moving window should be constructed
        in respectively the X or Y direction of the spatial dimension. If
        :obj:`None`, the window is constructed in both directions, forming a
        two-dimensional square. Ignored when ``dimension`` is not the spatial
        dimension.
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
    if dimension not in self._obj.dims:
      raise exceptions.UnknownDimensionError(
        f"Dimension '{dimension}' is not present in the input object"
      )
    # Parse size.
    # Size parameter defines neighborhood size at each side of the pixel.
    size = size * 2 + 1
    # Create the rolling window object.
    # Spatial dimension needs special treatment because it is stacked.
    if dimension == SPACE:
      spatial = True
      obj = self._obj.unstack(dimension)
      if coord is None:
        obj = obj.rolling({X: size, Y: size}, center = True)
      else:
        if coord not in obj.dims:
          raise exceptions.UnknownComponentError(
            f"Spatial dimension does not have coordinate '{coord}'"
          )
        obj = obj.rolling({coord: size}, center = True)
    else:
      spatial = False
      obj = self._obj.rolling({dimension: size}, center = True)
    # Apply the reducer to each window.
    out = reducer(obj, track_types = track_types, **kwargs)
    # Stack and return.
    if spatial:
      out = out.sq.stack_spatial_dims()
    return out

  def trim(self, dimension = None, force_regular = True):
    """Apply the trim verb to the array.

    The trim verb trims the dimensions of an array, meaning that all dimension
    coordinates for which all values are missing are removed from the array.

    Parameters
    ----------
      dimension : :obj:`str`
        Name of the dimension to be trimmed. If :obj:`None`, all dimensions
        will be trimmed.
      force_regular : :obj:`bool`
        If the spatial dimension should be trimmed, should the regularity of
        this dimension be preserved? If :obj:`True` the spatial dimensions are
        trimmed only at their edges.

    Returns
    -------
      :obj:`xarray.DataArray`

    Raises
    ------
      :obj:`exceptions.UnknownDimensionError`
        If a dimension with the given name is not present in the array.

    """
    # Determine dimensions to be trimmed.
    if dimension is None:
      trim_dims = self._obj.dims # Trim all dims.
    else:
      if dimension not in self._obj.dims:
        raise exceptions.UnknownDimensionError(
          f"Dimension '{dimension}' is not present in the input object"
        )
      trim_dims = [dimension]
    # Define sub-functions for regular and spatial trimming.
    def _trim_regular(obj, dims):
      for dim in dims:
        other_dims = [d for d in obj.dims if d != dim]
        out = obj.isel({dim: obj.count(other_dims) > 0})
        return out
    def _trim_space(obj, other_dims):
      # Find the smallest and largest spatial coords containing valid values.
      obj = obj.unstack(SPACE)
      y_idxs = np.nonzero(obj.count(other_dims + [X]).data)[0]
      x_idxs = np.nonzero(obj.count(other_dims + [Y]).data)[0]
      y_slice = slice(y_idxs.min(), y_idxs.max() + 1)
      x_slice = slice(x_idxs.min(), x_idxs.max() + 1)
      # Limit the x and y coordinates to only those ranges.
      out = obj.isel({Y: y_slice, X: x_slice})
      # Stack x and y dimensions back together.
      out = out.sq.stack_spatial_dims()
      return out
    # Trim.
    obj = self._obj
    if dimension is not None:
      if force_regular and dimension == SPACE:
        other_dims = [d for d in obj.dims if d != SPACE]
        out = _trim_space(obj, other_dims)
      else:
        out = _trim_regular(obj, trim_dims)
    else:
      if force_regular and SPACE in obj.dims:
        other_dims = [d for d in obj.dims if d != SPACE]
        out = _trim_space(_trim_regular(obj, other_dims), other_dims)
      else:
        out = _trim_regular(obj, trim_dims)
    return out

  def delineate(self, **kwargs):
    """Apply the delineate verb to the array.

    The delineate verb deliniates spatio-temporal objects in a binary array.

    Parameters
    -----------
      **kwargs:
        Ignored.

    Returns
    --------
      :obj:`xarray.DataArray`

    """
    obj = xr.apply_ufunc(utils.null_as_zero, self._obj)
    dims = obj.dims
    is_spatial = SPACE in dims
    is_temporal = TIME in dims
    if is_spatial and is_temporal:
      if len(dims) > 2:
        raise exceptions.TooManyDimensionsError(
          f"Delineate is only supported for arrays with dimension '{TIME}' "
          f"and/or '{SPACE}', not: {list(dims)}"
        )
      obj = obj.transpose(TIME, SPACE) # Make sure dimension order is correct.
      obj = obj.unstack(SPACE) # Unstack spatial dims.
      nb = np.array([
        [[0,0,0],[0,1,0],[0,0,0]],
        [[1,1,1],[1,1,1],[1,1,1]],
        [[0,0,0],[0,1,0],[0,0,0]]
      ])
    elif is_spatial or is_temporal:
      if len(dims) > 1:
        raise exceptions.TooManyDimensionsError(
          f"Delineate is only supported for arrays with dimension '{TIME}' "
          f"and/or '{SPACE}', not: {list(dims)}"
        )
      if is_spatial:
        obj = obj.unstack(SPACE)
        nb = np.array([
          [1,1,1],
          [1,1,1],
          [1,1,1]
        ])
      else:
        nb = np.array([1,1,1])
    else:
      raise exceptions.MissingDimensionError(
        f"Delineate is only supported for arrays with dimension '{TIME}' "
        f"and/or '{SPACE}', not: {list(dims)}"
      )
    out = xr.apply_ufunc(lambda x, y: ndimage.label(x, y)[0], obj, nb)
    if is_spatial:
      out = out.sq.stack_spatial_dims()
    out = out.where(pd.notnull(self._obj)) # Preserve nan.
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
    if SPACE not in self._obj.dims:
      return self._obj
    # Extract spatial coordinates.
    xcoords = self._obj[SPACE][X]
    ycoords = self._obj[SPACE][Y]
    # Update spatial coordinates.
    res = self.spatial_resolution
    xcoords = np.arange(xcoords[0], xcoords[-1] + res[1], res[1])
    ycoords = np.arange(ycoords[0], ycoords[-1] + res[0], res[0])
    # Reindex array.
    out = self._obj.unstack(SPACE).reindex({Y: ycoords, X: xcoords})
    out = out.sq.stack_spatial_dims()
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
    if SPACE not in self._obj.dims:
      return self._obj
    # To make rioxarray work:
    # Array needs to have unstacked spatial dimensions.
    # Array can have only the spatial_ref non-dimension coordinate.
    obj = self._obj.unstack(SPACE)
    obj = obj.sq.drop_non_dimension_coords(keep = ["spatial_ref"])
    # Reproject.
    out = obj.rio.reproject(crs, **kwargs)
    # Stack spatial dimensions back together.
    out = out.sq.stack_spatial_dims()
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
      zone = tz.zone
    except AttributeError:
      zone = pytz.timezone(tz).zone
    obj["temporal_ref"] = 0
    obj["temporal_ref"].attrs["zone"] = zone
    return obj

  def stack_spatial_dims(self):
    """Stack spatial X and Y dimensions into a single spatial dimension.

    Returns
    --------
      :obj:`xarray.DataArray`
        The input array with a multi-index spatial dimension. If no X and Y
        dimensions could be found, the input array is returned without
        modifications.

    """
    out = self._obj.stack({SPACE: [Y, X]})
    out[SPACE].sq.value_type = "coords"
    return out

  def unstack_spatial_dims(self):
    """Unstack the spatial dimension into separate X and Y dimensions.

    Returns
    --------
      :obj:`xarray.DataArray`
        The input array with unstacked spatial dimensions. If no spatial
        dimension could be found, the input array is returned without
        modifications.

    """
    if SPACE not in self._obj.dims:
      return self._obj
    return self._obj.unstack(SPACE)

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
    obj = self.drop_non_dimension_coords().sq.unstack_spatial_dims()
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
    obj = self.unstack_spatial_dims()
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
    # Write data to file.
    try:
      obj.rio.to_raster(file, **config)
    except rioxarray.exceptions.TooManyDimensions:
      ndims = len(obj.dims) - 2
      raise exceptions.TooManyDimensionsError(
        f"GeoTIFF export is only supported for 2D or 3D arrays, not {ndims}D"
      )
    return file

class Collection(list):
  """Internal representation of a collection of multiple arrays.

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
        "numerical", "nominal", "ordinal" and "boolean".
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
        coords = raw.get_index(dimension)
        clean = raw.isel({dimension: np.invert(coords.duplicated())})
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

  def shift(self, dimension, steps, coord = None, **kwargs):
    """Apply the shift verb to all arrays in the collection.

    See :meth:`Array.shift`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([dimension, steps, coord])
    out = copy.deepcopy(self)
    out[:] = [x.sq.shift(*args, **kwargs) for x in out]
    return out

  def smooth(self, reducer, dimension, size, coord = None, track_types = True,
             **kwargs):
    """Apply the smooth verb to all arrays in the collection.

    See :meth:`Array.smooth`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([reducer, dimension, size, coord, track_types])
    out = copy.deepcopy(self)
    out[:] = [x.sq.smooth(*args, **kwargs) for x in out]
    return out

  def trim(self, dimension = None, force_regular = True, **kwargs):
    """Apply the trim verb to all arrays in the collection.

    See :meth:`Array.trim`

    Returns
    -------
      :obj:`Collection`

    """
    args = tuple([dimension, force_regular])
    out = copy.deepcopy(self)
    out[:] = [x.sq.trim(*args, **kwargs) for x in out]
    return out

  def delineate(self, **kwargs):
    """Apply the delineate verb to all arrays in the collection.

    See :meth:`Array.delineate`

    Returns
    -------
      :obj:`Collection`

    """
    out = copy.deepcopy(self)
    out[:] = [x.sq.delineate(**kwargs) for x in out]
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