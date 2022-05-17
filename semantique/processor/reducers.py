import numpy as np
from scipy import stats

from semantique.processor.types import TypePromoter

def _nodata(x):
  return np.datetime64("NaT") if x.dtype.kind == "M" else np.nan

def _is_all_nodata(x, axis):
  return np.equal(np.sum(np.isfinite(x), axis = axis), 0)

def _nodata_as_zero(x):
  return np.nan_to_num(x, nan = 0, posinf = 0, neginf = 0)

#
# STATISTICAL REDUCERS
#

def mean_(x, dimension, track_types = True, **kwargs):
  """Calculate the mean of a set of values.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  Note
  -----
    When tracking value types, this reducer uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["mean"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "mean")
    promoter.check()
  def f(x, axis, **kwargs):
    return np.nanmean(x, axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def median_(x, dimension, track_types = True, **kwargs):
  """Return the median value in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  Note
  -----
    When tracking value types, this reducer uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["median"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "median")
    promoter.check()
  def f(x, axis, **kwargs):
    return np.nanmedian(x, axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def mode_(x, dimension, track_types = True, **kwargs):
  """Return the most occuring value in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  Note
  -------
    If there are multiple modal values in a set, the minimum of them (i.e. the
    smallest of these values) is returned.

  Note
  -----
    When tracking value types, this reducer uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["mode"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "mode")
    promoter.check()
  def f(x, axis, **kwargs):
    values = stats.mode(x, axis = axis, nan_policy = "omit")[0].squeeze(axis = axis)
    return np.where(_is_all_nodata(x, axis), _nodata(x), values)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def max_(x, dimension, track_types = True, **kwargs):
  """Return the maximum value in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  Note
  -----
    When tracking value types, this reducer uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["max"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "max")
    promoter.check()
  def f(x, axis, **kwargs):
    return np.nanmax(x, axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def min_(x, dimension, track_types = True, **kwargs):
  """Return the minimum value in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  Note
  -----
    When tracking value types, this reducer uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["min"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "min")
    promoter.check()
  def f(x, axis, **kwargs):
    return np.nanmin(x, axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def product_(x, dimension, track_types = True, **kwargs):
  """Calculate the product of a set of values.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  Note
  -----
    When tracking value types, this reducer uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["product"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "product")
    promoter.check()
  def f(x, axis, **kwargs):
    values = np.nanprod(x, axis = axis)
    return np.where(_is_all_nodata(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def standard_deviation_(x, dimension, track_types = True, **kwargs):
  """Calculate the standard deviation of a set of values.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  Note
  -----
    When tracking value types, this reducer uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["standard_deviation"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "standard_deviation")
    promoter.check()
  def f(x, axis, **kwargs):
    return np.nanstd(x, axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def sum_(x, dimension, track_types = True, **kwargs):
  """Calculate the sum of a set of values.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  Note
  -----
    When tracking value types, this reducer uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["sum"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "sum")
    promoter.check()
  def f(x, axis, **kwargs):
    values = np.nansum(x, axis = axis)
    return np.where(_is_all_nodata(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def variance_(x, dimension, track_types = True, **kwargs):
  """Calculate the variance of a set of values.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  Note
  -----
    When tracking value types, this reducer uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["variance"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "variance")
    promoter.check()
  def f(x, axis, **kwargs):
    return np.nanvar(x, axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

#
# BOOLEAN REDUCERS
#

def all_(x, dimension, track_types = True, **kwargs):
  """Test if all values in a set are true.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  Note
  -----
    When tracking value types, this reducer uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["all"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "all")
    promoter.check()
  def f(x, axis, **kwargs):
    values = np.all(x, axis = axis)
    return np.where(_is_all_nodata(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def any_(x, dimension, track_types = True, **kwargs):
  """Test if at least one value in a set is true.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  Note
  -----
    When tracking value types, this reducer uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["any"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "any")
    promoter.check()
  def f(x, axis, **kwargs):
    values = np.any(_nodata_as_zero(x), axis = axis)
    return np.where(_is_all_nodata(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

#
# OCCURENCE REDUCERS
#

def count_(x, dimension, track_types = True, **kwargs):
  """Count the number of true values in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  Note
  -----
    When tracking value types, this reducer uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["count"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "count")
    promoter.check()
  def f(x, axis, **kwargs):
    values = np.count_nonzero(_nodata_as_zero(x), axis = axis)
    return np.where(_is_all_nodata(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def percentage_(x, dimension, track_types = True, **kwargs):
  """Calculate the percentage of true values in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  Note
  -----
    When tracking value types, this reducer uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["percentage"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "percentage")
    promoter.check()
  def f(x, axis, **kwargs):
    part = np.count_nonzero(_nodata_as_zero(x), axis = axis)
    part = np.where(_is_all_nodata(x, axis), np.nan, part)
    whole = np.sum(np.isfinite(x), axis = axis)
    return np.multiply(np.divide(part, whole), 100)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

#
# POSITIONAL REDUCERS
#

def first_(x, dimension, track_types = True, **kwargs):
  """Return the first value in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  Note
  -----
    When tracking value types, this reducer uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["first"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "first")
    promoter.check()
  def f(x, axis, **kwargs):
    is_value = np.isfinite(x)
    is_first = np.equal(np.cumsum(np.cumsum(is_value, axis = axis), axis = axis), 1)
    return np.nanmax(np.where(is_first, x, _nodata(x)), axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def last_(x, dimension, track_types = True, **kwargs):
  """Return the last value in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The data cube to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reduction function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced data cube.

  Note
  -----
    When tracking value types, this reducer uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["last"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "last")
    promoter.check()
  def f(x, axis, **kwargs):
    xflipped = np.flip(x, axis = axis)
    is_value = np.isfinite(xflipped)
    is_first = np.equal(np.cumsum(np.cumsum(is_value, axis = axis), axis = axis), 1)
    return np.nanmax(np.where(is_first, xflipped, _nodata(x)), axis = axis)
  out = x.reduce(f, dim = dimension, **kwargs)
  if track_types:
    out = promoter.promote(out)
  return out