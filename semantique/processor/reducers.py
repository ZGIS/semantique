import pandas as pd
import numpy as np

from scipy import stats

from semantique.processor.types import TypePromoter
from semantique.processor.utils import get_null, allnull, null_as_zero

#
# STATISTICAL REDUCERS
#

def mean_(x, dimension, track_types = True, **kwargs):
  """Calculate the mean of a set of values.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The array to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reducer function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced array.

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
  f = lambda x, axis: np.nanmean(x, axis = axis)
  out = x.reduce(f, dim = dimension)
  if track_types:
    out = promoter.promote(out)
  return out

def median_(x, dimension, track_types = True, **kwargs):
  """Return the median value in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The array to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reducer function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced array.

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
  f = lambda x, axis: np.nanmedian(x, axis = axis)
  out = x.reduce(f, dim = dimension)
  if track_types:
    out = promoter.promote(out)
  return out

def mode_(x, dimension, track_types = True, **kwargs):
  """Return the most occuring value in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The array to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reducer function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced array.

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
  def f(x, axis):
    values = stats.mode(x, axis = axis, nan_policy = "omit")[0].squeeze(axis = axis)
    return np.where(allnull(x, axis), get_null(x), values)
  out = x.reduce(f, dim = dimension)
  if track_types:
    out = promoter.promote(out)
  return out

def max_(x, dimension, track_types = True, **kwargs):
  """Return the maximum value in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The array to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reducer function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced array.

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
  f = lambda x, axis: np.nanmax(x, axis)
  out = x.reduce(f, dim = dimension)
  if track_types:
    out = promoter.promote(out)
  return out

def min_(x, dimension, track_types = True, **kwargs):
  """Return the minimum value in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The array to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reducer function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced array.

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
  f = lambda x, axis: np.nanmin(x, axis)
  out = x.reduce(f, dim = dimension)
  if track_types:
    out = promoter.promote(out)
  return out

def range_(x, dimension, track_types = True, **kwargs):
  """Return the difference between the maximum and minimum values in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The array to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reducer function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced array.

  Note
  -----
    When tracking value types, this reducer uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["range"]
      obj.pop("__preserve_labels__")
      print(obj)

  Note
  -----
    When applied to an input containing timestamps as values, the output array
    will contain numerical values representing the difference between the most
    recent and least recent timestamp in nanoseconds.

  """
  if track_types:
    promoter = TypePromoter(x, function = "range")
    promoter.check()
  f = lambda x, axis: np.subtract(np.nanmax(x, axis), np.nanmin(x, axis))
  out = x.reduce(f, dim = dimension)
  if track_types:
    out = promoter.promote(out)
  return out

def n_(x, dimension, track_types = True, **kwargs):
  """Return the number of observations in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The array to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reducer function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced array.

  Note
  -----
    Missing values in ``x`` are ignored. That means that this reducer will
    return the number of non-null values in the set. If all values in the set
    are null, it returns 0.

  Note
  -----
    When tracking value types, this reducer uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["n"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "n")
    promoter.check()
  f = lambda x, axis: np.nansum(pd.notnull(x), axis)
  out = x.reduce(f, dim = dimension)
  if track_types:
    out = promoter.promote(out)
  return out

def product_(x, dimension, track_types = True, **kwargs):
  """Calculate the product of a set of values.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The array to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reducer function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced array.

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
  f = lambda x, axis: np.where(allnull(x, axis), np.nan, np.nanprod(x, axis))
  out = x.reduce(f, dim = dimension)
  if track_types:
    out = promoter.promote(out)
  return out

def standard_deviation_(x, dimension, track_types = True, **kwargs):
  """Calculate the standard deviation of a set of values.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The array to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reducer function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced array.

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
  f = lambda x, axis: np.nanstd(x, axis = axis)
  out = x.reduce(f, dim = dimension)
  if track_types:
    out = promoter.promote(out)
  return out

def sum_(x, dimension, track_types = True, **kwargs):
  """Calculate the sum of a set of values.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The array to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reducer function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced array.

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
  f = lambda x, axis: np.where(allnull(x, axis), np.nan, np.nansum(x, axis))
  out = x.reduce(f, dim = dimension)
  if track_types:
    out = promoter.promote(out)
  return out

def variance_(x, dimension, track_types = True, **kwargs):
  """Calculate the variance of a set of values.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The array to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reducer function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced array.

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
  f = lambda x, axis: np.nanvar(x, axis = axis)
  out = x.reduce(f, dim = dimension)
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
      The array to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reducer function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced array.

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
  def f(x, y):
    values = np.all(x, axis)
    return np.where(allnull(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension)
  if track_types:
    out = promoter.promote(out)
  return out

def any_(x, dimension, track_types = True, **kwargs):
  """Test if at least one value in a set is true.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The array to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reducer function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced array.

  Note
  -----
    Missing values in ``x`` are ignored. That means that this reducer will
    return null rather than false if all values in the set are null.

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
  def f(x, axis):
    values = np.any(null_as_zero(x), axis)
    return np.where(allnull(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension)
  if track_types:
    out = promoter.promote(out)
  return out

def none_(x, dimension, track_types = True, **kwargs):
  """Test if none of the values in a set are true.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The array to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reducer function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced array.

  Note
  -----
    Missing values in ``x`` are ignored. That means that this reducer will
    return null rather than true if all values in the set are null.

  Note
  -----
    When tracking value types, this reducer uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["none"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "none")
    promoter.check()
  def f(x, axis):
    values = np.logical_not(np.any(null_as_zero(x), axis))
    return np.where(allnull(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension)
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
      The array to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reducer function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced array.

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
  def f(x, axis):
    values = np.count_nonzero(null_as_zero(x), axis)
    return np.where(allnull(x, axis), np.nan, values)
  out = x.reduce(f, dim = dimension)
  if track_types:
    out = promoter.promote(out)
  return out

def percentage_(x, dimension, track_types = True, **kwargs):
  """Calculate the percentage of true values in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The array to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reducer function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced array.

  Note
  -----
    Missing values in ``x`` are ignored. That means that this reducer will
    return how much percent of the non-null values are true. If all values in
    the set are null, it returns null.

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
  def f(x, axis):
    part = np.count_nonzero(null_as_zero(x), axis)
    part = np.where(allnull(x, axis), np.nan, part)
    whole = np.sum(pd.notnull(x), axis)
    return np.multiply(np.divide(part, whole), 100)
  out = x.reduce(f, dim = dimension)
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
      The array to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reducer function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced array.

  Note
  -----
    Missing values in ``x`` are ignored. That means that this reducer will
    return the first non-null value in the set. If all values in the set are
    null, it returns null.

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
  def f(x, axis):
    is_value = pd.notnull(x)
    is_first = np.equal(np.cumsum(np.cumsum(is_value, axis), axis), 1)
    return np.nanmax(np.where(is_first, x, get_null(x)), axis)
  out = x.reduce(f, dim = dimension)
  if track_types:
    out = promoter.promote(out)
  return out

def last_(x, dimension, track_types = True, **kwargs):
  """Return the last value in a set.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      The array to be reduced.
    dimension : :obj:`str`
      Name of the dimension to apply the reducer function to.
    track_types : :obj:`bool`
      Should the reducer promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      The reduced array.

  Note
  -----
    Missing values in ``x`` are ignored. That means that this reducer will
    return the last non-null value in the set. If all values in the set are
    null, it returns null.

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
  def f(x, axis):
    xflipped = np.flip(x, axis)
    is_value = pd.notnull(xflipped)
    is_first = np.equal(np.cumsum(np.cumsum(is_value, axis), axis), 1)
    return np.nanmax(np.where(is_first, xflipped, get_null(x)), axis)
  out = x.reduce(f, dim = dimension)
  if track_types:
    out = promoter.promote(out)
  return out