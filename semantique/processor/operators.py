import pandas as pd
import numpy as np
import xarray as xr

from semantique.processor import utils
from semantique.processor.types import TypePromoter
from semantique.processor.values import Interval
from semantique.dimensions import SPACE

def get_accessor(data, meta = False):
    """Get the appropriate accessor for the data object.

    Parameters
    ----------
      data : :obj:`xarray.DataArray`
        The data object to get the accessor for.
      meta : :obj:`bool`
        Should the meta accessor be used? If False, the standard accessor is
        used. The meta accessor is used to access MetaArray and MetaCollection
        instead of Array and Collection objects.
    """
    if meta:
        return data.sqm
    else:
        return data.sq

#
# UNIVARIATE OPERATORS
#

def not_(x, track_types = True, **kwargs):
  """Test if x is not true.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["not"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "not")
    promoter.check()
  f = lambda x: np.where(pd.notnull(x), np.logical_not(x), np.nan)
  out = xr.apply_ufunc(f, x, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def is_missing_(x, track_types = True, **kwargs):
  """Test if x is a missing observation.

  Missing values can occur because of several reasons:

  * The observed value is removed by the filter verb.

  * The observed value is erroneous.

  * The area of interest spans multiple orbits. In practice this means that
    the timestamps at which observations where made differ within the area of
    interest. The time dimensions contains all these timestamps as coordinates.
    For a single location in space, the time coordinates at which no
    observation was made are filled with null values (i.e. the observation is
    marked as "missing").

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["is_missing"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "is_missing")
    promoter.check()
  f = lambda x: pd.isnull(x)
  out = xr.apply_ufunc(f, x, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def not_missing_(x, track_types = True, **kwargs):
  """Test if x is a valid observation.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["not_missing"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "not_missing")
    promoter.check()
  f = lambda x: pd.notnull(x)
  out = xr.apply_ufunc(f, x, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def absolute_(x, track_types = True, **kwargs):
  """Compute the absolute value of x.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["absolute"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "absolute")
    promoter.check()
  f = lambda x: np.absolute(x)
  out = xr.apply_ufunc(f, x, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def ceiling_(x, track_types = True, **kwargs):
  """Compute the ceiling of x.

  The ceiling is the smallest integer *i* such that *i >= x*.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["ceiling"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "ceiling")
    promoter.check()
  f = lambda x: np.ceil(x)
  out = xr.apply_ufunc(f, x, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def cosine_(x, track_types = True, **kwargs):
  """Compute the cosine of x.

  This operator assumes the values of x are angles in radians.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["cosine"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "cosine")
    promoter.check()
  f = lambda x: np.cos(x)
  out = xr.apply_ufunc(f, x, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def cosecant_(x, track_types = True, **kwargs):
  """Compute the cosecant of x.

  This operator assumes the values of x are angles in radians.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["cosecant"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "cosecant")
    promoter.check()
  def f(x):
    sin = np.sin(x)
    sin_nozero = np.where(np.equal(sin, 0), np.nan, sin)
    return np.divide(1, sin_nozero)
  out = xr.apply_ufunc(f, x, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def cotangent_(x, track_types = True, **kwargs):
  """Compute the cotangent of x.

  This operator assumes the values of x are angles in radians.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["cotangent"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "cotangent")
    promoter.check()
  def f(x):
    tan = np.tan(x)
    tan_nozero = np.where(np.equal(tan, 0), np.nan, tan)
    return np.divide(1, tan_nozero)
  out = xr.apply_ufunc(f, x, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def cube_root_(x, track_types = True, **kwargs):
  """Compute the cube root of x.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["cube_root"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "cube_root")
    promoter.check()
  f = lambda x: np.cbrt(x)
  out = xr.apply_ufunc(f, x, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def exponential_(x, track_types = True, **kwargs):
  """Compute the exponential function of x.

  The exponential function of x is defined as e to the power x, in which e is
  Eulers number (approximately equal to 2.718). It is the inverse function of
  :func:`natural_logarithm_`.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["exponential"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "exponential")
    promoter.check()
  f = lambda x: np.exp(x)
  out = xr.apply_ufunc(f, x, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def floor_(x, track_types = True, **kwargs):
  """Compute the floor of x.

  The floor is the largest integer *i* such that *i <= x*.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["floor"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "floor")
    promoter.check()
  f = lambda x: np.floor(x)
  out = xr.apply_ufunc(f, x, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def natural_logarithm_(x, track_types = True, **kwargs):
  """Compute the natural logarithm of x.

  The natural logarithm of x is the logarithm with base e, in which e is
  Eulers number (approximately equal to 2.718). It is the inverse function of
  :func:`exponential_`.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["natural_logarithm"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "natural_logarithm")
    promoter.check()
  f = lambda x: np.where(np.equal(x, 0), np.nan, np.log(x))
  out = xr.apply_ufunc(f, x, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def secant_(x, track_types = True, **kwargs):
  """Compute the secant of x.

  This operator assumes the values of x are angles in radians.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["secant"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "secant")
    promoter.check()
  def f(x):
    cos = np.cos(x)
    cos_nozero = np.where(np.equal(cos, 0), np.nan, cos)
    return np.divide(1, cos_nozero)
  out = xr.apply_ufunc(f, x, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def sine_(x, track_types = True, **kwargs):
  """Compute the sine of x.

  This operator assumes the values of x are angles in radians.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["sine"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "sine")
    promoter.check()
  f = lambda x: np.sin(x)
  out = xr.apply_ufunc(f, x, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def square_root_(x, track_types = True, **kwargs):
  """Compute the square root of x.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["square_root"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "square_root")
    promoter.check()
  f = lambda x: np.sqrt(x)
  out = xr.apply_ufunc(f, x, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def tangent_(x, track_types = True, **kwargs):
  """Compute the tangent of x.

  This operator assumes the values of x are angles in radians.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["tangent"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "tangent")
    promoter.check()
  f = lambda x: np.tan(x)
  out = xr.apply_ufunc(f, x, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def to_degrees_(x, track_types = True, **kwargs):
  """Converts angles in radians to angles in degrees.

  This operator assumes the values of x are angles in radians.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["to_degrees"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "to_degrees")
    promoter.check()
  f = lambda x: np.rad2deg(x)
  out = xr.apply_ufunc(f, x, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def to_radians_(x, track_types = True, **kwargs):
  """Converts angles in degrees to angles in radians.

  This operator assumes the values of x are angles in degrees.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["to_radians"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "to_radians")
    promoter.check()
  f = lambda x: np.deg2rad(x)
  out = xr.apply_ufunc(f, x, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

#
# ALGEBRAIC OPERATORS
#

def add_(x, y, track_types = True, meta = False, **kwargs):
  """Add y to x.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another array which can be aligned to the same shape as array ``x``.
      In the latter case, when evaluating the expression for a pixel in array
      ``x`` the second operand is the value of the pixel in array ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    meta : :obj:`bool`
      Should the MetaArray accessor be used?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["add"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "add")
    promoter.check()
  f = lambda x, y: np.add(x, y)
  y = get_accessor(xr.DataArray(y), meta).align_with(x)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def divide_(x, y, track_types = True, meta = False, **kwargs):
  """Divide x by y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another array which can be aligned to the same shape as array ``x``.
      In the latter case, when evaluating the expression for a pixel in array
      ``x`` the second operand is the value of the pixel in array ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    meta : :obj:`bool`
      Should the meta accessor be used?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["divide"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "divide")
    promoter.check()
  f = lambda x, y: np.divide(x, np.where(np.equal(y, 0), np.nan, y))
  y = get_accessor(xr.DataArray(y), meta).align_with(x)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def multiply_(x, y, track_types = True, meta = False, **kwargs):
  """Multiply x by y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another array which can be aligned to the same shape as array ``x``.
      In the latter case, when evaluating the expression for a pixel in array
      ``x`` the second operand is the value of the pixel in array ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    meta : :obj:`bool`
      Should the meta accessor be used?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["multiply"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "multiply")
    promoter.check()
  f = lambda x, y: np.multiply(x, y)
  y = get_accessor(xr.DataArray(y), meta).align_with(x)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def power_(x, y, track_types = True, meta = False, **kwargs):
  """Raise x to the yth power.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another array which can be aligned to the same shape as array ``x``.
      In the latter case, when evaluating the expression for a pixel in array
      ``x`` the second operand is the value of the pixel in array ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    meta : :obj:`bool`
      Should the meta accessor be used?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["power"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "power")
    promoter.check()
  f = lambda x, y: np.power(x, y)
  y = get_accessor(xr.DataArray(y), meta).align_with(x)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def subtract_(x, y, track_types = True, meta = False, **kwargs):
  """Subtract y from x.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another array which can be aligned to the same shape as array ``x``.
      In the latter case, when evaluating the expression for a pixel in array
      ``x`` the second operand is the value of the pixel in array ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    meta : :obj:`bool`
      Should the meta accessor be used?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["subtract"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "subtract")
    promoter.check()
  f = lambda x, y: np.subtract(x, y)
  y = get_accessor(xr.DataArray(y), meta).align_with(x)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def normalized_difference_(x, y, track_types = True, meta = False, **kwargs):
  """Compute the normalized difference between x and y.

  The normalized difference is used to calculate common indices in remote
  sensing, such as the normalized difference vegetation index (NDVI) or the
  normalized difference water index (NDWI). It is defined as (x - y) / (x + y).

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another array which can be aligned to the same shape as array ``x``.
      In the latter case, when evaluating the expression for a pixel in array
      ``x`` the second operand is the value of the pixel in array ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    meta : :obj:`bool`
      Should the meta accessor be used?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["normalized_difference"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "normalized_difference")
    promoter.check()
  f = lambda x, y: np.divide(np.subtract(x, y), np.add(x, y))
  y = get_accessor(xr.DataArray(y), meta).align_with(x)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

#
# BOOLEAN OPERATORS
#

def and_(x, y, track_types = True, meta = False, **kwargs):
  """Test if both x and y are true.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another array which can be aligned to the same shape as array ``x``.
      In the latter case, when evaluating the expression for a pixel in array
      ``x`` the second operand is the value of the pixel in array ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    meta : :obj:`bool`
      Should the meta accessor be used?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["and"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "and")
    promoter.check()
  def f(x, y):
    y = utils.null_as_zero(y)
    return np.where(pd.notnull(x), np.logical_and(x, y), np.nan)
  y = get_accessor(xr.DataArray(y), meta).align_with(x)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def or_(x, y, track_types = True, meta = False, **kwargs):
  """Test if at least one of x and y are true.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another array which can be aligned to the same shape as array ``x``.
      In the latter case, when evaluating the expression for a pixel in array
      ``x`` the second operand is the value of the pixel in array ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    meta : :obj:`bool`
      Should the meta accessor be used?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    Missing values in ``x`` are always preserved. That means that each
    expression in which x is null will be evaluated as null, no matter if y
    is true. However, when x is true and y is null, the expression will be
    evaluated as true.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["or"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "or")
    promoter.check()
  def f(x, y):
    y = utils.null_as_zero(y)
    return np.where(pd.notnull(x), np.logical_or(x, y), np.nan)
  y = get_accessor(xr.DataArray(y), meta).align_with(x)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def exclusive_or_(x, y, track_types = True, meta = False, **kwargs):
  """Test if either x or y is true but not both.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another array which can be aligned to the same shape as array ``x``.
      In the latter case, when evaluating the expression for a pixel in array
      ``x`` the second operand is the value of the pixel in array ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    meta : :obj:`bool`
      Should the meta accessor be used?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    Missing values in ``x`` are always preserved. That means that each
    expression in which x is null will be evaluated as null, no matter if y
    is true. However, when x is true and y is null, the expression will be
    evaluated as true.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["exclusive_or"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "exclusive_or")
    promoter.check()
  def f(x, y):
    y = utils.null_as_zero(y)
    return np.where(pd.notnull(x), np.logical_xor(x, y), np.nan)
  y = get_accessor(xr.DataArray(y), meta).align_with(x)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

#
# EQUALITY OPERATORS
#

def equal_(x, y, track_types = True, meta = False, **kwargs):
  """Test if x is equal to y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another array which can be aligned to the same shape as array ``x``.
      In the latter case, when evaluating the expression for a pixel in array
      ``x`` the second operand is the value of the pixel in array ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    meta : :obj:`bool`
      Should the meta accessor be used?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["equal"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "equal")
    promoter.check()
  f = lambda x, y: np.where(pd.notnull(x), np.equal(x, y), np.nan)
  y = get_accessor(xr.DataArray(y), meta).align_with(x)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def in_(x, y, track_types = True, **kwargs):
  """Test if x is a member of set y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression.
    y : :obj:`list`
      Operands at the right-hand side of each expression. Should be a set of
      values, which remains constant among all evaluated expressions.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["in"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "in")
    promoter.check()
  def f(x, y):
    if isinstance(y, Interval):
      a = np.greater_equal(x, y.lower)
      b = np.less_equal(x, y.upper)
      return np.where(pd.notnull(x), np.logical_and(a, b), np.nan)
    else:
      return np.where(pd.notnull(x), np.isin(x, y), np.nan)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def not_equal_(x, y, track_types = True, meta = False, **kwargs):
  """Test if x is not equal to y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another array which can be aligned to the same shape as array ``x``.
      In the latter case, when evaluating the expression for a pixel in array
      ``x`` the second operand is the value of the pixel in array ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    meta : :obj:`bool`
      Should the meta accessor be used?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["not_equal"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "not_equal")
    promoter.check()
  f = lambda x, y: np.where(pd.notnull(x), np.not_equal(x, y), np.nan)
  y = get_accessor(xr.DataArray(y), meta).align_with(x)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def not_in_(x, y, track_types = True, **kwargs):
  """Test if x is not a member of set y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression.
    y : :obj:`list`
      Operands at the right-hand side of each expression. Should be a set of
      values, which remains constant among all evaluated expressions.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["not_in"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "not_in")
    promoter.check()
  def f(x, y):
    if isinstance(y, Interval):
      a = np.less(x, y.lower)
      b = np.greater(x, y.upper)
      return np.where(pd.notnull(x), np.logical_or(a, b), np.nan)
    else:
      return np.where(pd.notnull(x), np.isin(x, y, invert = True), np.nan)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

#
# REGULAR RELATIONAL OPERATORS
#

def greater_(x, y, track_types = True, meta = False, **kwargs):
  """Test if x is greater than y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another array which can be aligned to the same shape as array ``x``.
      In the latter case, when evaluating the expression for a pixel in array
      ``x`` the second operand is the value of the pixel in array ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    meta : :obj:`bool`
      Should the meta accessor be used?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["greater"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "greater")
    promoter.check()
  f = lambda x, y: np.where(pd.notnull(x), np.greater(x, y), np.nan)
  y = get_accessor(xr.DataArray(y), meta).align_with(x)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def greater_equal_(x, y, track_types = True, meta = False, **kwargs):
  """Test if x is greater than or equal to y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another array which can be aligned to the same shape as array ``x``.
      In the latter case, when evaluating the expression for a pixel in array
      ``x`` the second operand is the value of the pixel in array ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    meta : :obj:`bool`
      Should the meta accessor be used?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["greater_equal"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "greater_equal")
    promoter.check()
  f = lambda x, y: np.where(pd.notnull(x), np.greater_equal(x, y), np.nan)
  y = get_accessor(xr.DataArray(y), meta).align_with(x)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def less_(x, y, track_types = True, meta = False, **kwargs):
  """Test if x is less than y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another array which can be aligned to the same shape as array ``x``.
      In the latter case, when evaluating the expression for a pixel in array
      ``x`` the second operand is the value of the pixel in array ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    meta : :obj:`bool`
      Should the meta accessor be used?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["less"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "less")
    promoter.check()
  f = lambda x, y: np.where(pd.notnull(x), np.less(x, y), np.nan)
  y = get_accessor(xr.DataArray(y), meta).align_with(x)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def less_equal_(x, y, track_types = True, meta = False, **kwargs):
  """Test if x is less than or equal to y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another array which can be aligned to the same shape as array ``x``.
      In the latter case, when evaluating the expression for a pixel in array
      ``x`` the second operand is the value of the pixel in array ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    meta : :obj:`bool`
      Should the meta accessor be used?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["less_equal"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "less_equal")
    promoter.check()
  f = lambda x, y: np.where(pd.notnull(x), np.less_equal(x, y), np.nan)
  y = get_accessor(xr.DataArray(y), meta).align_with(x)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

#
# SPATIAL RELATIONAL OPERATORS
#

def intersects_(x, y, track_types = True, meta = False, **kwargs):
  """Test if x spatially intersects with y.

  This is a specific spatial relational operator meant to be evaluated with
  a spatial coordinate tuple as left-hand side operand, and
  :func:`semantique.geometries` as right-hand side operand. It will
  evaluate if the spatial point with the specified coordinates spatially
  intersects with any of the given geometries.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression, which should be spatial coordinate tuples.
    y :
      Spatial geometries to be used as the right-hand side of each expression.
      May also be another array with spatial coordinate tuples. In the
      latter case, when evaluating the expression for a coordinate tuple in
      array ``x`` the second operand is the spatial bounding box of all
      coordinate tuples in array ``y``.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    meta : :obj:`bool`
      Should the meta accessor be used?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
  The spatial coordinate reference systems of x and y are expected to be equal.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["intersects"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "intersects")
    promoter.check()
  try:
    y = y.unary_union
  except AttributeError:
    y = get_accessor(get_accessor(y, meta).trim(), meta).grid_points.envelope.unary_union
  values = get_accessor(x, meta).grid_points.intersects(y).astype(int)
  coords = get_accessor(x, meta).stack_spatial_dims()[SPACE].coords
  out = get_accessor(xr.DataArray(values, coords = coords), meta).unstack_spatial_dims()
  if track_types:
    out = promoter.promote(out)
  return out

#
# TEMPORAL RELATIONAL OPERATORS
#

def after_(x, y, track_types = True, **kwargs):
  """Test if x comes after y.

  This is a specific temporal relational operator meant to be evaluated with
  a temporal coordinate as left-hand side operand, and a
  :func:`semantique.time_instant` and/or :func:`semantique.time_interval` as
  right-hand side operand. It will evaluate if the specified temporal
  coordinate is later in time than the given time instant or the end of the
  given time interval.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression, which should be temporal coordinates.
    y :
      Time instant or time interval to be used as the right-hand side of each
      expression. May also be another array with temporal coordinates. In
      the latter case, when evaluating the expression for a coordinate in array
      ``x`` the second operand is the temporal bounding box of all coordinates
      in array ``y``.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
  The timezones of x and y are expected to be equal.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["after"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "after")
    promoter.check()
  f = lambda x, y: np.where(pd.notnull(x), np.greater(x, np.nanmax(y)), np.nan)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def before_(x, y, track_types = True, **kwargs):
  """Test if x comes before y.

  This is a specific temporal relational operator meant to be evaluated with
  a temporal coordinate as left-hand side operand, and a
  :func:`semantique.time_instant` and/or :func:`semantique.time_interval` as
  right-hand side operand. It will evaluate if the specified temporal
  coordinate is earlier in time than the given time instant or the start of
  the given time interval.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression, which should be temporal coordinates.
    y :
      Time instant or time interval to be used as the right-hand side of each
      expression. May also be another array with temporal coordinates. In
      the latter case, when evaluating the expression for a coordinate in array
      ``x`` the second operand is the temporal bounding box of all coordinates
      in array ``y``.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
  The timezones of x and y are expected to be equal.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["before"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "before")
    promoter.check()
  f = lambda x, y: np.where(pd.notnull(x), np.less(x, np.nanmin(y)), np.nan)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def during_(x, y, track_types = True, **kwargs):
  """Test if x is during interval y.

  This is a specific temporal relational operator meant to be evaluated with
  a temporal coordinate as left-hand side operand, and a
  :func:`semantique.time_interval` as right-hand side operand. It will evaluate
  if the specified temporal coordinate fall inside the given time interval.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression, which should be temporal coordinates.
    y :
      Time interval to be used as the right-hand side of each expression. May
      also be another array with temporal coordinates. In the latter case,
      when evaluating the expression for a coordinate in array ``x`` the second
      operand is the temporal bounding box of all coordinates in array ``y``.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
  The timezones of x and y are expected to be equal.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["during"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "during")
    promoter.check()
  def f(x, y):
    a = np.greater_equal(x, np.nanmin(y))
    b = np.less_equal(x, np.nanmax(y))
    return np.where(pd.notnull(x), np.logical_and(a, b), np.nan)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

#
# ASSIGNMENT OPERATORS
#

# Note: These are used by the assign verb.

def assign_(x, y, track_types = True, meta = False, **kwargs):
  """Replace x by y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another array which can be aligned to the same shape as array ``x``.
      In the latter case, when evaluating the expression for a pixel in array
      ``x`` the second operand is the value of the pixel in array ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    meta : :obj:`bool`
      Should the meta accessor be used?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["assign"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "assign")
    promoter.check()
  f = lambda x, y: np.where(pd.notnull(x), y, utils.get_null(y))
  y = get_accessor(xr.DataArray(y), meta).align_with(x)
  out = xr.apply_ufunc(f, x, y, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out

def assign_at_(x, y, z, track_types = True, meta = False, **kwargs):
  """Replace x by y where z is true.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Array containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another array which can be aligned to the same shape as array ``x``.
      In the latter case, when evaluating the expression for a pixel in array
      ``x`` the second operand is the value of the pixel in array ``y`` that
      has the same dimension coordinates.
    z : :obj:`xarray.DataArray`
      Binary array which can be aligned to the same shape as array ``x``,
      defining at which pixels in x the expression should be evaluated. All
      other pixels remain unchanged.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    meta : :obj:`bool`
      Should the meta accessor be used?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      An array with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the first layer of keys being the supported value types of
    ``x``, the second layer of keys being the supported value types of ``y``
    given the value type of ``x``, and the corresponding value being the
    promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["assign_at"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "assign_at")
    promoter.check()
  f = lambda x, y, z: np.where(np.logical_and(pd.notnull(z), z), y, x)
  y = get_accessor(xr.DataArray(y), meta).align_with(x)
  z = get_accessor(z, meta).align_with(x)
  out = xr.apply_ufunc(f, x, y, z, keep_attrs = True)
  if track_types:
    out = promoter.promote(out)
  return out
