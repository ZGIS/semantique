import numpy as np
import xarray as xr

from semantique.processor.types import TypePromoter

def _nodata(x):
  return np.datetime64("NaT") if x.dtype.kind == "M" else np.nan

#
# BOOLEAN UNIVARIATE OPERATORS
#

def invert_(x, track_types = True, **kwargs):
  """Compute the boolean inverse of x.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
      evaluated expressions.

  Note
  -----
    When tracking value types, this operator uses the following type promotion
    manual, with the keys being the supported value types of ``x``, and the
    corresponding value being the promoted value type of the output.

    .. exec_code::
      :hide_code:

      from semantique.processor.types import TYPE_PROMOTION_MANUALS
      obj = TYPE_PROMOTION_MANUALS["invert"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, function = "invert")
    promoter.check()
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.logical_not(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

#
# NUMERICAL UNIVARIATE OPERATORS
#

def absolute_(x, track_types = True, **kwargs):
  """Compute the absolute value of x.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.absolute(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def cube_root_(x, track_types = True, **kwargs):
  """Compute the cube root of x.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.cbrt(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
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
      Data cube containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.exp(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
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
      Data cube containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.log(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def square_root_(x, track_types = True, **kwargs):
  """Compute the square root of x.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the values to apply the operator to.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input object?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.sqrt(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

#
# ALGEBRAIC OPERATORS
#

def add_(x, y, track_types = True, **kwargs):
  """Add y to x.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another data cube which can be aligned to the same shape as cube ``x``.
      In the latter case, when evaluating the expression for a pixel in cube
      ``x`` the second operand is the value of the pixel in cube ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.add(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def divide_(x, y, track_types = True, **kwargs):
  """Divide x by y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another data cube which can be aligned to the same shape as cube ``x``.
      In the latter case, when evaluating the expression for a pixel in cube
      ``x`` the second operand is the value of the pixel in cube ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.divide(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def multiply_(x, y, track_types = True, **kwargs):
  """Multiply x by y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another data cube which can be aligned to the same shape as cube ``x``.
      In the latter case, when evaluating the expression for a pixel in cube
      ``x`` the second operand is the value of the pixel in cube ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.multiply(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def power_(x, y, track_types = True, **kwargs):
  """Raise x to the yth power.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another data cube which can be aligned to the same shape as cube ``x``.
      In the latter case, when evaluating the expression for a pixel in cube
      ``x`` the second operand is the value of the pixel in cube ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.power(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def subtract_(x, y, track_types = True, **kwargs):
  """Subtract y from x.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another data cube which can be aligned to the same shape as cube ``x``.
      In the latter case, when evaluating the expression for a pixel in cube
      ``x`` the second operand is the value of the pixel in cube ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.subtract(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

#
# BOOLEAN OPERATORS
#

def and_(x, y, track_types = True, **kwargs):
  """Test if both x and y are true.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another data cube which can be aligned to the same shape as cube ``x``.
      In the latter case, when evaluating the expression for a pixel in cube
      ``x`` the second operand is the value of the pixel in cube ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.logical_and(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def or_(x, y, track_types = True, **kwargs):
  """Test if either x or y are true.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another data cube which can be aligned to the same shape as cube ``x``.
      In the latter case, when evaluating the expression for a pixel in cube
      ``x`` the second operand is the value of the pixel in cube ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
      obj = TYPE_PROMOTION_MANUALS["or"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "or")
    promoter.check()
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.logical_or(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def exclusive_or_(x, y, track_types = True, **kwargs):
  """Test if only one of x and y is true.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another data cube which can be aligned to the same shape as cube ``x``.
      In the latter case, when evaluating the expression for a pixel in cube
      ``x`` the second operand is the value of the pixel in cube ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
      obj = TYPE_PROMOTION_MANUALS["exclusive_or"]
      obj.pop("__preserve_labels__")
      print(obj)

  """
  if track_types:
    promoter = TypePromoter(x, y, function = "exclusive_or")
    promoter.check()
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.logical_xor(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

#
# EQUALITY OPERATORS
#

def equal_(x, y, track_types = True, **kwargs):
  """Test if x is equal to y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another data cube which can be aligned to the same shape as cube ``x``.
      In the latter case, when evaluating the expression for a pixel in cube
      ``x`` the second operand is the value of the pixel in cube ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.equal(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def in_(x, y, track_types = True, **kwargs):
  """Test if x is a member of set y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the operands at the left-hand side of each
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
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.isin(x, y), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def not_equal_(x, y, track_types = True, **kwargs):
  """Test if x is not equal to y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another data cube which can be aligned to the same shape as cube ``x``.
      In the latter case, when evaluating the expression for a pixel in cube
      ``x`` the second operand is the value of the pixel in cube ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.not_equal(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def not_in_(x, y, track_types = True, **kwargs):
  """Test if x is not a member of set y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the operands at the left-hand side of each
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
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.isin(x, y, invert = True), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

#
# REGULAR RELATIONAL OPERATORS
#

def greater_(x, y, track_types = True, **kwargs):
  """Test if x is greater than y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another data cube which can be aligned to the same shape as cube ``x``.
      In the latter case, when evaluating the expression for a pixel in cube
      ``x`` the second operand is the value of the pixel in cube ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.greater(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def greater_equal_(x, y, track_types = True, **kwargs):
  """Test if x is greater than or equal to y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another data cube which can be aligned to the same shape as cube ``x``.
      In the latter case, when evaluating the expression for a pixel in cube
      ``x`` the second operand is the value of the pixel in cube ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.greater_equal(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def less_(x, y, track_types = True, **kwargs):
  """Test if x is less than y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another data cube which can be aligned to the same shape as cube ``x``.
      In the latter case, when evaluating the expression for a pixel in cube
      ``x`` the second operand is the value of the pixel in cube ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.less(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

def less_equal_(x, y, track_types = True, **kwargs):
  """Test if x is less than or equal to y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another data cube which can be aligned to the same shape as cube ``x``.
      In the latter case, when evaluating the expression for a pixel in cube
      ``x`` the second operand is the value of the pixel in cube ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.less_equal(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

#
# SPATIAL RELATIONAL OPERATORS
#

def intersects_(x, y, track_types = True, **kwargs):
  """Test if x spatially intersects with y.

  This is a specific spatial relational operator meant to be evaluated with
  a spatial coordinate tuple as left-hand side operand, and
  :func:`semantique.geometries` as right-hand side operand. It will
  evaluate if the spatial point with the specified coordinates spatially
  intersects with any of the given geometries.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the operands at the left-hand side of each
      expression, which should be spatial coordinate tuples.
    y :
      Spatial geometries to be used as the right-hand side of each expression.
      May also be another data cube with spatial coordinate tuples. In the
      latter case, when evaluating the expression for a coordinate tuple in
      cube ``x`` the second operand is the spatial bounding box of all
      coordinate tuples in cube ``y``.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
    y = y.sq.trim().sq.grid_points.envelope.unary_union
  values = x.sq.grid_points.intersects(y).astype(int)
  coords = x[x.sq.spatial_dimension].coords
  out = xr.DataArray(values, coords = coords).sq.align_with(x)
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
      Data cube containing the operands at the left-hand side of each
      expression, which should be temporal coordinates.
    y :
      Time instant or time interval to be used as the right-hand side of each
      expression. May also be another data cube with temporal coordinates. In
      the latter case, when evaluating the expression for a coordinate in cube
      ``x`` the second operand is the temporal bounding box of all coordinates
      in cube ``y``.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.greater(x, np.max(y)), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
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
      Data cube containing the operands at the left-hand side of each
      expression, which should be temporal coordinates.
    y :
      Time instant or time interval to be used as the right-hand side of each
      expression. May also be another data cube with temporal coordinates. In
      the latter case, when evaluating the expression for a coordinate in cube
      ``x`` the second operand is the temporal bounding box of all coordinates
      in cube ``y``.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.less(x, np.min(y)), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
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
      Data cube containing the operands at the left-hand side of each
      expression, which should be temporal coordinates.
    y :
      Time interval to be used as the right-hand side of each expression. May
      also be another data cube with temporal coordinates. In the latter case,
      when evaluating the expression for a coordinate in cube ``x`` the second
      operand is the temporal bounding box of all coordinates in cube ``y``.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  def f(x, y, **kwargs):
    a = np.greater_equal(x, np.min(y))
    b = np.less_equal(x, np.max(y))
    return np.where(np.isfinite(x), np.logical_and(a, b), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    out = promoter.promote(out)
  return out

#
# ASSIGNMENT OPERATORS
#

def assign_(x, y, track_types = True, **kwargs):
  """Replace x by y.

  Parameters
  ----------
    x : :obj:`xarray.DataArray`
      Data cube containing the operands at the left-hand side of each
      expression.
    y :
      Operands at the right-hand side of each expression. May be a constant,
      meaning that the same value is used in each expression. May also be
      another data cube which can be aligned to the same shape as cube ``x``.
      In the latter case, when evaluating the expression for a pixel in cube
      ``x`` the second operand is the value of the pixel in cube ``y`` that
      has the same dimension coordinates.
    track_types : :obj:`bool`
      Should the operator promote the value type of the output object, based
      on the value type of the input objects?
    **kwargs:
      Ignored.

  Returns
  -------
    :obj:`xarray.DataArray`
      A data cube with the same shape as ``x`` containing the results of all
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
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.where(np.isfinite(x), y, _nodata(y))
  if track_types:
    out = promoter.promote(out)
  return out
