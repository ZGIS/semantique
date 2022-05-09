import numpy as np
import xarray as xr

from semantique.processor.templates import TYPE_PROMOTION_TEMPLATES

#
# BOOLEAN UNIVARIATE OPERATORS
#

def invert_(x, track_types = False, **kwargs):
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

  """
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.logical_not(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["boolean_univariate_operators"]
    out.sq.promote_value_type(x, func = "invert", manual = manual)
  return out

#
# NUMERICAL UNIVARIATE OPERATORS
#

def absolute_(x, track_types = False, **kwargs):
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

  """
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.absolute(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["algebraic_univariate_operators"]
    out.sq.promote_value_type(x, func = "absolute", manual = manual)
  return out

def cube_root_(x, track_types = False, **kwargs):
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

  """
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.cbrt(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["algebraic_univariate_operators"]
    out.sq.promote_value_type(x, func = "cube_root", manual = manual)
  return out

def exponential_(x, track_types = False, **kwargs):
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

  """
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.exp(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["algebraic_univariate_operators"]
    out.sq.promote_value_type(x, func = "exponential", manual = manual)
  return out


def natural_logarithm_(x, track_types = False, **kwargs):
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

  """
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.log(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["algebraic_univariate_operators"]
    out.sq.promote_value_type(x, func = "natural_logarithm", manual = manual)
  return out

def square_root_(x, track_types = False, **kwargs):
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

  """
  def f(x, **kwargs):
    return np.where(np.isfinite(x), np.sqrt(x), np.nan)
  out = xr.apply_ufunc(f, x, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["algebraic_univariate_operators"]
    out.sq.promote_value_type(x, func = "square_root", manual = manual)
  return out

#
# ALGEBRAIC MULTIVARIATE OPERATORS
#

def add_(x, y, track_types = False, **kwargs):
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

  """
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.add(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["algebraic_multivariate_operators"]
    out.sq.promote_value_type(x, y, func = "add", manual = manual)
  return out

def divide_(x, y, track_types = False, **kwargs):
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

  """
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.divide(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["algebraic_multivariate_operators"]
    out.sq.promote_value_type(x, y, func = "divide", manual = manual)
  return out

def multiply_(x, y, track_types = False, **kwargs):
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

  """
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.multiply(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["algebraic_multivariate_operators"]
    out.sq.promote_value_type(x, y, func = "multiply", manual = manual)
  return out

def power_(x, y, track_types = False, **kwargs):
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

  """
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.power(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["algebraic_multivariate_operators"]
    out.sq.promote_value_type(x, y, func = "power", manual = manual)
  return out

def subtract_(x, y, track_types = False, **kwargs):
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

  """
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.subtract(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["algebraic_multivariate_operators"]
    out.sq.promote_value_type(x, y, func = "subtract", manual = manual)
  return out

#
# BOOLEAN MULTIVARIATE OPERATORS
#

def and_(x, y, track_types = False, **kwargs):
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

  """
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.logical_and(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["boolean_multivariate_operators"]
    out.sq.promote_value_type(x, y, func = "and", manual = manual)
  return out

def or_(x, y, track_types = False, **kwargs):
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

  """
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.logical_or(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["boolean_multivariate_operators"]
    out.sq.promote_value_type(x, y, func = "or", manual = manual)
  return out

def exclusive_or_(x, y, track_types = False, **kwargs):
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

  """
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.logical_xor(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["boolean_multivariate_operators"]
    out.sq.promote_value_type(x, y, func = "exclusive_or", manual = manual)
  return out

#
# EQUALITY OPERATORS
#

def equal_(x, y, track_types = False, **kwargs):
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

  """
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.equal(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["equality_operators"]
    out.sq.promote_value_type(x, y, func = "equal", manual = manual)
  return out

def in_(x, y, track_types = False, **kwargs):
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

  """
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.isin(x, y), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["equality_operators"]
    out.sq.promote_value_type(x, y, func = "in", manual = manual)
  return out

def not_equal_(x, y, track_types = False, **kwargs):
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

  """
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.not_equal(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["equality_operators"]
    out.sq.promote_value_type(x, y, func = "not_equal", manual = manual)
  return out

def not_in_(x, y, track_types = False, **kwargs):
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

  """
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.isin(x, y, invert = True), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["equality_operators"]
    out.sq.promote_value_type(x, y, func = "not_in", manual = manual)
  return out

#
# REGULAR RELATIONAL OPERATORS
#

def greater_(x, y, track_types = False, **kwargs):
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

  """
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.greater(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["regular_relational_operators"]
    out.sq.promote_value_type(x, y, func = "greater", manual = manual)
  return out

def greater_equal_(x, y, track_types = False, **kwargs):
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

  """
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.greater_equal(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["regular_relational_operators"]
    out.sq.promote_value_type(x, y, func = "greater_equal", manual = manual)
  return out

def less_(x, y, track_types = False, **kwargs):
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

  """
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.less(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["regular_relational_operators"]
    out.sq.promote_value_type(x, y, func = "less", manual = manual)
  return out

def less_equal_(x, y, track_types = False, **kwargs):
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

  """
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.less_equal(x, y), np.nan)
  y = xr.DataArray(y).sq.align_with(x)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["regular_relational_operators"]
    out.sq.promote_value_type(x, y, func = "less_equal", manual = manual)
  return out

#
# SPATIAL RELATIONAL OPERATORS
#

def intersects_(x, y, track_types = False, **kwargs):
  """Test if x spatially intersects with y.

  This is a specific spatial relational operator meant to be evaluated with
  a spatial coordinate tuple as left-hand side operand, and spatial
  `geometries <semantique.geometries>` as right-hand side operand. It will
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

  """
  x_gdf = x.sq.grid_points
  try:
    y_gdf = y.unary_union
  except AttributeError:
    y_gdf = y.sq.trim().grid_points.envelope.unary_union
  values = x_gdf.intersects(y_gdf)
  coords = x[x.sq.spatial_dimension].coords
  out = xr.DataArray(values, coords = coords).sq.align_with(x)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["spatial_relational_operators"]
    out.sq.promote_value_type(x, y, func = "intersects", manual = manual)
  return out

#
# TEMPORAL RELATIONAL OPERATORS
#

def after_(x, y, track_types = False, **kwargs):
  """Test if x comes after y.

  This is a specific temporal relational operator meant to be evaluated with
  a temporal coordinate as left-hand side operand, and a
  `time instant <semantique.time_instant>` and/or
  `time interval <semantique.time_interval>` as right-hand side operand. It
  will evaluate if the specified temporal coordinate is later in time than
  the given time instant or the end of the given time interval.

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

  """
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.greater(x, np.max(y)), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["temporal_relational_operators"]
    out.sq.promote_value_type(x, y, func = "after", manual = manual)
  return out

def before_(x, y, track_types = False, **kwargs):
  """Test if x comes before y.

  This is a specific temporal relational operator meant to be evaluated with
  a temporal coordinate as left-hand side operand, and a
  `time instant <semantique.time_instant>` and/or
  `time interval <semantique.time_interval>` as right-hand side operand. It
  will evaluate if the specified temporal coordinate is earlier in time than
  the given time instant or the start of the given time interval.

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

  """
  def f(x, y, **kwargs):
    return np.where(np.isfinite(x), np.less(x, np.min(y)), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["temporal_relational_operators"]
    out.sq.promote_value_type(x, y, func = "before", manual = manual)
  return out

def during_(x, y, track_types = False, **kwargs):
  """Test if x is during interval y.

  This is a specific temporal relational operator meant to be evaluated with
  a temporal coordinate as left-hand side operand, and a
  `time interval <semantique.time_interval>` as right-hand side operand. It
  will evaluate if the specified temporal coordinate fall inside the given
  time interval.

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

  """
  def f(x, y, **kwargs):
    a = np.greater_equal(x, np.min(y))
    b = np.less_equal(x, np.max(y))
    return np.where(np.isfinite(x), np.logical_and(a, b), np.nan)
  out = xr.apply_ufunc(f, x, y, kwargs = kwargs)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["temporal_relational_operators"]
    out.sq.promote_value_type(x, y, func = "during", manual = manual)
  return out

#
# ASSIGNMENT OPERATORS
#

def assign_(x, y, track_types = False, **kwargs):
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

  """
  y = xr.DataArray(y).sq.align_with(x)
  nodata = np.datetime64("NaT") if y.dtype.kind == "M" else np.nan
  out = xr.where(np.isfinite(x), y, nodata)
  if track_types:
    manual = TYPE_PROMOTION_TEMPLATES["assignment_operators"]
    out.sq.promote_value_type(x, y, func = "assign", manual = manual)
    if y.sq.value_labels is not None:
      out.sq.value_labels = y.sq.value_labels
  return out
