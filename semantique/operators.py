NOT = "not"
""":obj:`str` : Reference to operator function not.

This operator tests if x is not true.

See: :func:`semantique.processor.operators.not_`

"""

IS_MISSING = "is_missing"
""":obj:`str` : Reference to operator function is_missing.

This operator tests if x is a missing observation.

See: :func:`semantique.processor.operators.is_missing_`

"""

NOT_MISSING = "not_missing"
""":obj:`str` : Reference to operator function not_missing.

This operator tests if x is a valid observation.

See: :func:`semantique.processor.operators.not_missing_`

"""

ABSOLUTE = "absolute"
""":obj:`str` : Reference to operator function absolute.

This operator computes the absolute value of x.

See: :func:`semantique.processor.operators.absolute_`

"""

CEILING = "ceiling"
""":obj:`str` : Reference to operator function ceiling.

This operator computes the ceiling of x.

See: :func:`semantique.processor.operators.ceiling_`

"""

COS = "cos"
""":obj:`str` : Reference to operator function cos.

This operator computes the cosine of x, assuming x is an angle in radians.

See: :func:`semantique.processor.operators.cos_`

"""

CUBE_ROOT = "cube_root"
""":obj:`str` : Reference to operator function cube_root.

This operator computes the cube root of x.

See: :func:`semantique.processor.operators.cube_root_`

"""

EXPONENTIAL = "exponential"
""":obj:`str` : Reference to operator function exponential.

This operator computes the exponential function of x.

See: :func:`semantique.processor.operators.exponential_`

"""

FLOOR = "floor"
""":obj:`str` : Reference to operator function floor.

This operator computes the floor of x.

See: :func:`semantique.processor.operators.floor_`

"""

NATURAL_LOGARITHM = "natural_logarithm"
""":obj:`str` : Reference to operator function natural_logarithm.

This operator computes the natural logarithm of x.

See: :func:`semantique.processor.operators.natural_logarithm_`

"""

SIN = "sin"
""":obj:`str` : Reference to operator function sin.

This operator computes the sine of x, assuming x is an angle in radians.

See: :func:`semantique.processor.operators.sin_`

"""

SQUARE_ROOT = "square_root"
""":obj:`str` : Reference to operator function square_root.

This operator computes the square root of x.

See: :func:`semantique.processor.operators.square_root_`

"""

TAN = "tan"
""":obj:`str` : Reference to operator function tan.

This operator computes the tangent of x, assuming x is an angle in radians.

See: :func:`semantique.processor.operators.tan_`

"""

ADD = "add"
""":obj:`str` : Reference to operator function add.

This operator adds y to x.

See: :func:`semantique.processor.operators.add_`

"""

DIVIDE = "divide"
""":obj:`str` : Reference to operator function divide.

This operator divides x by y.

See: :func:`semantique.processor.operators.divide_`

"""

MULTIPLY = "multiply"
""":obj:`str` : Reference to operator function multiply.

This operator multiplies x by y.

See: :func:`semantique.processor.operators.multiply_`

"""

POWER = "power"
""":obj:`str` : Reference to operator function power.

This operator raises x to the yth power.

See: :func:`semantique.processor.operators.power_`

"""

SUBTRACT = "subtract"
""":obj:`str` : Reference to operator function subtract.

This operator subtracts y from x.

See: :func:`semantique.processor.operators.subtract_`

"""

NORMALIZED_DIFFERENCE = "normalized_difference"
""":obj:`str` : Reference to operator function normalized_difference.

This operator computes the normalized difference between x and y.

See: :func:`semantique.processor.operators.normalized_difference_`

"""

AND = "and"
""":obj:`str` : Reference to operator function and.

This operator tests if both x and y are true.

See: :func:`semantique.processor.operators.and_`

"""

OR = "or"
""":obj:`str` : Reference to operator function or.

This operator tests if at least one of x and y are true.

See: :func:`semantique.processor.operators.or_`

"""

EXCLUSIVE_OR = "exclusive_or"
""":obj:`str` : Reference to operator function exclusive_or.

This operator tests if either x or y is true but not both.

See: :func:`semantique.processor.operators.exclusive_or_`

"""

EQUAL = "equal"
""":obj:`str` : Reference to operator function equal.

This operator tests if x is equal to y.

See: :func:`semantique.processor.operators.equal_`

"""

IN = "in"
""":obj:`str` : Reference to operator function in.

This operator tests if x is a member of set y.

See: :func:`semantique.processor.operators.in_`

"""

NOT_EQUAL = "not_equal"
""":obj:`str` : Reference to operator function not_equal.

This operator tests if x is not equal to y.

See: :func:`semantique.processor.operators.not_equal_`

"""

NOT_IN = "not_in"
""":obj:`str` : Reference to operator function not_in.

This operator tests if x is not a member of set y.

See: :func:`semantique.processor.operators.not_in_`

"""

GREATER = "greater"
""":obj:`str` : Reference to operator function greater.

This operator tests if x is greater than y.

See: :func:`semantique.processor.operators.greater_`

"""

GREATER_EQUAL = "greater_equal"
""":obj:`str` : Reference to operator function greater_equal.

This operator tests if x is greater than or equal to y.

See: :func:`semantique.processor.operators.greater_equal_`

"""

LESS = "less"
""":obj:`str` : Reference to operator function less.

This operator tests if x is less than y.

See: :func:`semantique.processor.operators.less_`

"""

LESS_EQUAL = "less_equal"
""":obj:`str` : Reference to operator function less_equal.

This operator tests if x is less than or equal to y.

See: :func:`semantique.processor.operators.less_equal_`

"""

INTERSECTS = "intersects"
""":obj:`str` : Reference to operator function intersects.

This operator tests if x spatially intersects with y.

See: :func:`semantique.processor.operators.intersects_`

"""

AFTER = "after"
""":obj:`str` : Reference to operator function after.

This operator tests if x comes after y.

See: :func:`semantique.processor.operators.after_`

"""

BEFORE = "before"
""":obj:`str` : Reference to operator function before.

This operator tests if x comes before y.

See: :func:`semantique.processor.operators.before_`

"""

DURING = "during"
""":obj:`str` : Reference to operator function during.

This operator tests if x is during interval y.

See: :func:`semantique.processor.operators.during_`

"""
