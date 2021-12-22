class AlignmentError(Exception):
  pass

class EmptyDataError(Exception):
  pass

class InvalidValueTypeError(Exception):
  pass

class InvalidReferenceError(Exception):
  pass

class InvalidBuildingBlockError(Exception):
  pass

class UnknownReducerError(Exception):
  pass

class UnknownOperatorError(Exception):
  pass

class UnknownGeometryTypeError(Exception):
  pass

class MixedTimeZonesError(Exception):
  pass

class TooManyDimensionsError(Exception):
  pass

class MissingDimensionError(Exception):
  pass

class UnmatchingDimensionsError(Exception):
  pass

class UndefinedDimensionComponentError(Exception):
  pass

class UnknownLabelError(Exception):
  pass