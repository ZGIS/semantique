from semantique import dimensions

class time():
  """Components of the time dimension."""

  YEAR = "year"
  """:obj:`str` : Component storing the year of a timestamp."""

  SEASON = "season"
  """:obj:`str` : Component storing the season of a timestamp.

  There are four seasons defined. 1: March, April, May; 2: June, July, August;
  3: September, October, November; 4: December, January, February.

  """

  QUARTER = "quarter"
  """:obj:`str` : Component storing the year quarter of a timestamp.

  There are four quarters defined. 1: January, February, March; 2: April, May,
  June; 3: July, August, September; 4: October, November, December.

  """

  MONTH = "month"
  """:obj:`str` : Component storing the month number of a timestamp."""

  WEEK = "week"
  """:obj:`str` : Component storing the week number of a timestamp."""

  DAY = "day"
  """:obj:`str` : Component storing the day number of a timestamp."""

  DAY_OF_WEEK = "dayofweek"
  """:obj:`str` : Component storing the day of the week of a timestamp.

  The first day of the week is considered Monday, with an index of 0.

  """

  DAY_OF_YEAR = "dayofyear"
  """:obj:`str` : Component storing the day of the year of a timestamp."""

  HOUR = "hour"
  """:obj:`str` : Component storing the hour of a timestamp."""

  MINUTE = "minute"
  """:obj:`str` : Component storing the minute of a timestamp."""

  SECOND = "second"
  """:obj:`str` : Component storing the second of a timestamp."""

class space():
  """Components of the space dimension."""

  X = dimensions.X
  """:obj:`str` : Component storing the X coordinate of a coordinate tuple."""

  Y = dimensions.Y
  """:obj:`str` : Component storing the Y coordinate of a coordinate tuple."""

  FEATURE = "feature"
  """:obj:`str` : Component storing feature index of a coordinate tuple."""