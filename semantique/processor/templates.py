TYPE_PROMOTION_TEMPLATES = {
  "algebraic_multivariate_operators": {
    "numerical": {
      "numerical": "numerical",
      "i": "numerical",
      "u": "numerical",
      "f": "numerical",
      None: None
    },
    None: {
      "numerical": None,
      "i": None,
      "u": None,
      "f": None,
      None: None
    },
    "__preserve_labels__": False
  },
  "algebraic_univariate_operators": {
    "numerical": "numerical",
    None: None,
    "__preserve_labels__": False
  },
  "boolean_multivariate_operators": {
    "binary": {
      "binary": "binary",
      "b": "binary",
      None: None
    },
    None: {
      "binary": None,
      "b": None,
      None: None
    },
    "__preserve_labels__": True
  },
  "boolean_univariate_operators": {
    "binary": "binary",
    None: None,
    "__preserve_labels__": True
  },
  "equality_operators": {
    "binary": {
      "binary": "binary",
      "b": "binary",
      None: None
    },
    "nominal": {
      "nominal": "binary",
      "i": "binary",
      "u": "binary",
      "U": "binary",
      None: None
    },
    "numerical": {
      "numerical": "binary",
      "i": "binary",
      "u": "binary",
      "f": "binary",
      None: None
    },
    "ordinal": {
      "ordinal": "binary",
      "i": "binary",
      "u": "binary",
      None: None
    },
    "time": {
      "time": "binary",
      "M": "binary",
      None: None
    },
    None: {
      "binary": None,
      "ordinal": None,
      "numerical": None,
      "time": None,
      "space": None,
      "b": None,
      "i": None,
      "u": None,
      "f": None,
      "M": None,
      "O": None,
      "U": None,
      None: None
    },
    "__preserve_labels__": False
  },
  "regular_relational_operators": {
    "binary": {
      "binary": "binary",
      "b": "binary",
      None: None
    },
    "numerical": {
      "numerical": "binary",
      "i": "binary",
      "u": "binary",
      "f": "binary",
      None: None
    },
    "ordinal": {
      "ordinal": "binary",
      "i": "binary",
      "u": "binary",
      None: None
    },
    "time": {
      "time": "binary",
      "M": "binary",
      None: None
    },
    None: {
      "binary": None,
      "ordinal": None,
      "numerical": None,
      "time": None,
      "space": None,
      "b": None,
      "i": None,
      "u": None,
      "f": None,
      "M": None,
      "O": None,
      "U": None,
      None: None
    },
    "__preserve_labels__": False
  },
  "spatial_relational_operators": {
    "space": {
      "space": "binary",
      "O": "binary",
      None: None
    },
    None: {
      "space": None,
      "O": None,
      None: None
    },
    "__preserve_labels__": False
  },
  "temporal_relational_operators": {
    "time": {
      "time": "binary",
      "M": "binary",
      None: None
    },
    None: {
      "time": None,
      "M": None,
      None: None
    },
    "__preserve_labels__": False
  },
  "assignment_operators": {
    "binary": {
      "binary": "binary",
      "nominal": "nominal",
      "numerical": "numerical",
      "ordinal": "ordinal",
      "time": "time",
      "space": "space",
      "b": "binary",
      "i": "numerical",
      "u": "numerical",
      "f": "numerical",
      "M": "time",
      "O": "nominal",
      "U": "nominal",
      None: None
    },
    "nominal": {
      "binary": "binary",
      "nominal": "nominal",
      "numerical": "numerical",
      "ordinal": "ordinal",
      "time": "time",
      "space": "space",
      "b": "binary",
      "i": "numerical",
      "u": "numerical",
      "f": "numerical",
      "M": "time",
      "O": "nominal",
      "U": "nominal",
      None: None
    },
    "numerical": {
      "binary": "binary",
      "nominal": "nominal",
      "numerical": "numerical",
      "ordinal": "ordinal",
      "time": "time",
      "space": "space",
      "b": "binary",
      "i": "numerical",
      "u": "numerical",
      "f": "numerical",
      "M": "time",
      "O": "nominal",
      "U": "nominal",
      None: None
    },
    "ordinal": {
      "binary": "binary",
      "nominal": "nominal",
      "numerical": "numerical",
      "ordinal": "ordinal",
      "time": "time",
      "space": "space",
      "b": "binary",
      "i": "numerical",
      "u": "numerical",
      "f": "numerical",
      "M": "time",
      "O": "nominal",
      "U": "nominal",
      None: None
    },
    "time": {
      "binary": "binary",
      "nominal": "nominal",
      "numerical": "numerical",
      "ordinal": "ordinal",
      "time": "time",
      "space": "space",
      "b": "binary",
      "i": "numerical",
      "u": "numerical",
      "f": "numerical",
      "M": "time",
      "O": "nominal",
      "U": "nominal",
      None: None
    },
    "space": {
      "binary": "binary",
      "nominal": "nominal",
      "numerical": "numerical",
      "ordinal": "ordinal",
      "time": "time",
      "space": "space",
      "b": "binary",
      "i": "numerical",
      "u": "numerical",
      "f": "numerical",
      "M": "time",
      "O": "nominal",
      "U": "nominal",
      None: None
    },
    None: {
      "binary": None,
      "nominal": None,
      "numerical": None,
      "ordinal": None,
      "time": None,
      "space": None,
      "b": None,
      "i": None,
      "u": None,
      "f": None,
      "M": None,
      "O": None,
      "U": None,
      None: None
    },
    "__preserve_labels__": False
  },
  "boolean_reducers": {
    "binary": "binary",
    None: None,
    "__preserve_labels__": True
  },
  "count_reducers": {
    "binary": "numerical",
    None: None,
    "__preserve_labels__": False
  },
  "numerical_reducers": {
    "numerical": "numerical",
    None: None,
    "__preserve_labels__": False
  },
  "ordered_reducers": {
    "binary": "binary",
    "numerical": "numerical",
    "ordinal": "ordinal",
    "time": "time",
    None: None,
    "__preserve_labels__": True
  },
  "universal_reducers": {
    "binary": "binary",
    "nominal": "nominal",
    "numerical": "numerical",
    "ordinal": "ordinal",
    "time": "time",
    "space": "space",
    None: None,
    "__preserve_labels__": True
  }
}