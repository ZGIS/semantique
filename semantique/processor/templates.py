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
    }
  },
  "algebraic_univariate_operators": {
    "numerical": "numerical",
    None: None
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
    }
  },
  "boolean_univariate_operators": {
    "binary": "binary",
    None: None
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
    }
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
    }
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
    }
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
    }
  },
  "boolean_reducers": {
    "binary": "binary",
    None: None
  },
  "count_reducers": {
    "binary": "numerical",
    None: None
  },
  "numerical_reducers": {
    "numerical": "numerical",
    None: None
  },
  "ordered_reducers": {
    "binary": "binary",
    "numerical": "numerical",
    "ordinal": "ordinal",
    "time": "time",
    None: None
  },
  "universal_reducers": {
    "binary": "binary",
    "nominal": "nominal",
    "numerical": "numerical",
    "ordinal": "ordinal",
    "time": "time",
    "space": "space",
    None: None
  },
  "replacers": {
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
    }
  }
}