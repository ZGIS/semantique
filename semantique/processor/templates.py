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
  "logical_multivariate_operators": {
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
  "logical_univariate_operators": {
    "binary": "binary",
    None: None
  },
  "comparison_operators": {
    "binary": {
      "binary": "binary",
      "b": "binary",
      None: None
    },
    "categorical": {
      "categorical": "binary",
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
    "time": {
      "time": "binary",
      "M": "binary",
      None: None
    },
    None: {
      "binary": None,
      "categorical": None,
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
  "spatial_operators": {
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
  "temporal_operators": {
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
  "algebraic_reducers": {
    "numerical": "numerical",
    None: None
  },
  "count_reducers": {
    "binary": "numerical",
    None: None
  },
  "logical_reducers": {
    "binary": "binary",
    None: None
  },
  "universal_reducers": {
    "binary": "binary",
    "categorical": "categorical",
    "numerical": "numerical",
    "time": "time",
    "space": "space",
    None: None
  },
  "replacers": {
    "binary": {
      "binary": "binary",
      "categorical": "categorical",
      "numerical": "numerical",
      "time": "time",
      "space": "space",
      "b": "binary",
      "i": "numerical",
      "u": "numerical",
      "f": "numerical",
      "M": "time",
      "O": "categorical",
      "U": "categorical",
      None: None
    },
    "categorical": {
      "binary": "binary",
      "categorical": "categorical",
      "numerical": "numerical",
      "time": "time",
      "space": "space",
      "b": "binary",
      "i": "numerical",
      "u": "numerical",
      "f": "numerical",
      "M": "time",
      "O": "categorical",
      "U": "categorical",
      None: None
    },
    "numerical": {
      "binary": "binary",
      "categorical": "categorical",
      "numerical": "numerical",
      "time": "time",
      "space": "space",
      "b": "binary",
      "i": "numerical",
      "u": "numerical",
      "f": "numerical",
      "M": "time",
      "O": "categorical",
      "U": "categorical",
      None: None
    },
    "time": {
      "binary": "binary",
      "categorical": "categorical",
      "numerical": "numerical",
      "time": "time",
      "space": "space",
      "b": "binary",
      "i": "numerical",
      "u": "numerical",
      "f": "numerical",
      "M": "time",
      "O": "categorical",
      "U": "categorical",
      None: None
    },
    "space": {
      "binary": "binary",
      "categorical": "categorical",
      "numerical": "numerical",
      "time": "time",
      "space": "space",
      "b": "binary",
      "i": "numerical",
      "u": "numerical",
      "f": "numerical",
      "M": "time",
      "O": "categorical",
      "U": "categorical",
      None: None
    },
    None: {
      "binary": None,
      "categorical": None,
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
  }
}