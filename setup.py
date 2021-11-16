from setuptools import setup, find_packages

# Get the long description from the README file.
with open("README.md", encoding = "utf-8") as file:
  long_description = file.read()

# List dependencies.
dependencies = [
  'datacube>=1.8',
  'geocube',
  'geopandas',
  'numpy',
  'pandas',
  'pyproj>=3.0',
  'pytz',
  'rasterio',
  'rioxarray',
  'scipy',
  'setuptools',
  'xarray>=0.16'
]

# Setup.
setup(
  name = "semantique",
  version = "0.1.0",
  description = "Semantic querying of Earth Observation data",
  long_description = long_description,
  long_description_content_type = "text/markdown",
  url = "https://github.com/ZGIS/semantique",
  author = "Lucas van der Meer",
  author_email = "lucas.vandermeer@sbg.ac.at",
  packages = find_packages(),
  python_requires = ">=3.7",
  install_requires = dependencies
)