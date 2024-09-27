from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("wazeasy")
except PackageNotFoundError:
    # package is not installed
    pass
