"""
================
pansat.exception
================

This module defines a hierarchy of exception classes which should be used
by pansat code.
"""


class PansatException(Exception):
    """
    Base class for all exceptions thrown from within pansat.
    """


class CommunicationError(PansatException):
    """
    This error is thrown when the communication with any of the remote
    servers fails.
    """


class NoAvailableProvider(PansatException):
    """
    Exception indicating that no suitable provider could be found for
    a product.
    """


class MissingDependency(PansatException):
    """
    Thrown when an optional dependency is found to be missing.
    """
