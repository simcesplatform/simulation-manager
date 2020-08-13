# -*- coding: utf-8 -*-

"""Common tools for the use of Omega and Simulation Manager."""

import datetime

UTC_TIMEZONE_MARK = "Z"
DIGITS_IN_MILLISECONDS = 3


def get_utcnow_in_milliseconds():
    """Returns the current ISO 8601 format datetime string in UTC timezone."""
    return isoformat_to_milliseconds(datetime.datetime.utcnow().isoformat()) + UTC_TIMEZONE_MARK


def to_iso_format_datetime_string(datetime_value):
    """Returns the given datetime value as ISO 8601 formatted string in UTC timezone.
       Accepts either datetime objects or strings.
       Return None if the given values was invalidate."""
    if isinstance(datetime_value, datetime.datetime):
        return isoformat_to_milliseconds(
            datetime_value.astimezone(datetime.timezone.utc).isoformat()) + UTC_TIMEZONE_MARK
    if isinstance(datetime_value, str):
        datetime_object = to_utc_datetime_object(datetime_value)
        return to_iso_format_datetime_string(datetime_object)
    return None


def to_utc_datetime_object(datetime_str):
    """Returns a datetime object corresponding to the given ISO 8601 formatted string."""
    return datetime.datetime.fromisoformat(datetime_str.replace(UTC_TIMEZONE_MARK, "+00:00"))


def isoformat_to_milliseconds(datetime_str: str):
    """Returns the given ISO 8601 format datetime string in millisecond precision.
       Also removes timezone information."""
    date_mark_index = datetime_str.find("T")
    if date_mark_index < 0:
        return None

    plus_mark_index = datetime_str.find("+", date_mark_index)
    if plus_mark_index >= 0:
        datetime_str = datetime_str[:plus_mark_index]

    minus_mark_index = datetime_str.find("-", date_mark_index)
    if minus_mark_index >= 0:
        datetime_str = datetime_str[:minus_mark_index]

    second_fraction_mark_index = datetime_str.find(".")
    if second_fraction_mark_index >= 0:
        number_of_decimals = len(datetime_str) - second_fraction_mark_index
        return (
            datetime_str[:second_fraction_mark_index + DIGITS_IN_MILLISECONDS + 1] +
            "0" * max(DIGITS_IN_MILLISECONDS - number_of_decimals, 0)
        )

    return datetime_str + "." + "0" * DIGITS_IN_MILLISECONDS
