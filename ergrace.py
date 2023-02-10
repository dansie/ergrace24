#!/usr/bin/env python
"""
Copyright (c) 2023, Daniel Siebert
Licensed under the Simplified BSD License.

This application has been created for an ergometer rowing event with
long period non-stop rowing time. There are rumors about a reset after 100 km
so exceptions are handled as far as possible. Lost connections are tried to be
reestablished.
Ergometer data is read from the PM5 and stored as a csv file.
"""

import csv
import time
import datetime
import sys
import functools
from typing import Callable, Any, Union, Type, Optional, Tuple, IO, Dict
sys.path.insert(0, 'Py3Row')
from pyrow.pyrow import PyErg
from pyrow import pyrow

class RaceException(Exception):
    """
    Base exception type used here.
    """

class ErgNotFoundException(RaceException):
    """
    No ergometer found.
    """

def print_exception(
        exception_types: Union[Type[BaseException], Tuple[Type[BaseException], ...]]) \
            -> Callable[Callable[..., Any], Callable[..., Any]]:
    """
    Return a decorator that catches an exception, prints its message and raises
    it again.
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        """
        decorator function
        """
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except exception_types as exception:
                print(str(exception))
                raise
        return wrapper
    return decorator

def retry_on_exception(
        exception_types: Union[Type[BaseException], Tuple[Type[BaseException], ...]],
        max_nretries: Optional[int],
        retry_delay: Optional[float]) \
            -> Callable[Callable[..., Any], Callable[..., Any]]:
    """
    Return a decorator that catches exceptions and retries to call the decorated
    function.
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        """
        decorator function
        """
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            nretries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except exception_types:
                    if not nretries is None and nretries == max_nretries:
                        raise
                    nretries += 1
                    if not retry_delay is None:
                        time.sleep(retry_delay)
        return wrapper
    return decorator

class ErgRace():
    """
    ErgRace class used to gather RowErg monitor data and store these in a csv
    file.
    """

    _headers = [ 'time', 'distance', 'spm', 'power', 'pace' ]
    _csv_file = None
    _csv_writer = None
    _header_written = False
    _erg: PyErg = None
    _mon_data_last = None

    def __init__(self, iostream: IO):
        self._csv_file = iostream
        self._csv_writer = csv.writer(file)
        self._erg = ErgRace._connect()

    @staticmethod
    def _getdate() -> str:
        """
        Return the current date and time as string.
        """
        return datetime.datetime.now().strftime("%y-%m-%d %H:%M:%S")

    @retry_on_exception(RaceException, max_nretries=None, retry_delay=1)
    @print_exception(RaceException)
    @staticmethod
    def _connect() -> PyErg:
        """
        Find a RowErg and connect. If there are more than one RowErgs then
        connect to the first one.
        """
        devices = list(pyrow.find())
        if len(devices) == 0:
            raise ErgNotFoundException("No Concept2 RowErg found.")
        if len(devices) > 1:
            print("Multiple Concept2 RowErgs connected. Using the first one, only.")
        erg = PyErg(devices[0], debug=False)
        return erg

    def get_erg_serial(self) -> str:
        """
        Return the RowErg serial number.
        """
        return str(self._erg.get_erg()["serial"])

    @retry_on_exception(Exception, max_nretries=10, retry_delay=0.1)
    def get_data(self, skip_unchanged=False) -> Optional[Dict[str, str]]:
        """
        Read monitor data from the RowErg, write these to the csv file and
        return them.
        """
        mon_data=self._erg.get_monitor(forceplot=True)
        if skip_unchanged and mon_data == self._mon_data_last:
            return None
        self._mon_data_last = mon_data
        if not self._header_written:
            self._csv_writer.writerow(['date'] + self._headers)
            self._header_written = True
        values=[ self._getdate() ] + [ mon_data[h] for h in self._headers ]
        self._csv_writer.writerow(values)
        self._csv_file.flush()
        return values

@retry_on_exception((ConnectionError,RaceException), max_nretries=None, retry_delay=1)
@print_exception(ConnectionError)
def main(iostream: IO) -> None:
    erg_race = ErgRace(iostream)
    print("Connected to RowErg with serial " + erg_race.get_erg_serial())
    while True:
        values = erg_race.get_data()
        if values:
            print(values)
        time.sleep(1)

if __name__ == '__main__':
    filename = "erg_" + "_" + datetime.datetime.now().strftime("%y%m%d_%H%M%S.csv")
    print("Saving to CSV file \"" + filename + "\"")
    try:
        with open(filename, 'w', encoding="UTF8") as file:
            main(file)
    except KeyboardInterrupt:
        pass

