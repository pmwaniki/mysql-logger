from logging import handlers

import logging
import os

class GroupWriteRotatingFileHandler(handlers.RotatingFileHandler):
    def _open(self):
        prevumask = os.umask(0o000)  # -rw-rw-rw-
        rtv = logging.handlers.RotatingFileHandler._open(self)
        os.umask(prevumask)
        return rtv