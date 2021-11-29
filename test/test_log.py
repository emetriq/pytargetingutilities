import unittest
from pytargetingutilities.tools.log import setup_logging, setup_logging_graylog
import logging


class TestLog(unittest.TestCase):

    def test_md5(self):
        logger = setup_logging("test", logging.INFO)
        self.assertIsNotNone(logger)
        try:
            logger.info('test')
        except:
            self.fail("logger.info raised Exception unexpectedly!")

        logger = setup_logging_graylog("test", logging.INFO, True, "", 0)
        self.assertIsNotNone(logger)
        try:
            logger.info('test')
        except:
            self.fail("logger.info raised Exception unexpectedly!")
