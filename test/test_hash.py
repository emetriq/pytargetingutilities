import unittest
from pytargetingutilities.tools.hash import md5
import tempfile


class TestHash(unittest.TestCase):

    def test_md5(self):
        m_hash = md5('Pathtofilethatdoesnotexist.json')
        self.assertIsNone(m_hash)
        with tempfile.NamedTemporaryFile() as temp:
            temp.write(b'test data')
            temp.flush()
            my_hash = md5(temp.name)
            self.assertEqual(my_hash, 'eb733a00c0c9d336e65691a37ab54293')
