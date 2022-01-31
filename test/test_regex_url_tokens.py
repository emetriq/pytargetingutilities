import unittest
from pytargetingutilities.urltokenizer.regex_url_tokenizer import (
    relevant_tokens,
    tokens,
)


class TestRegexURLTokens(unittest.TestCase):
    def test_local_tokenizer(self):
        url_tokens = tokens('http://ebay.de/autos/luxus/de')
        assert sorted(url_tokens) == sorted(['ebay.de', 'autos', 'luxus'])

    def test_local_tokenizer_with_min_length(self):
        url_tokens = tokens(
            'http://google.com/autos/luxus/deutschland', min_length=6
        )
        assert sorted(url_tokens) == sorted(['google.com', 'deutschland'])

    def test_relevant_token_eq(self):
        urls = [
            'http://test.com/autos',
            'http://test1.com/autos',
            'http://test2.com/autos',
        ]
        assert relevant_tokens(urls) == ['autos']

        urls = [
            'http://test.com/autos',
            'http://test1.com/autos',
            'http://test2.com/autos',
            'http://test.com/games/spiele/starcraft',
            'http://test1.com/games/spiele',
            'http://test2.com/autos/spiele',
        ]
        assert sorted(relevant_tokens(urls)) == sorted(['autos', 'spiele'])

    def test_relevant_tokens_in(self):
        urls = [
            'http://test.com/auto',
            'http://test1.com/auto',
            'http://test2.com/auto',
            'http://test3.com/autos',
        ]
        assert relevant_tokens(urls, lambda x, y: x in y) == ['auto']

    def test_relevant_tokens_exclude(self):
        urls1 = [
            'https://www.autobild.de/artikel/neue-zoe-elektroautos-bis-2024--5777435',
            'https://www.e-autos.de/reanault-zoe-elektroautos',
        ]
        urls2 = [
            'https://teslamag.de/news/luxus-haeuser-tesla-technik-siedlung-florida',
            'https://teslamag.de/news/luxus-eautos-teuer',
        ]

        tokens = relevant_tokens(urls1)
        assert tokens == ['elektroautos']
        tokens = relevant_tokens(urls2, not_allowed=['luxus', 'teslamag.de'])
        assert tokens == ['news']
