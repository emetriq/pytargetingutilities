
import re
from typing import List
from urllib.parse import urlparse


def tokens(url: str, min_length=3) -> List[str]:
    """ Tokenize a URL using regex.
    :param url: URL to tokenize
    :param min_length: minimum length of tokens
    :return: list of tokens
    """
    url = url.lower()
    parsed = urlparse(url)
    token_list = [parsed.netloc] + [
        token
        for token in re.split(
            r'[!"#$%&()\*\+,-\./:;<=>?@\[\\\]^_`{|}~\'\d]', parsed.path
        )
        if token != ""
    ]
    return list(filter(lambda x: len(x) >= min_length, token_list))


def relevant_tokens(
    urls: List[str],
    compare_func=lambda x, y: x == y,
    min_length=3,
    not_allowed=[],
    sort_key=lambda x: (x[1], len(x[0]), x[0]),
) -> List[str]:
    """
    Finds relevant tokens in a list of urls for matching purposes.
    :param urls: List of urls
    :param compare_func: Function to compare two tokens (default: ==)
    :param min_length: Minimum length of a token (default: 3)
    :param not_allowed: List of tokens that
    :param sort_key: Function to sort the tokens (default: sort by occurrence,
    then by token length, then by token)
    should not be considered (default: [])
    :return: List of relevant tokens
    """
    urls_tokens = list(map(lambda x: tokens(x, min_length), urls))
    all_tokens = [token for url_tokens in urls_tokens for token in url_tokens]
    sorted_tokens = sorted(
        [(x, all_tokens.count(x)) for x in set(all_tokens)],
        key=sort_key,
        reverse=True,
    )
    relevant_tokens = list(map(lambda x: x[0], sorted_tokens))
    result = set()
    for url_tokens in urls_tokens:
        for token in relevant_tokens:
            for url_token in url_tokens:
                if (
                    compare_func(
                        token, url_token
                    )  # check token against url_token token
                    and token not in not_allowed
                    # check token against not_allowed tokens
                    and not any(
                        [val in url_tokens for val in result]
                    )  # check token against already found tokens
                ):
                    result.add(token)
                    break
            else:
                continue  # Continue if the inner loop wasn't broken.
            break  # Inner loop was broken, break the outer
    return list(result)
