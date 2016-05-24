
"""
Module to help with normalizing descriptions
"""

import os, re
from itertools import ifilter, imap
from collections import defaultdict

# reads in default stop words from stopwords.txt
STOP_WORDS_FILE = os.path.join('amazon_products','stopwords.txt')
DEFAULT_STOP_WORDS = set(w.strip().lower() for w in open(STOP_WORDS_FILE))

def isHTML(tok):
  return any(map(lambda ch: ch in tok, ["/","=",">","<","\\","\""]))

def get_normalized_tokens(descr, stopwords=DEFAULT_STOP_WORDS):
  """
    returns generator yielding stop words from description string
  """
  # create tokens
  tokens = (word.strip().lower() for word in re.split('[?., \t)(]', descr))
  # remove stop words and empty strings
  tokens = ifilter(lambda t: t and t not in stopwords, tokens) 
  # remove odd html artifacts
  tokens = ifilter(lambda t: not(isHTML(t)), tokens)
  # remove improper unicode
  tokens = imap(lambda s: s.encode('ascii', 'ignore').decode('ascii'), tokens)
  return tokens

def descriptions_to_wordcounts(descriptions):
  """
    converts descriptions (list[string]) to Map[string -> int]
  """
  ret = defaultdict(int)
  for descr in descriptions:
    for word in get_normalized_tokens(descr):
      ret[word] += 1
  return ret
