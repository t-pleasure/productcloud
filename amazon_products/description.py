import os, re
from collections import defaultdict

# reads in default stop words from stopwords.txt
STOP_WORDS_FILE = os.path.join('amazon_products','stopwords.txt')
DEFAULT_STOP_WORDS = set(w.strip().lower() for w in open(STOP_WORDS_FILE))

def get_normalized_tokens(descr, stopwords=DEFAULT_STOP_WORDS):
  """
    returns generator yielding stop words from description string
  """
  for word in re.split('[?., \t]', descr):
    normword = word.strip().lower()
    if not(normword) or normword in stopwords:
      continue
    yield normword


def descriptions_to_wordcounts(descriptions):
  """
    converts descriptions (list[string]) to Map[string -> int]
  """
  ret = defaultdict(int)
  for descr in descriptions:
    for word in get_normalized_tokens(descr):
      ret[word] += 1
  return ret
