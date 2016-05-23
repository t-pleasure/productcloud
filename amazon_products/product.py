from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AMAZON_ASSOC_TAG
from amazon.api import AmazonAPI
from description import descriptions_to_wordcounts

api = AmazonAPI(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AMAZON_ASSOC_TAG)


class Product:
  def __init__(self, data):
    self.id = data.asin
    self.title = data.title
    self.descriptions = data.editorial_reviews
    self.wordcounts = descriptions_to_wordcounts(self.descriptions)

  @staticmethod 
  def isvalid_pid(pid):
    try:
      api.lookup(ItemId=pid)
      return True
    except:
      return False

  @staticmethod 
  def fetch_product(pid):
    return Product(api.lookup(ItemId=pid))

  @staticmethod
  def url_to_productid(url):
    assert "www.amazon.com/gp/product/" in url, "invalid url: must have the form: www.amazon.com/gp/product/$ID"
    return url.strip().split("/")[-1]
