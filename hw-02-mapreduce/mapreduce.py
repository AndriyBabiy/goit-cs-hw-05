import string
import asyncio
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict, Counter
import httpx
from matplotlib import pyplot as plt
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

async def get_text(url):
  async with httpx.AsyncClient() as client:
    response = await client.get(url)
    if response.status_code == 200:
      return response.text
    else:
      return None
    
def remove_punctuation(text):
  return text.translate(str.maketrans("", "", string.punctuation)) 

def map_function(word) -> tuple:
  return word, 1 

def shuffle_function(mapped_values):
  shuffled = defaultdict(list)
  for key, value in mapped_values:
    shuffled[key].append(value)
  return shuffled

async def reduce_function(key_values):
  key, values = key_values
  return key, sum(values)
    
async def map_reduce(url, search_words=None):
  text = await get_text(url)
  if text:
    text = remove_punctuation(text)
    words = text.split()

    if search_words:
      words = [word for word in words if word in [word for word in search_words]] 

    # Map step
    mapped_values = [map_function(word) for word in words]

    # Shuffle step
    shuffled_values = shuffle_function(mapped_values)

    # Reduce step using asyncio.gather to handle asynchronous reduce function calls
    reduced_values = await asyncio.gather(*(reduce_function(item) for item in shuffled_values.items()))

    return dict(reduced_values)

    # mapped_values = await asyncio.gather(*(map_function(word) for word in words)) 

    # shuffled_values = shuffle_function(mapped_values)

    # reduced_values = await asyncio.gather(*(reduce_function(key_values) for key_values in shuffled_values)) 

    # return dict(reduced_values)
  else:
    return None
  
def visual_result(result):
  if (result):
    top_10 = Counter(result).most_common(10)
    labels, values = zip(*top_10)
    plt.figure(figsize=(10, 5))
    plt.barh(labels, values, color='g')
    plt.xlabel('Quantity')
    plt.ylabel('Word')
    plt.title('Top 10 popular words')
    # plt.gca().invert_yaxis()
    plt.show()
  else:
    print('No results to display')
    
    
if __name__ == "__main__":
  url = "https://gutenberg.net.au/ebooks01/0100021.txt"
  search_words = ['brother', 'Brother', 'Big', 'big', 'hate', "Hate", 'peace']
  result = asyncio.run(map_reduce(url, search_words))

  print("Result from word calculation: ", result)
  visual_result(result)

