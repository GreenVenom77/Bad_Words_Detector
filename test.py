import random
from time import time


list = []
for i in range(46000):
    list.append(random.randint(0, 46_000_0000))
start = time()
list.sort()
end = time()
print(end - start)
