from pyspark.sql import SparkSession
import sys
import binascii
import random

filename = sys.argv[1]

spark = SparkSession.builder.master("local") \
                    .appName('Assignment3') \
                    .getOrCreate()

hospitals_rdd = spark.read.option("header", True) \
    .csv(filename)

header = hospitals_rdd.columns


# =============================================================================
#                     Task 1: Extract Binary Features
# =============================================================================


def map_partitions(partition):
    for element in partition:
        yield element


df = hospitals_rdd.rdd.mapPartitions(map_partitions)


def shingles(x, y):
    shingle = [y[0]]
    s = set()
    for i in range(len(x)):
        if i != 0 and x[i]:
            y1 = y[i] or ""
            shingle_str = x[i]+':'+y1
            # crc = binascii.crc32(str.encode(shingle)) & 0xffffffff
            s.add(shingle_str)
    shingle.append(s)
    return shingle


def map_convert(code):
    return list(shingles(header, code))


df = df.map(lambda x: (map_convert(x)))


for i in df.collect():
    if '150034' in i:
        print(i)
    if '050739' in i:
        print(i)
    if '330231' in i:
        print(i)
    if '241326' in i:
        print(i)
    if '070008' in i:
        print(i)


# =============================================================================
#                     Task 2: Minhash
# =============================================================================

def hashing_function(input):
    final_hash = [input[0]]
    s = set()
    for element in input[1]:
        hash = binascii.crc32(str.encode(element)) & 0xffffffff
        s.add(hash)
    final_hash.append(s)
    return final_hash


df = df.map(lambda x: (hashing_function(x)))

df = df.reduceByKey(lambda a, b: a.union(b))

df = df.map(lambda x: (dict([x])))


hospitals = df.map(lambda x: list(x)[0]).collect()


res = {}
for line in df.collect():
    res.update(line)

docsAsShingleSets = res

maxShingleID = 2**32-1
nextPrime = 4294967311


def pick_random_coefficient(k):
    randList = []
    while k > 0:
        randIndex = random.randint(0, maxShingleID)
        while randIndex in randList:
            randIndex = random.randint(0, maxShingleID)
        randList.append(randIndex)
        k = k - 1
    return randList


numHashes = 100
a = pick_random_coefficient(numHashes)
b = pick_random_coefficient(numHashes)

print('\nGenerating MinHash signature vectors for all Hospitals ...')


def get_signatures(x):
    signature = []
    for i in range(0, numHashes):
        minHashCode = nextPrime + 1
        for shingleID in list(x.values())[0]:
            hashCode = (a[i] * shingleID + b[i]) % nextPrime
            if hashCode < minHashCode:
                minHashCode = hashCode
        signature.append(minHashCode)
    if any(e in list(x.keys()) for e in ['150034', '050739', '330231', '241326', '070008']):
        print(signature)
    return signature

df = df.map(lambda x: get_signatures(x))
signatures = df.collect()


# =============================================================================
#                     Task 3: Find Similar Pairs Using LSH
# =============================================================================

def jaccard(x, y):
    return len(x.intersection(y)) / len(x.union(y))

print(jaccard(set(signatures[1]), set(signatures[2])))


def split_vector(signature, b):
    # assert len(signature) % b == 0
    r = int(len(signature) / b)
    subvecs = []
    for i in range(0, len(signature), r):
        if len(signature[i:i+r]) >= r:
            subvecs.append(signature[i:i+r])
    return subvecs


band_a = split_vector(signatures[1], 40)
band_b = split_vector(signatures[2], 40)

for (a_rows, b_rows) in zip(band_a, band_b):
    if a_rows == b_rows:
        print(f"Candidate pair: {a_rows} == {b_rows}")


numElems = int(len(hospitals) * (len(hospitals) - 1) / 2)

estJSim = [0 for x in range(numElems)]


def getTriangleIndex(i, j):
    # If i == j that's an error.
    if i == j:
        sys.stderr.write("Can't access triangle matrix with i == j")
        sys.exit(1)
    # If j < i just swap the values.
    if j < i:
        temp = i
        i = j
        j = temp
    k = int(i * (len(hospitals) - (i + 1) / 2.0) + j - i) - 1
    return k




# =============================================================================
#                     Compare All Hospital Signatures
# =============================================================================

print('\nComparing all the hospital signatures, vector matrix ...')

for i in range(0, len(hospitals)):
    signature1 = signatures[i]
    for j in range(i + 1, len(hospitals)):
        signature2 = signatures[j]
        count = 0
        for k in range(0, numHashes):
            count = count + (signature1[k] == signature2[k])
        estJSim[getTriangleIndex(i, j)] = (count / numHashes)


# =============================================================================
#                   Display Similar Hospital Pairs
# =============================================================================


set_150034 = []
set_050739 = []
set_330231 = []
set_241326 = []
set_070008 = []

for i in range(0, len(hospitals)):
    for j in range(i + 1, len(hospitals)):
        estJ = estJSim[getTriangleIndex(i, j)]
        s1 = docsAsShingleSets[hospitals[i]]
        s2 = docsAsShingleSets[hospitals[j]]
        J = (len(s1.intersection(s2)) / len(s1.union(s2)))
        if hospitals[i] == '150034':
            index = hospitals.index("150034")
            set_150034.append((hospitals[j], J, signatures[index][:10]))
        if hospitals[i] == '050739':
            index = hospitals.index("050739")
            set_050739.append((hospitals[j], J, signatures[index][:10]))
        if hospitals[i] == '330231':
            index = hospitals.index("330231")
            set_330231.append((hospitals[j], J, signatures[index][:10]))
        if hospitals[i] == '241326':
            index = hospitals.index("241326")
            set_241326.append((hospitals[j], J, signatures[index][:10]))
        if hospitals[i] == '070008':
            index = hospitals.index("070008")
            set_070008.append((hospitals[j], J, signatures[index][:10]))


print("\nBelow are top 20 similar hospitals for given Id's")

s_150034 = sorted(set_150034, key = lambda x: x[1], reverse=True)[:20]
s_050739 = sorted(set_050739, key = lambda x: x[1], reverse=True)[:20]
s_330231 = sorted(set_330231, key = lambda x: x[1], reverse=True)[:20]
s_241326 = sorted(set_241326, key = lambda x: x[1], reverse=True)[:20]
s_070008 = sorted(set_070008, key = lambda x: x[1], reverse=True)[:20]

print('\nFor --> 150034')
for i in range(20):
    print(s_150034[i][0], s_150034[i][1], s_150034[i][2])

print('\nFor --> 050739')
for i in range(20):
    print(s_050739[i][0], s_050739[i][1], s_050739[i][2])

print('\nFor --> 330231')
for i in range(20):
    print(s_330231[i][0], s_330231[i][1], s_330231[i][2])

print('\nFor --> 241326')
for i in range(len(s_241326)):
    print(s_241326[i][0], s_241326[i][1], s_241326[i][2])

print('\nFor --> 070008')
for i in range(len(s_070008)):
    print(s_070008[i][0], s_070008[i][1], s_070008[i][2])


threshold = 0.5
print("\nList of Hospital Pairs with Jaccard Value more than", threshold)
print("Values shown are the estimated Jaccard similarity and the actual")
print("Jaccard similarity.\n")
print("                   Est. J   Act. J")

for i in range(0, len(hospitals)):
    for j in range(i + 1, len(hospitals)):
        estJ = estJSim[getTriangleIndex(i, j)]
        if estJ >= threshold:
            s1 = docsAsShingleSets[hospitals[i]]
            s2 = docsAsShingleSets[hospitals[j]]
            J = (len(s1.intersection(s2)) / len(s1.union(s2)))
            print("  %5s --> %5s   %.2f     %.2f" % (hospitals[i], hospitals[j], estJ, J))
