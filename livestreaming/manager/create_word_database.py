import nltk
from random import choice
import re
import pickle
import os.path
import sys

# provide some sufficiently long text to extract nouns and adjectives from
word_source = 'word_source.txt'
output_file = 'random_word_database.pickle'

def remove_special(words):
	return list(map(lambda x: re.sub('[^A-Za-z0-9]+', '', x), words))

def remove_empty(words):
	return list(filter(lambda x: len(x) > 3, words))

def to_lowercase(words):
	return list(map(lambda x: x.lower(), words))

with open('word_source.txt', 'r') as file:
	speech = file.read().replace('\n', '')

if not os.path.isfile(word_source):
	print(f"file {word_source} not found")
	sys.exit()

tagged = nltk.pos_tag(nltk.word_tokenize(speech))

nouns_tagged = list(filter(lambda x: x[1] == 'NN', tagged))
nouns = list(map(lambda x: x[0], nouns_tagged))
nouns = list(set(nouns))
nouns = remove_special(nouns)
nouns = remove_empty(nouns)
nouns = to_lowercase(nouns)

adj_tagged = list(filter(lambda x: x[1] == 'JJ', tagged))
adj = list(map(lambda x: x[0], adj_tagged))
adj = list(set(adj))
adj = remove_special(adj)
adj = remove_empty(adj)
adj = to_lowercase(adj)

print(f"#nouns: {len(nouns)} #adj: {len(adj)}")
print("\nExamples:\n")

for i in range(20):
	print(f"{choice(adj)}-{choice(nouns)}")

wordlists = {'nouns': nouns, 'adj': adj}

print(f"\n\nwriting {output_file}...")

with open(output_file, "wb") as f:
	pickle.dump(wordlists, f)
