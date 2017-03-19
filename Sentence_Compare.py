from Sentence_Parse import parse_sentences

from fuzzywuzzy import fuzz

import csv

text_file1 = '/media/windows-share/SEC_Downloads/ABT/1800/10-K/0001047469-08-001480.txt'
text_file2 = '/media/windows-share/SEC_Downloads/ABT/1800/10-K/0001047469-09-001642.txt'

sentences1 = sorted(parse_sentences(text_file1)) # add an index first so you know the sentence order
sentences2 = sorted(parse_sentences(text_file2)) # add an index first so you know the sentence order
max_scores = []

with open('test.csv', 'w') as csvfile:
    writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
    i = 0
    for new_sentence in sentences2:
        max_score = 0
        for old_sentence in sentences1:
            if fuzz.token_sort_ratio(new_sentence, old_sentence) > max_score:
                max_score = fuzz.token_sort_ratio(new_sentence, old_sentence)
            if max_score == 100:
                break
        i += 1
        writer.writerow([max_score, new_sentence])
        print(len(sentences2)-i)
