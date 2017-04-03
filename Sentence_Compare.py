from fuzzywuzzy import fuzz
import time


def compare_sentences(new_sentences, old_sentences):
    start = time.time()
    old_sentences = sorted(old_sentences)
    rows = []
    for new_sentence, sentence_id in new_sentences:
        max_score = 0
        for old_sentence in old_sentences:
            if fuzz.token_sort_ratio(new_sentence, old_sentence) > max_score:
                max_score = fuzz.token_sort_ratio(new_sentence, old_sentence)
            if max_score == 100:
                break
        compare_dict = dict(sentence_id=sentence_id, max_score=max_score, sentence_length=len(new_sentence.split(" ")))
        rows.append(compare_dict)
    end = time.time()
    print(end-start)
    return rows


def compare_single_sentence(new_sentence, new_sentence_id, old_sentences):
    start_time = time.time()
    rows = []
    max_score = 0
    for old_sentence in old_sentences:
        if fuzz.token_sort_ratio(new_sentence, old_sentence) > max_score:
            max_score = fuzz.token_sort_ratio(new_sentence, old_sentence)
        if max_score == 100:
            break
    compare_dict = dict(sentence_id=new_sentence_id, max_score=max_score, sentence_length=len(new_sentence.split(" ")))
    print compare_dict
    rows.append(compare_dict)
    end_time = time.time()
    print(end_time-start_time)
    return rows
