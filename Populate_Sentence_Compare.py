from Database_Operations import pull_filings_by_year_and_ticker
import time
from fuzzywuzzy.StringMatcher import StringMatcher


def compare_sentences(new_sentences, old_sentences):
    rows = []
    for new_sentence in new_sentences:
        max_score = 0
        for old_sentence in old_sentences:
            if StringMatcher(None, new_sentence[0], old_sentence).ratio() > max_score:
                max_score = StringMatcher(None, new_sentence[0], old_sentence).ratio()
            if max_score > .75:
                break
        rows.append(dict(sentence_id=new_sentence[1], max_score=max_score, sentence_length=len(new_sentence[0].split(" "))))
    return rows

year = 2015
ticker = 'CMC'

if __name__ == '__main__':

    start_time = time.time()
    rows = []
    new_sentences = [(x['sentence'], x['id']) for x in pull_filings_by_year_and_ticker(year, ticker)]
    old_sentences = [x['sentence'] for x in pull_filings_by_year_and_ticker(year - 1, ticker)]

    for x in compare_sentences(new_sentences, old_sentences):
        print x
        # {'sentence_length': 43, 'sentence_id': 909720, 'max_score': 0.759493670886076}
    end_time = time.time()
    print(end_time-start_time)
