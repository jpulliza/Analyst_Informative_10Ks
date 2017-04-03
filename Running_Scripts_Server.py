from mapping_functions import chunks
from multiprocessing import Pool
import time
from fuzzywuzzy.StringMatcher import StringMatcher
import dataset
from datetime import datetime
import logging
import csv

logging.basicConfig(filename='general_log.log', level=logging.INFO)
db = dataset.connect('sqlite:////media/windows-share/SEC_Downloads/Sentence_DB_Server.db')
# db = dataset.connect('sqlite:////home/jpp156/Sentence_DB_Server.db')


def pull_tickers_by_year(filing_year):
    query = 'SELECT ticker FROM Tickers_{0} ORDER BY ticker'.format(filing_year)
    results = db.query(query)
    return results


def pull_filings_by_year_and_ticker(filing_year, ticker):
    table_name = 'Sentences' + str(filing_year)
    table = db[table_name]
    result = table.find(ticker=ticker)
    return result


def chunk_list(input_list, number_of_chunks):
    return_list = []
    for chunk in chunks(input_list, len(input_list) / number_of_chunks):
        return_list.append(chunk)
    return return_list


def compare_single_sentence(new_sentences):
    global old_sentences
    rows = []
    for new_sentence in new_sentences:
        max_score = 0
        old_sentence_id = ''
        for old_sentence in old_sentences:
            if StringMatcher(None, new_sentence[0], old_sentence[0]).ratio() > max_score:
                max_score = StringMatcher(None, new_sentence[0], old_sentence[0]).ratio()
                old_sentence_id = old_sentence[1]
            if max_score == 1:
                break
        rows.append(dict(sentence_id=new_sentence[1], old_sentence_id=old_sentence_id,
                         max_score=max_score, sentence_length=len(new_sentence[0].split(" "))))
    return rows


def record_sentence_ids_similarity(filing_year, ticker, rows):
    table_name = 'Similarities' + str(filing_year)
    table = db[table_name]
    db.begin()
    try:
        table.insert_many(rows)
        db.commit()
        logging.info("DB Loaded Similarities By Year {0} - {1} - {2}".format(ticker, filing_year, str(datetime.now())))
    except Exception:
        logging.error("DB Error Similarities By Year {0} - {1} - {2}".format(ticker, filing_year, str(datetime.now())))
        db.rollback()
        pass


def run_sentence_similarities(year, number_of_chunks):
    for result in pull_tickers_by_year(year):
        start_time = time.time()
        global old_sentences
        ticker = result['ticker']
        print(ticker)
        old_sentences = [(x['sentence'], x['id']) for x in pull_filings_by_year_and_ticker(year - 1, ticker)]
        if len(old_sentences) > 0:
            current_year_ids_sentences = [(x['sentence'], x['id'])
                                          for x in pull_filings_by_year_and_ticker(year, ticker)]
            filename = 'similarities_{0}_{1}.csv'.format(year, ticker)
            with open(filename, 'w') as f:
                headers = ['sentence_id', 'old_sentence_id', 'max_score', 'sentence_length']
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                p = Pool(number_of_chunks)
                for chunk in p.map(compare_single_sentence, chunk_list(current_year_ids_sentences, number_of_chunks)):
                    # record_sentence_ids_similarity(year, ticker, x)
                        writer.writerows(chunk)
        else:
            print("No {0} data for {1}".format(year - 1, ticker))
        end_time = time.time()
        print("End - Ticker: {0}, Year:{1}, Time: {2}".format(ticker, year, end_time - start_time))

if __name__ == '__main__':
    run_sentence_similarities(2015, 2)
