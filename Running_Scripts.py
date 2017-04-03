from List_10K_Files import list_10k_files
from Database_Operations import record_filings, record_sentences, pull_filings, \
    pull_filings_by_year, record_sentences_by_year, pull_filings_by_year_and_ticker, pull_tickers_by_year
from mapping_functions import chunks
from multiprocessing import Pool
import time
from Sentence_Compare import compare_sentences
from fuzzywuzzy.StringMatcher import StringMatcher
import dataset
from datetime import datetime
import logging

logging.basicConfig(filename='general_log.log', level=logging.INFO)
db = dataset.connect('sqlite:////media/windows-share/SEC_Downloads/Sentence_DB_Server.db')


def populate_filings_table():
    tickers, filing_years, file_locations = list_10k_files()

    for i in range(len(tickers)):
        record_filings(tickers[i], filing_years[i], file_locations[i])


def sql_db_load():
    for filing in pull_filings():
        record_sentences(filing_id=filing['id'],
                         ticker=filing['ticker'],
                         filing_year=filing['filing_year'],
                         file_location=filing['file_location'])


def sql_db_load_by_year():
    for i in range(2013, 2016, 1):
        for filing in pull_filings_by_year(i):
            record_sentences_by_year(filing_id=filing['id'], ticker=filing['ticker'], filing_year=filing['filing_year'], file_location=filing['file_location'])
        print('The year {0} Complete'.format(i))


def compare_sentence_filings(ticker, year):
    old_sentences = []
    new_sentences = []

    for current_row in pull_filings_by_year_and_ticker('{}'.format(year-1), ticker):
        old_sentences.append(current_row['sentence'])

    for current_row in pull_filings_by_year_and_ticker('{}'.format(year), ticker):
        new_sentences.append([current_row['id'], current_row['sentence']])

    compare_sentences(new_sentences, old_sentences)


def chunk_list(input_list):
    return_list = []
    for chunk in chunks(input_list, len(input_list) / 8):
        return_list.append(chunk)
    return return_list


def compare_single_sentence(new_sentences):
    global old_sentences
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

year = 2015

if __name__ == '__main__':
    for result in pull_tickers_by_year(year):
        start_time = time.time()
        i = 1
        print("Start Ticker Number {0} for Year {1}: {2}".format(i, year, str(datetime.now())))
        global old_sentences
        ticker = result['ticker']
        old_sentences = [x['sentence'] for x in pull_filings_by_year_and_ticker(year - 1, ticker)]
        if len(old_sentences) > 0:
            current_year_ids_sentences = [(x['sentence'], x['id'])
                                          for x in pull_filings_by_year_and_ticker(year, ticker)]
            p = Pool(8)
            for x in p.map(compare_single_sentence, chunk_list(current_year_ids_sentences)):
                record_sentence_ids_similarity(year, ticker, x)
            end_time = time.time()
        else:
            print("No {0} data for {1}".format(year-1, ticker))
        print("End - Ticker: {0}, Year:{1}, Time: {2}".format(ticker, year, end_time-start_time))
        exit()
        i += 1
'''
if __name__ == '__main__':
    start_time = time.time()
    global old_sentences
    old_sentences = sorted([x['sentence'] for x in pull_filings_by_year_and_ticker(year - 1, ticker)])
    current_year_ids_sentences = [(x['sentence'], x['id']) for x in pull_filings_by_year_and_ticker(year, ticker)]
    with open('/media/windows-share/SEC_Downloads/pooled.csv', 'w') as f:
        fieldnames = ['sentence_length', 'sentence_id', 'max_score']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        p = Pool(8)
        for x in p.map(compare_single_sentence, chunk_list(current_year_ids_sentences)):
            for y in x:
                writer.writerow(y)

    end_time = time.time()
    print(end_time-start_time)
'''
