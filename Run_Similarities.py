from mapping_functions import chunks
import time
from fuzzywuzzy.StringMatcher import StringMatcher
import dataset
from datetime import datetime
import logging
import csv

logging.basicConfig(filename='general_log.log', level=logging.INFO)
db = dataset.connect('sqlite:////media/windows-share/SEC_Downloads/Sentence_DB_Server.db')
# db = dataset.connect('sqlite:////home/jpp156/Sentence_DB_Server.db')

summary_directory = "/media/windows-share/SEC_Downloads/Document_Summary/"
similarity_headers = ['sentence_id', 'old_sentence_id', 'max_score', 'sentence_length']

def pull_tickers_by_year(filing_year):
    results = []
    filename = "{0}Tickers_{1}.csv".format(summary_directory, filing_year)
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            results.append(row)
    return results


def pull_sentences_by_year(filing_year):
    results = []
    filename = "{0}Sentences{1}.csv".format(summary_directory, filing_year)
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            results.append(row)
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


def compare_single_sentence(new_sentence, old_sentences):
    max_score = 0
    old_sentence_id = ''
    for old_sentence in old_sentences:
        if StringMatcher(None, new_sentence['sentence'], old_sentence['sentence']).ratio() > max_score:
            max_score = StringMatcher(None, new_sentence['sentence'], old_sentence['sentence']).ratio()
            old_sentence_id = old_sentence['id']
        if max_score == 1:
            break
    results = dict(sentence_id=new_sentence['id'], old_sentence_id=old_sentence_id,
                     max_score=max_score, sentence_length=len(new_sentence['sentence'].split(" ")))
    return results


def server_run_similarities(year, tickers):
    start_time = time.time()
    rows = []
    new_sentences_complete = pull_sentences_by_year(year)
    old_sentences_complete = pull_sentences_by_year(year-1)
    for ticker in tickers:
        old_sentences = [x for x in old_sentences_complete if x['ticker'] == ticker]
        if len(old_sentences) > 0:
            for new_sentence in [x for x in new_sentences_complete if x['ticker'] == ticker]:
                rows.append(compare_single_sentence(new_sentence, old_sentences))
            logging.info("Similarities {0} complete for {1} - {2}".format(year, ticker, datetime.now()))
        else:
            print("No {0} sentences for {1}".format(year - 1, ticker))
            logging.info("No {0} sentences for {1} - {2}".format(year - 1, ticker, datetime.now()))
        end_time = time.time()
        print('{0} - {1} complete - {2}'.format(ticker, year, end_time-start_time))
    return rows

if __name__ == '__main__':
    year = 2015
    filename = 'similarities_{0}.csv'.format(year)
    with open(filename, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=similarity_headers)
        writer.writeheader()
        ticker_list = pull_tickers_by_year(year)
        writer.writerows(server_run_similarities(year, ticker_list))
