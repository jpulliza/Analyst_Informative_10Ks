import dataset
from Sentence_Parse import parse_sentences
import logging

logging.basicConfig(filename='general_log.log', level=logging.INFO)

db = dataset.connect('sqlite:////media/windows-share/SEC_Downloads/Sentence_DB_Server.db')


def record_sentences(filing_id, ticker, filing_year, file_location):
    table = db['Sentences']
    db.begin()
    try:
        sentences = parse_sentences(file_location)
        rows = []
        for sentence in sentences:
            sentence_dict = dict(ticker=ticker, filing_year=filing_year, file_location=file_location, sentence=sentence)
            rows.append(sentence_dict)
        table.insert_many(rows)
        db.commit()
        logging.info("DB Loaded Sentences {0} - {1} - {2} - {3}".format(filing_id, ticker, filing_year, file_location))
    except Exception:
        logging.error("DB Error Sentences {0} - {1} - {2} - {3}".format(filing_id, ticker, filing_year, file_location))
        db.rollback()
        pass


def record_filings(ticker, filing_year, file_location):
    table = db['Filings']
    db.begin()
    try:
        table.insert(dict(ticker=ticker,
                          filing_year=filing_year,
                          file_location=file_location))
        db.commit()
        logging.info("DB Loaded Filings {0} - {1} - {2}".format(ticker, filing_year, file_location))
    except Exception:
        logging.error("DB Error Filings {0} - {1} - {2}".format(ticker, filing_year, file_location))
        db.rollback()
        pass


def pull_filings():
    table = db['New_Filings']
    return table


def pull_filings_by_year(filing_year):
    table = db['New_Filings']
    return table.find(filing_year=filing_year)


def record_sentences_by_year(filing_id, ticker, filing_year, file_location):
    table_name = 'Sentences' + filing_year
    table = db[table_name]
    db.begin()
    try:
        sentences = parse_sentences(file_location)
        rows = []
        for sentence in sentences:
            sentence_dict = dict(ticker=ticker, file_location=file_location, sentence=sentence)
            rows.append(sentence_dict)
        table.insert_many(rows)
        db.commit()
        logging.info("DB Loaded Sentences By Year {0} - {1} - {2} - {3}".format(filing_id, ticker, filing_year, file_location))
    except Exception:
        logging.error("DB Error Sentences By Year {0} - {1} - {2} - {3}".format(filing_id, ticker, filing_year, file_location))
        db.rollback()
        pass


def pull_filings_by_year_and_ticker(filing_year, ticker):
    table_name = 'Sentences' + str(filing_year)
    table = db[table_name]
    return table.find(ticker=ticker)


def pull_sentence_by_year_and_id(filing_year, id):
    table_name = 'Sentences' + filing_year
    table = db[table_name]
    return table.find_one(id=id)


def pull_tickers_by_year(filing_year):
    query = 'SELECT ticker FROM Tickers_{0} ORDER BY ticker'.format(filing_year)
    results = db.query(query)
    return results


def pull_sentences_by_year(filing_year):
    table_name = 'Sentences' + str(filing_year)
    table = db[table_name]
    return table

