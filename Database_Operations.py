import dataset
from List_10K_Files import list_10k_files
from Sentence_Parse import parse_sentences
import logging

logging.basicConfig(filename='general_log.log', level=logging.INFO)

db = dataset.connect('sqlite:////media/windows-share/SEC_Downloads/10K_Sentences.db')


def record_sentences(ticker, filing_year, file_location):
    table = db['10K_Sentences']
    db.begin()
    try:
        sentences = parse_sentences(file_location)
        for sentence in sentences:
            table.insert(dict(ticker=ticker,
                              filing_year=filing_year,
                              file_location=file_location,
                              sentence=sentence))
        db.commit()
        logging.info("DB Loaded Sentences {0} - {1} - {2}".format(ticker, filing_year, file_location))
    except Exception:
        logging.error("DB Error Sentences {0} - {1} - {2}".format(ticker, filing_year, file_location))
        db.rollback()
        pass


def record_filings(id, ticker, filing_year, file_location):
    table = db['Filings']
    db.begin()
    try:
        table.insert(dict(ticker=ticker,
                          filing_year=filing_year,
                          file_location=file_location))
        db.commit()
        logging.info("DB Loaded Filings {0} - {1} - {2} - {3}".format(id, ticker, filing_year, file_location))
    except Exception:
        logging.error("DB Error Filings {0} - {1} - {2} - {3}".format(id, ticker, filing_year, file_location))
        db.rollback()
        pass
