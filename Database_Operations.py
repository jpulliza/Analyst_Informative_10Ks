import dataset
from List_10K_Files import list_10k_files
from Sentence_Parse import parse_sentences

db = dataset.connect('sqlite:////media/windows-share/SEC_Downloads/10K_Sentences.db')

table = db['10K_Sentences']

tickers, filing_years, file_locations = list_10k_files()

for i in range(len(file_locations)):
    db.begin()
    try:
        sentences = parse_sentences(file_locations[i])
        for sentence in sentences:
            table.insert(dict(ticker=tickers[i],
                              filing_year=filing_years[i],
                              sentence=sentence))
        db.commit()
    except Exception:
        print("DB Loading Error: {0}").format(file_locations[i])
        db.rollback()
        pass
