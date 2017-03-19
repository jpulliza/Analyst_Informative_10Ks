def get_ciks_tickers():
    cik_tickers_csv = '/media/windows-share/SEC_Downloads/NYSE Manufacturing CIKs and Tickers 2017-03-17.csv'

    ciks = []
    tickers = []

    import csv

    with open(cik_tickers_csv, 'rb') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
           ciks.append(row[0])
           tickers.append(row[1])

    print('{0} CIKs and {1} Tickers found'.format(len(ciks), len(tickers)))

    return ciks, tickers
