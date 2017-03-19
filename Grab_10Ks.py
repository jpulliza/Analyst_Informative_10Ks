import time
from SECEdgar.crawler import SecCrawler
from Get_CIKs_Tickers import get_ciks_tickers


def get_filings(cik, ticker):
    t1 = time.time()

    # create object
    seccrawler = SecCrawler()

    companyCode = ticker    # company code for apple
    cik = cik      # cik code for apple
    date = '20170101'       # date from which filings should be downloaded
    count = '10'            # no of filings

    seccrawler.filing_10K(str(companyCode), str(cik), str(date), str(count))

    t2 = time.time()
    print ("Total Time taken: "),
    print (t2-t1)

ciks = []
tickers = []

ciks, tickers = get_ciks_tickers()

for i in range(len(ciks)):
    get_filings(ciks[i], tickers[i])


if __name__ == '__main__':
    get_filings()

