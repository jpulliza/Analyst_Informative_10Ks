import os


def list_10k_files():

    tickers = []
    filing_years = []
    file_locations = []

    for root, dirs, files in os.walk("/media/windows-share/SEC_Downloads"):
        for file in files:
            if file.endswith(".txt"):
                file_location = os.path.join(root, file)
                file_locations.append(file_location)

                tickers.append(file_location.split("/")[4])

                if int(file_location.split("/")[7].split("-")[1]) > 16:
                    filing_years.append("19" + file_location.split("/")[7].split("-")[1])
                else:
                    filing_years.append("20" + file_location.split("/")[7].split("-")[1])

    return tickers, filing_years, file_locations

if __name__ == '__main__':
    list_10k_files()
