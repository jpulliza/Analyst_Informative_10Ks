from textblob import TextBlob
from bs4 import BeautifulSoup
import re

import logging

logging.basicConfig(filename='general_log.log', level=logging.INFO)


def parse_sentences(textfile):
    f = open(textfile, 'r')
    try:
        soup = BeautifulSoup(f.read(), 'lxml')

        text = soup.find('text')

        text = re.sub("\d", "#", text.get_text())  # replace all numbers

        text = text.replace("#.#", "###")
        text = text.replace("#", "")
        text = text.replace("www.", "www/")
        text = text.replace(".com", "/com")
        text = text.replace(".gov", "/gov")
        text = text.replace(".", ". ")
        text = re.sub(r'[^a-zA-Z.\s]+', ' ', text)

        blob = TextBlob(text)

        sentences = []

        for sentence in blob.sentences:
            sentence_words = "{0}".format(sentence)
            sentence_words = sentence_words.replace(".", "")
            sentence_words = ' '.join(sentence_words.split())
            if len(sentence_words.split(" ")) > 3:
                sentences.append(sentence_words)
        logging.info("Sentence Parser Complete: {0}".format(textfile))
        return sentences
    except Exception:
        logging.error("Sentence Parser Error: {0}".format(textfile))
        pass
