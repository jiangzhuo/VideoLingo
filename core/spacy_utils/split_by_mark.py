import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import os,sys
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from core.spacy_utils.load_nlp_model import init_nlp
from core.step2_whisper import get_whisper_language
from config import get_joiner, WHISPER_LANGUAGE
from rich import print

def split_by_mark(nlp):
    language = get_whisper_language() if WHISPER_LANGUAGE == 'auto' else WHISPER_LANGUAGE # consider force english case
    joiner = get_joiner(language)
    print(f"[blue]🔍 Using {language} language joiner: '{joiner}'[/blue]")
    chunks = pd.read_excel("output/log/cleaned_chunks.xlsx")
    chunks.text = chunks.text.apply(lambda x: x.strip('"'))
    
    # join with joiner
    input_text = joiner.join(chunks.text.to_list())

    # Split input_text into smaller chunks
    max_bytes = 49000  # Slightly less than the max to be safe
    text_chunks = []
    current_chunk = ""
    
    if joiner:
        sentences = input_text.split(joiner)
    else:
        sentences = [input_text]  # Treat the entire input as one sentence if no joiner

    for sentence in sentences:
        if len((current_chunk + sentence).encode('utf-8')) > max_bytes:
            text_chunks.append(current_chunk)
            current_chunk = sentence
        else:
            current_chunk += (joiner if current_chunk and joiner else "") + sentence
    
    if current_chunk:
        text_chunks.append(current_chunk)

    sentences_by_mark = []
    for chunk in text_chunks:
        # Ensure each chunk is within the byte limit
        if len(chunk.encode('utf-8')) > max_bytes:
            sub_chunks = []
            current_sub_chunk = ""
            for char in chunk:
                if len((current_sub_chunk + char).encode('utf-8')) > max_bytes:
                    sub_chunks.append(current_sub_chunk)
                    current_sub_chunk = char
                else:
                    current_sub_chunk += char
            if current_sub_chunk:
                sub_chunks.append(current_sub_chunk)
            
            for sub_chunk in sub_chunks:
                doc = nlp(sub_chunk)
                assert doc.has_annotation("SENT_START")
                sentences_by_mark.extend([sent.text for sent in doc.sents])
        else:
            doc = nlp(chunk)
            assert doc.has_annotation("SENT_START")
            sentences_by_mark.extend([sent.text for sent in doc.sents])

    with open("output/log/sentence_by_mark.txt", "w", encoding="utf-8") as output_file:
        for i, sentence in enumerate(sentences_by_mark):
            if i > 0 and sentence.strip() in [',', '.', '，', '。', '？', '！']:
                # ! If the current line contains only punctuation, merge it with the previous line, this happens in Chinese, Japanese, etc.
                output_file.seek(output_file.tell() - 1, os.SEEK_SET)  # Move to the end of the previous line
                output_file.write(sentence)  # Add the punctuation
            else:
                output_file.write(sentence + "\n")
    
    print("[green]💾 Sentences split by punctuation marks saved to →  `sentences_by_mark.txt`[/green]")

if __name__ == "__main__":
    nlp = init_nlp()
    split_by_mark(nlp)
