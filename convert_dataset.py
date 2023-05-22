import pandas as pd
import json
from pathlib import Path

def read_in_data(dataset_path: Path):
    # return pd.read_json(dataset_path).to_list(orient="records")
    #Reading the test_seen_entities.jsonl file
    data = []
    with open(dataset_path, "r") as file:
        for line in file:
            data.append(json.loads(line.strip()))

    return data
        

def convert(data):
    #initializing three empty lists that will be used to store the extracted data 
    texts_data = []
    entities_data = []
    mentions_data = []

    for entry in data:
        #Extracting context and generating the text_id
        context = entry["context"]
        text_id = len(texts_data) + 1

        #Adding entry to the texts_data list as dictionary 
        texts_data.append({"original_text": context, "id": text_id})

        #Adding entries to entities and mentions list
        mention2entity = entry["mention2entity"]
        entity2type = entry["entity2type"]

        for mention, entities in mention2entity.items():
            for entity in entities:
                entity_id = entity

                #Checking if the mention exists in the context
                if mention in context:
                    span_start = context.index(mention)
                    span_end = span_start + len(mention)
                    span = mention

                    #Adding entry to the entities list
                    entities_data.append({"qid": entity_id})

                    #Adding entry to the mentions list
                    mentions_data.append({
                        "text_id": text_id, 
                        "entity_id": entity_id, 
                        "span_start": span_start, 
                        "span_end": span_end, 
                        "span": span
                    })
                    
    return texts_data, entities_data, mentions_data


def get_span(text, span_start, span_end):
    span = text[span_start:span_end]
    return span


def check_span(mentions_df, entities_df, texts_df, index):
    mention_row = mentions_df.iloc[index]

    span_start = mention_row.span_start
    span_end= mention_row.span_end
    span = mention_row.span

    text_id = mention_row.text_id
    text = texts_df.loc[texts_df["id"] == text_id, "original_text"].values[0]
    # entity_id = mention_row.entity_id

    extracted_span = get_span(text, span_start, span_end)

    #Checking if the extracted span matches the provided span
    span_correct = extracted_span == span

    # #Checking if the entity ID matches the provided entity ID
    # entity_id_correct = entities_df["qid"].str.contains(entity_id).any()

    return span_correct, True
      

def main(dataset_path: Path):
  
    data = read_in_data(dataset_path)

    texts_data, entities_data, mentions_data = convert(data)

    #Creating DataFrames from list
    texts_df = pd.DataFrame(texts_data)
    entities_df = pd.DataFrame(entities_data)
    mentions_df = pd.DataFrame(mentions_data)

    #Saving DataFrames as Parquet files
    texts_df.to_parquet("texts.parquet")
    entities_df.to_parquet("entities.parquet")
    mentions_df.to_parquet("mentions.parquet")


    #few indices to check
    indices = [15, 20, 25, 30, 35]  

    for index in indices:
        span_correct, entity_id_correct = check_span(
            mentions_df=mentions_df, 
            texts_df=texts_df, 
            entities_df=entities_df,
            index=index
        )

        print(f"Index: {index}")
        print(f"Span Correct: {span_correct}")
        print(f"Entity ID Correct: {entity_id_correct}")
        print()
   
  
if __name__ == "__main__":
    main(Path("test_unseen_entities.jsonl"))
    # import typer
    # typer.run(main)
