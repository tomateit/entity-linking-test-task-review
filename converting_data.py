import pandas as pd
import json


def read_jsonl_file(file_path):
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            data.append(json.loads(line))
    return data


def extract_data_from_jsonl(data):
    texts_data = []
    entities_data = []
    mentions_data = []

    for entry in data:
        context = entry['context']
        text_id = len(texts_data) + 1
        texts_data.append({'original_text': context, 'id': text_id})

        mention2entity = entry['mention2entity']
        entity2type = entry['entity2type']

        for mention, entities in mention2entity.items():
            for entity in entities:
                entity_id = entity
                if mention in context:
                    span_start = context.index(mention)
                    span_end = span_start + len(mention)
                    span = mention

                    entities_data.append({'qid': entity_id})
                    mentions_data.append({
                        'text_id': text_id,
                        'entity_id': entity_id,
                        'span_start': span_start,
                        'span_end': span_end,
                        'span': span
                    })

    return texts_data, entities_data, mentions_data


def save_dataframes_to_parquet(texts_data, entities_data, mentions_data, texts_file, entities_file, mentions_file):
    texts_df = pd.DataFrame(texts_data)
    entities_df = pd.DataFrame(entities_data)
    mentions_df = pd.DataFrame(mentions_data)

    texts_df.to_parquet(texts_file)
    entities_df.to_parquet(entities_file)
    mentions_df.to_parquet(mentions_file)


# Main function
def main():
    data = read_jsonl_file('test_seen_entities.jsonl')
    texts_data, entities_data, mentions_data = extract_data_from_jsonl(data)
    save_dataframes_to_parquet(texts_data, entities_data, mentions_data,
                               'test_seen_texts.parquet',
                               'test_seen_entities.parquet',
                               'test_seen_mentions.parquet')


if __name__ == '__main__':
    main()
