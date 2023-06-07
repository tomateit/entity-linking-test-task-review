from converting_data import extract_data_from_jsonl, save_dataframes_to_parquet

data = [
    {
        "context": "An alternative approach to the comparison of Buddhist thought with Western philosophy is "
                   "to use the concept of the Middle Way in Buddhism as a critical tool for the assessment of "
                   "Western philosophies. In this way Western philosophies can be classified in Buddhist "
                   "terms as eternalist or nihilist. In a Buddhist view all philosophies are considered "
                   "non-essential views (ditthis) and not to be clung to.",
        "mention2entity": {"Middle Way": ["Q833475"]},
        "entity2type": {"Q833475": ["Q23847174"]}

    }
]
texts_data, entities_data, mentions_data = extract_data_from_jsonl(data)

print(texts_data)
print(entities_data)
print(mentions_data)
