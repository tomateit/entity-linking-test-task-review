import os
import unittest
from converting_data import extract_data_from_jsonl, save_dataframes_to_parquet


class TestDataProcessing(unittest.TestCase):

    def test_extract_data_from_jsonl(self):
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

        self.assertEqual(len(texts_data), 1)
        self.assertEqual(len(entities_data), 1)
        self.assertEqual(len(mentions_data), 1)

    def test_save_dataframes_to_parquet(self):
        texts_data = [{'original_text': 'UwU UwU UwU', 'id': 1}]
        entities_data = [{'qid': '1'}, {'qid': '2'}]
        mentions_data = [{'text_id': 1, 'entity_id': '1', 'span_start': 0, 'span_end': 5, 'span': 'Hello'},
                         {'text_id': 1, 'entity_id': '2', 'span_start': 6, 'span_end': 11, 'span': 'world'}]

        texts_file = 'unit_test_texts.parquet'
        entities_file = 'unit_test_entities.parquet'
        mentions_file = 'unit_test_mentions.parquet'

        save_dataframes_to_parquet(texts_data, entities_data, mentions_data, texts_file, entities_file, mentions_file)

        self.assertTrue(os.path.exists(texts_file))
        self.assertTrue(os.path.exists(entities_file))
        self.assertTrue(os.path.exists(mentions_file))

        # Perform further checks if necessary, such as reading and comparing the contents of the Parquet files


if __name__ == '__main__':
    unittest.main()
