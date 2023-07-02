import pytest
import os
from converting_data import (
    InputSchema, EntitySchema, MentionSchema, TextSchema,
    extract_entities_from_input_chunks
)

text_blob = ("An alternative approach to the comparison of Buddhist thought with Western philosophy is "
                "to use the concept of the Middle Way in Buddhism as a critical tool for the assessment of "
                "Western philosophies. In this way Western philosophies can be classified in Buddhist "
                "terms as eternalist or nihilist. In a Buddhist view all philosophies are considered "
                "non-essential views (ditthis) and not to be clung to.")

test_input = InputSchema(
    id=25635,
    context=text_blob,
    mention2entity={"Middle Way": ["Q833475"]},
    entity2type={"Q833475": ["Q23847174"]},
)

test_output_mention = MentionSchema(
    text_id=25635,
    span="Middle Way",
    span_start=115,
    span_end=125,
    entity_id="Q833475",
)

test_output_text = TextSchema(
    original_text=text_blob,
    id=25635,
)

test_output_entity = EntitySchema(
    qid="Q833475"
)


class TestDatasetTransform:
    def test_extract_data(self):
        texts_data, entities_data, mentions_data = extract_entities_from_input_chunks([test_input])

        assert len(texts_data) == 1
        assert len(entities_data) == 1
        assert len(mentions_data) == 1

        assert texts_data[0] == test_output_text
        assert entities_data[0] == test_output_entity
        assert mentions_data[0] == test_output_mention
