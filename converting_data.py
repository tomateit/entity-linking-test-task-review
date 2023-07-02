import json
from pathlib import Path
from dataclasses import dataclass, asdict

import pandas as pd


@dataclass(frozen=True)
class InputSchema:
    context: str
    mention2entity: dict[str, list[str]]
    entity2type: dict[str, list[str]]
    id: int


@dataclass(frozen=True)
class EntitySchema:
    qid: str


@dataclass(frozen=True)
class MentionSchema:
    text_id: int
    entity_id: str
    span_start: int
    span_end: int
    span: str


@dataclass(frozen=True)
class TextSchema:
    original_text: str
    id: int


def read_jsonl_file(file_path: Path) -> list[InputSchema]:
    data = []
    with open(file_path, "r") as file:
        for idx, line in enumerate(file):
            datum = json.loads(line)
            datum = InputSchema(**datum, id=idx)
            data.append(datum)
    return data


def process_entry(entry: InputSchema) -> tuple[set[EntitySchema], list[MentionSchema]]:
    context = entry.context
    mention2entity = entry.mention2entity

    entities = set()
    mentions = []

    for mention, (entity_qid, *extra_entities) in mention2entity.items():
        if len(extra_entities) > 0:
            print(extra_entities)

        entity = EntitySchema(qid=entity_qid)
        entities.add(entity)

        if mention not in context:
            raise ValueErrord(f"Mention {mention} is not a substring of `{context}`")

        span_start = context.index(mention)
        span_end = span_start + len(mention)
        span = mention

        new_mention = MentionSchema(
            text_id=entry.id,
            entity_id=entity_qid,
            span_start=span_start,
            span_end=span_end,
            span=span
        )

        mentions.append(new_mention)
    
    return entities, mentions


def extract_entities_from_input_chunks(data: list[InputSchema]):
    texts_buffer = []
    entities_buffer = []
    mentions_buffer = []

    for entry in data:
        entities, mentions = process_entry(entry)
        text = TextSchema(
            original_text=entry.context,
            id=entry.id
        )

        texts_buffer.append(text)
        entities_buffer.extend(entities)
        mentions_buffer.extend(mentions)

    return texts_buffer, entities_buffer, mentions_buffer


# Main function
def main():
    # 1. Read in the data
    data: list[InputSchema] = read_jsonl_file("test_seen_entities.jsonl")
    
    # 2. Transform and extract entities
    texts_data, entities_data, mentions_data = extract_data_from_jsonl(data)

    # 3. Save texts as a dataframe
    texts_path = Path("./test_seen_texts.parquet").resolve()
    assert not texts_path.exists(), f"The output path contains a file already: {texts_path}"
    texts_dataframe = pd.DataFrame([asdict(d) for d in texts_data])
    texts_dataframe.to_parquet(texts_path)

    # 4. Save entities as a dataframe
    entities_path = Path("./test_seen_entities.parquet").resolve()
    assert not entities_path.exists(), f"The output path contains a file already: {entities_path}"
    entities_dataframe = pd.DataFrame([asdict(d) for d in entities_data])
    entities_dataframe.to_parquet(entities_path)

    # 5. Save mentions as a dataframe
    mentions_path = Path("./test_seen_mentions.parquet").resolve()
    assert not mentions_path.exists(), f"The output path contains a file already: {mentions_path}"
    mentions_dataframe = pd.DataFrame([asdict(d) for d in mentions_data])
    mentions_dataframe.to_parquet(mentions_path)

    print("Done")


if __name__ == '__main__':
    main()
