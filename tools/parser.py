import json
from tqdm import tqdm
from markdownify import markdownify as md
from collections import defaultdict
from lxml import etree

# ['Id', 'PostTypeId', 'AcceptedAnswerId', 'CreationDate', 'Score', 'ViewCount', 'Body',
# 'OwnerUserId', 'OwnerDisplayName', 'LastEditorUserId', 'LastEditorDisplayName', 'LastEditDate',
# 'LastActivityDate', 'Title', 'Tags', 'AnswerCount', 'CommentCount', 'FavoriteCount', 'ContentLicense']

# ['Id', 'PostTypeId', 'ParentId', 'CreationDate', 'Score', 'Body', 'OwnerUserId', 'LastEditorUserId',
# 'LastEditorDisplayName', 'LastEditDate', 'LastActivityDate', 'CommentCount', 'CommunityOwnedDate', 'ContentLicense']

def safe_int(val, default=0):
    try:
        return int(val)
    except (ValueError, TypeError):
        return default

"""
questions = {
    Id: {Body: str,
         Title: str,
         Tags: list,
         AcceptedAnswerId: str}
}

rejected = {
    Id: {BestScore: int,
         Best: str,
         SecondScore: int,
         Second: str}
}
"""

questions = {}
answers = defaultdict(lambda: {'SecondScore': float('-inf'), 'Second': '',
                                'BestScore': float('-inf'), 'Best': ''})

rows_each_cat = 50000

context = etree.iterparse('data/Posts.xml', events=('end',), tag='row')
context = iter(context)
_, root = next(context)

print('Collecting:')
rel_tags = ('c', 'c++', 'c#', 'python', 'java', 'javascript', 'php', 'sql')
pbars = [tqdm(desc=tag.ljust(12),
              position=i,
              disable=False,
              bar_format='{l_bar} {n_fmt}') for i, tag in enumerate(rel_tags)]

for event, elem in tqdm(context):
    attrs = dict(elem.attrib)

    if attrs['PostTypeId'] == '1' and safe_int(attrs.get('AnswerCount')) >= 5:

        if all(pbar.n == rows_each_cat for pbar in pbars):
            continue

        tags = attrs.get('Tags', '').split('|')
        idx = [i for i, tag in enumerate(rel_tags) if tag in tags]

        if len(idx) > 0:
            if any(pbars[i].n >= rows_each_cat for i in idx):
                continue

            [pbar.update(1) for i, pbar in enumerate(pbars) if i in idx]

            questions[attrs['Id']] = {
                'Body': attrs.get('Body', ''),
                'Title': attrs.get('Title', ''),
                'Tags': tags,
            }

    elif attrs['PostTypeId'] == '2':
        parent_id = attrs['ParentId']

        score = attrs.get('Score')
        if not score:
            continue
        score = int(score)

        if score > answers[parent_id]['BestScore']:
            answers[parent_id]['BestScore'] = score
            answers[parent_id]['Best'] = attrs.get('Body', '')
        elif score > answers[parent_id]['SecondScore']:
            answers[parent_id]['SecondScore'] = score
            answers[parent_id]['Second'] = attrs.get('Body', '')

    elem.clear()
    root.clear()
    while elem.getprevious() is not None:
        del elem.getparent()[0]


del context

"""
Prompt
Chosen
Rejected
Title
Tags
"""

output_path = 'data/dataset.json'
with open(output_path, 'w', encoding='utf-8') as f:
    for id, data in tqdm(questions.items(), desc='Writing'):
        try:
            if not answers[id]['Best'].strip() or not answers[id]['Second'].strip():
                continue

            part = {
                'Prompt': md(data['Body'], heading_style="ATX"),
                'Chosen': md(answers[id]['Best'], heading_style="ATX"),
                'Rejected': md(answers[id]['Second'], heading_style="ATX"),
                'Title': data['Title'],
                'Tags': data['Tags']
            }
            f.write(json.dumps(part, ensure_ascii=False) + '\n')

        except Exception as e:
            print(f'Exception during writing: {e}')

    print(f"Successfully saved into {output_path}")
    print(f"Total rows: {len(questions)}")


