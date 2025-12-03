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


ROWS_EACH_CAT = 50_000
MIN_SCORE = 0
MIN_ANSWERS = 3

def safe_int(val, default=0):
    try:
        return int(val)
    except (ValueError, TypeError):
        return default

def get_questions(xml_path, questions):
    context = etree.iterparse(xml_path, events=('end',), tag='row', recover=True)
    context = iter(context)

    print('\nCollecting questions:')
    rel_tags = ('c++', 'c#', 'python', 'java', 'javascript', 'php', 'sql')
    pbars = [tqdm(desc=tag.ljust(12),
                  position=i,
                  disable=False,
                  bar_format='{l_bar} {n_fmt}') for i, tag in enumerate(rel_tags)]

    with tqdm() as total_pbar:
        for event, elem in context:
            attrs = dict(elem.attrib)

            if attrs['PostTypeId'] == '1':
                if safe_int(attrs.get('AnswerCount')) < MIN_ANSWERS:
                    continue
                if safe_int(attrs.get('Score')) < MIN_SCORE:
                    continue
                accepted_id = attrs.get('AcceptedAnswerId')
                if not accepted_id  or accepted_id in ('0', '-1'):
                    continue

                tags = attrs.get('Tags', '').split('|')
                idx = [i for i, tag in enumerate(rel_tags) if tag in tags]

                if len(idx) == 1:
                    if any(pbars[i].n >= ROWS_EACH_CAT for i in idx):
                        continue

                    [pbar.update(1) for i, pbar in enumerate(pbars) if i in idx]

                    questions[attrs['Id']] = {
                        'Body': attrs.get('Body', ''),
                        'Title': attrs.get('Title', ''),
                        'Tags': tags,
                        'AcceptedAnswerId': accepted_id
                    }

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

            total_pbar.update(1)

            if all(pbar.n == ROWS_EACH_CAT for pbar in pbars):
                break

    del context

    return questions

def get_answers(xml_path, questions, answers):
    context = etree.iterparse(xml_path, events=('end',), tag='row', recover=True)
    context = iter(context)

    print('\nCollecting answers:')

    for event, elem in tqdm(context):
        attrs = dict(elem.attrib)

        if attrs.get('PostTypeId', '') == '2':
            parent_id = attrs.get('ParentId')

            if not parent_id in questions:
                continue

            score = safe_int(attrs.get('Score'))

            if attrs.get('Id') == questions[parent_id]['AcceptedAnswerId']:
                answers[parent_id]['Accepted'] = attrs.get('Body', '')

            elif score > answers[parent_id]['SecondScore']:
                answers[parent_id]['SecondScore'] = score
                answers[parent_id]['Second'] = attrs.get('Body', '')

        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    del context

    return answers

def writer(output_path, questions, answers):
    """
    Prompt
    Chosen
    Rejected
    Title
    Tags
    """

    print(f'\nWriting into {output_path}')

    with open(output_path, 'w', encoding='utf-8') as f:
        for id, data in tqdm(questions.items(), desc='Writing'):

            try:
                if not answers[id]['Accepted'].strip() or not answers[id]['Second'].strip():
                    continue

                part = {
                    'Prompt': md(data['Body'], heading_style="ATX"),
                    'Chosen': md(answers[id]['Accepted'], heading_style="ATX"),
                    'Rejected': md(answers[id]['Second'], heading_style="ATX"),
                    'Title': data['Title'],
                    'Tags': data['Tags']
                }
                f.write(json.dumps(part, ensure_ascii=False) + '\n')

            except Exception as e:
                print(f'Exception during writing: {e}')

        print(f"\nSuccessfully saved into {output_path}")
        print(f"Total rows: {len(questions)}")

if __name__ == '__main__':
    xml_path = 'data/Posts.xml'
    output_path = 'data/dataset.json'

    questions = defaultdict(lambda: {'Body': '', 'Title': '', 'Tags': [], 'AcceptedAnswerId': ''})
    answers = defaultdict(lambda: {'SecondScore': float('-inf'), 'Second': '', 'Accepted': ''})

    questions = get_questions(xml_path, questions)
    print(len(questions.keys()))

    answers = get_answers(xml_path, questions, answers)
    print(len(answers.keys()))

    writer(output_path, questions, answers)
