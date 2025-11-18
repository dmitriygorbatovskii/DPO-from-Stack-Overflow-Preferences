import requests
import time
import json
from tqdm import tqdm


def top_k_questions(k=5):
    API_URL = "https://api.stackexchange.com/2.3/questions"

    params = {
        "tagged": "python",
        "sort": "votes",
        "order": "desc",
        "site": "stackoverflow",
        "pagesize": 1,  # максимум за раз
    }

    all_questions = []

    for page in tqdm(range(1, k+1)):
        params["page"] = page

        res = requests.get(API_URL, params=params, timeout=10)
        data = res.json()

        items = data.get("items", [])

        if not items:
            break

        all_questions.extend(items)
        time.sleep(0.15)

    return all_questions

def get_answers(questions):
    ids = ';'.join([str(question['question_id']) for question in questions])
    url = f"https://api.stackexchange.com/2.3/questions/{ids}/answers"

    params = {
        "sort": "votes",
        "order": "desc",
        "site": "stackoverflow",
        "filter": "!nNPvSNdWme",
    }

    res = requests.get(url, params=params, timeout=10)
    data = res.json()

    return data

if __name__ == '__main__':
    max_questions = 500
    batch_size = 10
    dataset = []

    questions = top_k_questions(k=max_questions)

    for i in tqdm(range(0, max_questions, batch_size)):
        batch = questions[i:i+batch_size]

        answers = None
        for j in range(10):
            try:
                answers = get_answers(batch)['items']
                break
            except Exception as e:
                print(e)
                time.sleep(0.5)
        if answers is None:
            continue

        for question in batch:
            id = question['question_id']
            answers_list = list(filter(lambda answer: answer.get('question_id') == id, answers))

            data_part = {'content':  question['title'],
                         'chosen': answers_list[0]['body'],
                         'rejected': answers_list[-1]['body']}
            dataset.append(data_part)

    with open("so_python_questions_top.json", "w", encoding="utf-8") as f:
        json.dump(dataset, f, ensure_ascii=False, indent=2)












