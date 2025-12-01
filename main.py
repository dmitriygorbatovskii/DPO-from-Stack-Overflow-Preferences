import requests
import time
import json
from tqdm import tqdm


def top_k_questions(params, k=5):
    API_URL = "https://api.stackexchange.com/2.3/questions"
    all_questions = []

    with tqdm(total=k*params["pagesize"]) as pbar:
      for page in range(1, k+1):
          params["page"] = (page - 1) * params["pagesize"] + 1

          res = requests.get(API_URL,
                            params=params,
                            timeout=10)
          data = res.json()
          items = data.get("items", [])

          if not items:
              break

          all_questions.extend(items)
          time.sleep(0.15)
          pbar.update(params["pagesize"])

    return all_questions

def get_answers(question):
  qid = question['question_id']
  url = f"https://api.stackexchange.com/2.3/questions/{qid}/answers"
  params = {"site": "stackoverflow", "filter": "withbody"}

  res = requests.get(url, params=params, timeout=10)
  print(res)
  res = res.json().get('items', [])
  time.sleep(2.5)

  if res:
      chosen = res[0]
      rejected = res[-1]

      if chosen["score"] / 2 >= rejected["score"]:

        chosen = md(chosen["body"], heading_style="ATX")
        rejected = md(rejected["body"], heading_style="ATX")
        prompt = md(question["body"], heading_style="ATX")

        return {
                'prompt': prompt,
                'chosen': chosen,
                'rejected': rejected}
  return

if __name__ == '__main__':
    params = {
        "tagged": ["python"],
        "sort": "votes",
        "order": "desc",
        "site": "stackoverflow",
        "filter": "withbody",
        "pagesize": 10,  # максимум за раз
        "key": ""
    }
    

    batch_size = 10
    dataset = []
    
    questions = top_k_questions(k=100)
    
    dataset = []
    for question in tqdm(questions):
        dpo_data = get_answers(question)
        if dpo_data:
            dataset.append(dpo_data)

    with open("so_python_questions_top.json", "w", encoding="utf-8") as f:
        json.dump(dataset, f, ensure_ascii=False, indent=2)












