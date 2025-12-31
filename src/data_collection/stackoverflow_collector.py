import requests
import pandas as pd
import time
import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from typing import Any, Optional
try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautfiulSoup = None

def load_env_from_parents(env_filename: str=".env") -> None:
    current = Path(__file__).resolve()
    for parent in [current.parent, *current.parents]:
        candidate = parent / env_filename
        if candidate.exists():
            load_dotenv(dotenv_path=candidate)
            return

load_env_from_parents()

api_key = os.getenv("STACKEXCHANGE_API_KEY")

JSONDict = dict[str, Any]

class StackOverflowCollector:
    """
    Collect Q&A pairs from Stack Overflow via API

    Strategy (quality + correctness):
    1) Use /search/advanced to find high-quality question IDs (accepted answers, high score, tags).
    2) Batch-fetch question bodies with /questions/{ids}?filter=withbdy.
    3) Batch-fetch answer bodies with /questions/{ids}/answers?filter=withbody.
    4) Keep ONLY the accepted answer for each question.
    """
    SEARCH_ADVANCED_ENDPOINT = "/search/advacned"
    QUESTIONS_BY_IDS_ENDPOINT = "/questions/{ids}"
    ANSWERS_FOR_QUESTIONS_ENDPOINT = "/questions/{ids}/answers"

    MAX_IDS_PER_REQUEST = 100
    MAX_RETRIES = 3
    DEFAULT_TIMEOUT_SEC = 30

    def __init__(self, 
                 api_key: Optional[str] = None, 
                 site: str="stackoverflow", 
                 base_url: str="https://api.stackexchange.com/2.3",
                 min_request_delay_sec: float=0.2,
                 print_quota: bool=False
    ):
        """
        Initialize the Stack Exchange API client
        """
        self.base_url = base_url.rstrip("/")
        self.site = site
        self.api_key = api_key
        self.min_request_delay_sec = 0.2
        self.min_request_delay_sec = float(min_request_delay_sec)
        self.session = requests.Session()

    def get_questions(
            self,
            tagged: Optional[list[str]] = None,
            min_score: int = 10,
            require_accepted: bool = True,
            min_answers: int = 1,
            page_size: int = 100,
            max_pages: int = 5,
    ) -> list[JSONDict]:
        question_ids = self._search_question_ids(
            tagged=tagged,
            min_score=min_score,
            require_accepted=require_accepted,
            min_aswers=min_answers,
            page_size=page_size,
            max_pages=max_pages,
        )
        if not question_ids:
            return []
        
        questions_by_id = self._fetch_questions_by_ids(question_ids)
        if not questions_by_id:
            return []
        
        answers_by_qid = self._fetch_answers_for_questions(question_ids)

        qa_pairs: list[JSONDict] = []
        for qid, question in questions_by_id.items():
            accepted_answer = self._find_accepted_answer(question, answers_by_qid.get(qid, []))
            if accepted_answer is None:
                continue

            qa_pairs.append(self._to_qa_pair(question, accepted_answer))

        return qa_pairs
    
    def collect_multiple_topics(
            self,
            topic_configs: list[JSONDict],
            output_dir: str = "data/raw",
    ) -> pd.DataFrame:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        all_rows: list[JSONDict] = []

        for cfg in topic_configs:
            name = cfg["name"]
            tags = cfg.get("tags")
            min_score = int(cfg.get("min_score", 10))
            min_answers = int(cfg.get("min_answers", 1))
            max_pages = int(cfg.get("max_pages", 5))

            rows = self.get_questions(
                tagged=tags,
                min_score=min_score,
                require_accepted=min_answers,
                page_size=100,
                max_pages=max_pages,
            )

            for r in rows:
                r["topic"] = name
            
            all_rows.extend(rows)

            topic_df = pd.DataFrame(rows)
            topic_file = output_path / f"stackoverflow_{name}.csv"
            topic_df.to_csv(topic_file, index=False)

            time.sleep(1)

        combined_df = pd.DataFrame(all_rows)
        combined_file = output_path / "stackoverflow_combined.csv"
        combined_df.to_csv(combined_file, index=False)

        return combined_df

    
    def _search_question_ids(
            self,
            tagged: Optional[list[str]],
            min_score: int,
            require_accepted: bool,
            min_answers: int,
            page_size: int,
            max_pages: int,
    ) -> list[int]:
        params: JSONDict = {
            "order": "desc",
            "sort": "votes",
            "pagesize": min(int(page_size), 100),
            "page": 1,
            "min": int(min_score),
            "answers": int(min_answers),
            "closed": "false",
            "migrated": "false",
        }

        if tagged:
            params["tagged"] = ";".join(tagged)

        if require_accepted:
            params["accepted"] = "true"

        ids: list[int] = []

        for page in range(1, int(max_pages) + 1):
            params["page"] = page
            data = self._request(self.SEARCH_ADVANCED_ENDPOINT, params)

            for item in data.get("items", []):
                qid = item.get("question_id")
                if qid is not None:
                    ids.append(int(qid))

            if not data.get("has_more", False):
                break

        return self._dedupe_preserve_order(ids)
    
    def _fetch_questions_by_ids(self, question_ids: list[int]) -> dict[int, JSONDict]:
        questions_by_id: dict[int, JSONDict] = {}

        for batch in self._chunks(question_ids, self.MAX_IDS_PER_REQUEST):
            endpoint = self.QUESTIONS_BY_IDS_ENDPOINT.format(ids=self._join_ids(batch))
            params: JSONDict = {"filter": "withbody"}

            data = self._request(endpoint, params)
            for q in data.get("items", []):
                qid = int(q["question_id"])
                questions_by_id[qid] = q

        return questions_by_id
    
    def _fetch_answers_for_questions(self, question_ids: list[int]) -> dict[int, list[JSONDict]]:
        answers_by_qid: dict[int, list[JSONDict]] = {}

        for batch in self._chunks(question_ids, self.MAX_IDS_PER_REQUEST):
            endpoint = self.ANSWERS_FOR_QUESTIONS_ENDPOINT.format(ids=self._join_ids(batch))
            params: JSONDict = {
                "order": "desc",
                "sort": "votes",
                "filter": "withbody",
                "pagesize": 100,
            }
            
        data = self._request(endpoint, params)
        for ans in data.get("items", []):
            qid = int(ans["question_id"])
            answers_by_qid.setdefault(qid, []).append(ans)

        return answers_by_qid
    
    def _find_accepted_answer(self, question: JSONDict, answers: list[JSONDict]) -> [JSONDict]:
        """
        Returns the accepted answer JSON object for a question, or None
        """
        accepted_id = question.get("accepted_answer_id")
        if not accepted_id:
            return None
        
        for ans in answers:
            if ans.get("is_accepted") is True:
                return ans
            
        for ans in answers:
            if ans.get("answer_id") == accepted_id:
                return ans
        
        return None
    
    def _request(self, endpoint: str, params: JSONDict) -> JSONDict:
        url = f"{self.base_url}{endpoint}"
        
        full_params = self._with_standard_params(params)

        time.sleep(self.min_request_delay_sec)

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                resp = self.session.get(url, params=full_params, timeout=self.DEFAULT_TIMEOUT_SEC)
                resp.raise_for_status()
                data: JSONDict = resp.json()

                self._raise_if_api_error(data, attempt)

                self._sleep_if_backoff_requested(data)

                if self.print_quota:
                    self._print_quota_if_present(data)

                return data
            
            except (requests.RequestException, ValueError):
                if attempt == self.MAX_RETRIES:
                    raise
                time.sleep(2 * attempt)

            return {}
        
    def _with_standard_params(self, params: JSONDict) -> JSONDict:
        merged = {"site": self.site, **params}
        if self.api_key:
            merged["key"] = self.api_key
        return merged
        
    def _raise_if_api_error(self, data: JSONDict, attempt: int) -> None:
        if "error_name" not in data:
            return
        
        name = data.get("error_name", "")
        msg = data.get("error_message", "")

        if name == "throttle_violation":
            # if there is still a throttle violation then we will back off more aggresively for each attempt
            time.sleep(10 * attempt)
            raise requests.RequestException(f"Throttle violation: {msg}")
        
        raise RunTimeError(f"StackExchange API error: {name} - {msg}")
    
    def _sleep_if_backoff_requested(self, data: JSONDict) -> None:
        """
        In case StackExchange includes a 'backoff' field requiring to wait n secs
        """
        backoff = data.get("backoff")
        if backoff:
            time.sleep(int(backoff))

    def _print_quota_if_present(self, data: JSONDict) -> None:
        qrem = data.get("quota_remaining")
        qmax = data.get("quota_max")
        if qrem is not None and qmax is not None:
            print(f"Quota: {qrem}/{qmax} remaining")

    def _to_qa_pair(self, question: JSONDict, answer: JSONDict) -> JSONDict:
        """
        Convert raw API objects into a clean, flat dict for CSV/pandas
        """
        q_body_html = question.get("body", "") or ""
        a_body_html = answer.get("body", "") or ""

        return {
            "question_id": question.get("question_id"),
            "question_title": question.get("title", "") or "",
            "questions_score": int(question.get("score", 0) or 0),
            "questions_tag": question.get("tags", []) or [],
            "question_url": question.get("tags", []) or [],
            "question_url": question.get("linl", "") or "",
            "question_created_utc": self._ts_to_iso(question.get("creation_data")),
            "view_count": question.get("view_count"),

            "accepted_answer_id": question.get("accepted_answer_id"),
            "answer_id": answer.get("answer_id"),
            "answer_score": int(answer.get("score", 0) or 0),
            "answer_created_utc": self._ts_to_iso(answer.get("creation_date")),

            "question_body_html": q_body_html,
            "answer_body_html": a_body_html,
            "question_body_text": self._html_to_text(q_body_html),
            "answer_body_text": self._html_to_text(a_body_html),
        }
    
    def _ts_to_iso(self, unix_ts: Any) -> str:
        if unix_ts is None:
            return None
        return datetime.fromtimestamp(int(unix_ts), tz=timezone.utc).isoformat()
    
    def _html_to_text(self, html: str) -> str:
        if not html:
            return ""
        
        if BeautifulSoup is None:
            return html
        
        soup = BeautifulSoup(html, "html.parser")
        return soup.get_text(" ", strip=True)
    
    @staticmethod
    def _dedupe_preserve_order(values: list[int]) -> list[int]:
        seen: set[int] = set()
        out: list[int] = []
        for v in values:
            if v not in seen:
                seen.add(v)
                out.append(v)
        return out
    
    @staticmethod
    def _chunks(values: list[int], size: int):
        for i in range(0, len(values), size):
            yield values[i : i + size]

    @staticmethod
    def _join_ids(ids: list[int]) -> str:
        return ";".join(str(i) for i in ids)
            