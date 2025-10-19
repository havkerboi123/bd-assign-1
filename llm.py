from openai import OpenAI
from pydantic import BaseModel
import json
import os
from pathlib import Path
import time

from dotenv import load_dotenv
load_dotenv()

client = OpenAI(
    api_key=os.getenv("GROQ_API_KEY"),
    base_url="https://api.groq.com/openai/v1",
)

class PostAnalysis(BaseModel):
    category: str  # One of: campus_life, academics, admissions
    sentiment: str  # One of: positive, negative, neutral
    keywords: list[str]  # Top 3-5 relevant keywords
    description: str  # Brief explanation of categorization


DATA_DIR = Path("./data")
INPUT_FILE = DATA_DIR / "posts.jsonl"
OUTPUT_FILE = DATA_DIR / "posts_enriched.jsonl"

SYSTEM_PROMPT = """You are analyzing posts from Pakistani university subreddits (GIKI, NUST, LUMS).

Your task: Categorize each post into exactly ONE category and analyze sentiment.

CATEGORIES (choose ONE):
1. campus_life: Student life, hostel/dorm issues, mess/cafeteria food, campus facilities, events, clubs, societies, sports, social activities, transportation, general campus complaints
2. academics: Courses, lectures, professors/instructors, exams, quizzes, assignments, projects, grades, GPA, study help, course registration, academic schedules, degree programs
3. admissions: University applications, entrance tests (NTS, ECAT, SAT), merit lists, admission results, scholarships, financial aid, program selection advice (which university/major to choose), admission requirements

SENTIMENT ANALYSIS:
- positive: Happy, satisfied, grateful, supportive, encouraging tone
- negative: Frustrated, angry, disappointed, critical, complaining
- neutral: Factual information, questions without strong emotion, objective statements

KEYWORDS: Extract 3-5 most relevant terms that capture the main topic (e.g., "exams", "merit list", "hostel food", "scholarship")

Return ONLY valid JSON with these fields:
{
  "category": "campus_life" | "academics" | "admissions",
  "sentiment": "positive" | "negative" | "neutral",
  "keywords": ["keyword1", "keyword2", ...],
  "description": "brief reason for categorization"
}"""


def analyze_post(title: str, body: str, post_id: str) -> dict:
    """Analyze a single post with LLM"""
    try:
        # Combine title and body for analysis
        content = f"Title: {title}\nBody: {body if body else '[No body text]'}"

        response = client.responses.parse(
            model="openai/gpt-oss-20b",
            input=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Analyse the post content: {content}"},
            ],
            text_format=PostAnalysis,
        )
        
        # The response is already a Pydantic model, access fields directly
        parsed = response.output_parsed
        print(f"Parsed: {parsed}")
        
        # Convert Pydantic model to dict
        analysis = {
            "category": parsed.category,
            "sentiment": parsed.sentiment,
            "keywords": parsed.keywords,
            "description": parsed.description
        }
        
        # Validate category
        valid_categories = ["campus_life", "academics", "admissions"]
        if analysis["category"] not in valid_categories:
            print(f"⚠️  Invalid category '{analysis['category']}' for post {post_id}, defaulting to 'campus_life'")
            analysis["category"] = "campus_life"
        
        # Validate sentiment
        valid_sentiments = ["positive", "negative", "neutral"]
        if analysis["sentiment"] not in valid_sentiments:
            print(f"⚠️  Invalid sentiment '{analysis['sentiment']}' for post {post_id}, defaulting to 'neutral'")
            analysis["sentiment"] = "neutral"
        
        # Ensure keywords is a list
        if not isinstance(analysis["keywords"], list):
            analysis["keywords"] = []
        
        return analysis
        
    except Exception as e:
        print(f"❌ Error analyzing post {post_id}: {e}")
        # Return default values instead of 0
        return {
            "category": "campus_life",
            "sentiment": "neutral",
            "keywords": [],
            "description": "Error during analysis"
        }


def enrich_posts():
    """Read posts.jsonl, enrich with LLM, save to posts_enriched.jsonl"""
    
    if not INPUT_FILE.exists():
        print(f"❌ Input file not found: {INPUT_FILE}")
        return
    
    # Read all posts
    posts = []
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                posts.append(json.loads(line))
    
    print(f"📊 Found {len(posts)} posts to analyze")
    
    # Clear output file if exists
    if OUTPUT_FILE.exists():
        OUTPUT_FILE.unlink()
    
    # Process each post
    enriched_count = 0
    failed_count = 0
    
    for i, post in enumerate(posts, 1):
        post_id = post.get("id", "unknown")
        title = post.get("title", "")
        body = post.get("selftext", "")
        
        print(f"\n[{i}/{len(posts)}] Analyzing post {post_id}: {title[:50]}...")
        
        # Get LLM analysis
        analysis = analyze_post(title, body, post_id)
        
        # Add analysis fields to post
        post["category"] = analysis["category"]
        post["sentiment"] = analysis["sentiment"]
        post["keywords"] = analysis["keywords"]
        
        if "description" in analysis:
            post["llm_description"] = analysis["description"]
        
        # Write enriched post to output file
        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(post, ensure_ascii=False) + "\n")
        
        if analysis.get("description") != "Error during analysis":
            enriched_count += 1
            print(f"✅ Category: {analysis['category']}, Sentiment: {analysis['sentiment']}")
            print(f"   Keywords: {', '.join(analysis['keywords'][:3])}")
        else:
            print(f"⚠️  Using default analysis")
            failed_count += 1
        
        # Rate limiting - be nice to Groq API
        if i < len(posts):
            time.sleep(1)  # 1 second between requests
    
    print(f"\n{'='*60}")
    print(f"✅ Enrichment complete!")
    print(f"   Total posts: {len(posts)}")
    print(f"   Successfully analyzed: {enriched_count}")
    print(f"   Failed/defaults: {failed_count}")
    print(f"   Output saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    enrich_posts()