#!/usr/bin/env python3
"""
setup_neo4j.py
Initializes Neo4j schema for the Reddit → Graph project.

Creates:
- UNIQUE constraints (User, Post, Comment, Subreddit, Category, Keyword[optional])
- Helpful indexes (created_utc, iso_day, category, user.name)
- Seeds Category nodes: campus_life, academics, admissions

Env (.env or environment):
  NEO4J_URI=bolt://localhost:7687
  NEO4J_USER=neo4j
  NEO4J_PASSWORD=reddit123

Usage:
  python setup_neo4j.py
  python setup_neo4j.py --wipe   # ⚠️ deletes all data (dev only)
"""

import os
import sys
import argparse
from neo4j import GraphDatabase, basic_auth

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass  # dotenv is optional

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "reddit123")

CONSTRAINTS = [
    "CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
    "CREATE CONSTRAINT post_id IF NOT EXISTS FOR (p:Post) REQUIRE p.id IS UNIQUE",
    "CREATE CONSTRAINT comment_id IF NOT EXISTS FOR (c:Comment) REQUIRE c.id IS UNIQUE",
    "CREATE CONSTRAINT subreddit_name IF NOT EXISTS FOR (s:Subreddit) REQUIRE s.name IS UNIQUE",
    "CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE",
    # Keyword is optional in your schema; safe to create now for later use
    "CREATE CONSTRAINT keyword_name IF NOT EXISTS FOR (k:Keyword) REQUIRE k.name IS UNIQUE",
]

INDEXES = [
    "CREATE INDEX post_created IF NOT EXISTS FOR (p:Post) ON (p.created_utc)",
    "CREATE INDEX post_iso_day IF NOT EXISTS FOR (p:Post) ON (p.iso_day)",
    "CREATE INDEX post_category IF NOT EXISTS FOR (p:Post) ON (p.category)",
    "CREATE INDEX comment_created IF NOT EXISTS FOR (c:Comment) ON (c.created_utc)",
    "CREATE INDEX user_name IF NOT EXISTS FOR (u:User) ON (u.name)",
]

CATEGORIES = [
    ("campus_life", "Student life, events, hostel/food, facilities, clubs, sports, social activities"),
    ("academics", "Courses, exams, professors, assignments, grades, GPA, schedules"),
    ("admissions", "Applications, entrance tests, merit lists, scholarships, program selection"),
]


def run_batch(session, queries, label):
    print(f"▶ {label}…")
    for q in queries:
        session.run(q)
    print(f"✓ {label} done.\n")


def seed_categories(session):
    print("▶ Seeding Category nodes…")
    for name, desc in CATEGORIES:
        session.run(
            """
            MERGE (c:Category {name: $name})
            ON CREATE SET c.description = $desc
            ON MATCH  SET c.description = coalesce(c.description, $desc)
            """,
            name=name,
            desc=desc,
        )
    print("✓ Categories seeded.\n")


def wipe_all(session):
    print("⚠ Wiping ALL nodes + relationships (dev only)…")
    session.run("MATCH (n) DETACH DELETE n")
    print("✓ Database wiped.\n")


def verify(session):
    print("▶ Verifying schema and counts…\nConstraints:")
    for rec in session.run("SHOW CONSTRAINTS"):
        print(f"  - {rec.get('name')} :: {rec.get('type')} on {rec.get('entityType')} {rec.get('labelsOrTypes')}")
    print("\nIndexes:")
    for rec in session.run("SHOW INDEXES"):
        print(f"  - {rec.get('name')} :: {rec.get('type')} on {rec.get('labelsOrTypes')} {rec.get('properties')}")
    print("\nNode counts:")
    any_nodes = False
    for rec in session.run("MATCH (n) RETURN labels(n)[0] AS label, count(*) AS cnt ORDER BY cnt DESC"):
        any_nodes = True
        print(f"  - {rec['label']}: {rec['cnt']}")
    if not any_nodes:
        print("  (empty — load JSON next)")
    print("\nRelationship counts:")
    any_rels = False
    for rec in session.run("MATCH ()-[r]->() RETURN type(r) AS rel, count(*) AS cnt ORDER BY cnt DESC"):
        any_rels = True
        print(f"  - {rec['rel']}: {rec['cnt']}")
    if not any_rels:
        print("  (none yet — will appear after loading JSON)")
    print("\n✓ Verification complete.\n")


def main():
    parser = argparse.ArgumentParser(description="Initialize Neo4j schema for Reddit Graph.")
    parser.add_argument("--wipe", action="store_true", help="Delete ALL data before initializing (dev only).")
    args = parser.parse_args()

    print(f"Connecting to Neo4j @ {NEO4J_URI} as {NEO4J_USER} …")
    driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))

    try:
        with driver.session() as session:
            # quick ping
            ok = session.run("RETURN 1 AS ok").single()
            assert ok and ok["ok"] == 1, "Neo4j connection failed."

            if args.wipe:
                wipe_all(session)

            run_batch(session, CONSTRAINTS, "Creating constraints")
            run_batch(session, INDEXES, "Creating indexes")
            seed_categories(session)
            verify(session)

        print("🎉 Neo4j setup complete.")
        print("Next step: run your separate loader script to ingest users, subreddits, posts, and comments.")
    except Exception as e:
        print(f"❌ Setup failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
