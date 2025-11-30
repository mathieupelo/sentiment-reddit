#!/usr/bin/env python3
"""
Insert keywords for QUEST universe companies into varrock.company_keywords table.

This script:
1. Fetches companies from QUEST universe (id 491)
2. Inserts 5-10 relevant keywords per company with priorities 1-10
3. Keywords are designed to be unique to avoid false matches
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv
import psycopg

# Load environment variables
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path, override=True)
else:
    env_path = Path(__file__).parent.parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path, override=True)
    else:
        load_dotenv()

# Keywords for each company (ticker -> list of (keyword, priority))
# Priority 1 = highest priority (most important)
# Based on web research and company knowledge
KEYWORDS_BY_TICKER = {
    "BILI": [
        ("Bilibili", 1),
        ("B站", 2),
        ("哔哩哔哩", 3),
        ("bilibili video", 4),
        ("bilibili anime", 5),
        ("bilibili manga", 6),
        ("bilibili games", 7),
    ],
    "0434.HK": [
        ("Boyaa Interactive", 1),
        ("Boyaa Games", 2),
        ("博雅互动", 3),
        ("Boyaa poker", 4),
        ("Boyaa mobile games", 5),
    ],
    "9697.T": [
        ("Capcom", 1),
        ("Resident Evil", 2),
        ("Street Fighter", 3),
        ("Monster Hunter", 4),
        ("Devil May Cry", 5),
        ("Mega Man", 6),
        ("Dead Rising", 7),
    ],
    "CDR.WA": [
        ("CD Projekt", 1),
        ("Witcher", 2),
        ("Cyberpunk 2077", 3),
        ("GOG", 4),
        ("CDPR", 5),
        ("GOG.com", 6),
    ],
    "078340.KQ": [
        ("Com2uS", 1),
        ("Summoners War", 2),
        ("Com2us", 3),
        ("Summoners War Sky Arena", 4),
        ("Com2uS games", 5),
    ],
    "CRSR": [
        ("Corsair Gaming", 1),
        ("Corsair keyboard", 2),
        ("Corsair mouse", 3),
        ("Corsair headset", 4),
        ("Corsair RGB", 5),
        ("Corsair iCUE", 6),
    ],
    "2432.T": [
        ("DeNA", 1),
        ("DeNA Co", 2),
        ("Mobage", 3),
        ("DeNA games", 4),
        ("DeNA mobile", 5),
    ],
    "194480.KQ": [
        ("Devsisters", 1),
        ("Cookie Run", 2),
        ("Cookie Run Kingdom", 3),
        ("Cookie Run OvenBreak", 4),
        ("Devsisters games", 5),
    ],
    "DOYU": [
        ("DouYu", 1),
        ("斗鱼", 2),
        ("DouYu直播", 3),
        ("DouYu streaming", 4),
        ("DouYu platform", 5),
    ],
    "EA": [
        ("Electronic Arts", 1),
        ("FIFA", 2),
        ("Madden", 3),
        ("Apex Legends", 4),
        ("Battlefield", 5),
        ("The Sims", 6),
        ("EA Sports", 7),
    ],
    "EMBRAC-B.ST": [
        ("Embracer Group", 1),
        ("THQ Nordic", 2),
        ("Deep Silver", 3),
        ("Gearbox", 4),
        ("Crystal Dynamics", 5),
        ("Eidos", 6),
    ],
    "6180.TWO": [
        ("Gamania", 1),
        ("橘子", 2),
        ("Gamania Digital", 3),
        ("Gamania games", 4),
        ("橘子游戏", 5),
    ],
    "3903.T": [
        ("gumi", 1),
        ("gumi Inc", 2),
        ("Brave Frontier", 3),
        ("gumi games", 4),
        ("Final Fantasy Brave Exvius", 5),
    ],
    "3765.T": [
        ("GungHo", 1),
        ("GungHo Online", 2),
        ("Puzzle & Dragons", 3),
        ("GungHo Entertainment", 4),
        ("PAD", 5),
    ],
    "HUYA": [
        ("HUYA", 1),
        ("虎牙", 2),
        ("HUYA直播", 3),
        ("HUYA streaming", 4),
        ("HUYA platform", 5),
    ],
    "1119.HK": [
        ("iDreamSky", 1),
        ("创梦天地", 2),
        ("IDREAMSKY", 3),
        ("iDreamSky games", 4),
        ("创梦游戏", 5),
    ],
    "IMMR": [
        ("Immersion", 1),
        ("Immersion Corporation", 2),
        ("haptic technology", 3),
        ("haptic feedback", 4),
        ("TouchSense", 5),
    ],
    "3293.TWO": [
        ("International Games System", 1),
        ("IGS", 2),
        ("鈊象電子", 3),
        ("IGS games", 4),
        ("IGS slot machines", 5),
    ],
    "293490.KQ": [
        ("Kakao Games", 1),
        ("Kakao Games Corp", 2),
        ("PUBG Mobile", 3),
        ("Kakao Games mobile", 4),
        ("KakaoTalk games", 5),
    ],
    "3888.HK": [
        ("Kingsoft", 1),
        ("金山软件", 2),
        ("Kingsoft Office", 3),
        ("WPS Office", 4),
        ("Kingsoft Cloud", 5),
    ],
    "3635.T": [
        ("Koei Tecmo", 1),
        ("Nioh", 2),
        ("Dead or Alive", 3),
        ("Ninja Gaiden", 4),
        ("Atelier", 5),
        ("Dynasty Warriors", 6),
    ],
    "9766.T": [
        ("Konami", 1),
        ("Metal Gear", 2),
        ("Pro Evolution Soccer", 3),
        ("PES", 4),
        ("Yu-Gi-Oh", 5),
        ("Castlevania", 6),
        ("Silent Hill", 7),
    ],
    "259960.KS": [
        ("KRAFTON", 1),
        ("PUBG", 2),
        ("PlayerUnknown's Battlegrounds", 3),
        ("PUBG Mobile", 4),
        ("PUBG: Battlegrounds", 5),
    ],
    "2121.T": [
        ("MIXI", 1),
        ("Monster Strike", 2),
        ("MIXI Inc", 3),
        ("mixi", 4),
        ("MonSt", 5),
    ],
    "MTG-B.ST": [
        ("Modern Times Group", 1),
        ("MTG", 2),
        ("ESL Gaming", 3),
        ("ESL", 4),
        ("DreamHack", 5),
    ],
    "225570.KQ": [
        ("Nat Games", 1),
        ("Nat Games Co", 2),
        ("Nat Games mobile", 3),
        ("Nat Games Korea", 4),
        ("넷게임즈", 5),
    ],
    "036570.KS": [
        ("NCSOFT", 1),
        ("Lineage", 2),
        ("Guild Wars", 3),
        ("Aion", 4),
        ("Blade & Soul", 5),
    ],
    "0777.HK": [
        ("NetDragon", 1),
        ("网龙", 2),
        ("NetDragon Websoft", 3),
        ("NetDragon games", 4),
        ("网龙游戏", 5),
    ],
    "NTES": [
        ("NetEase", 1),
        ("Diablo Immortal", 2),
        ("Onmyoji", 3),
        ("Identity V", 4),
        ("Knives Out", 5),
        ("网易", 6),
    ],
    "251270.KS": [
        ("Netmarble", 1),
        ("Netmarble Corporation", 2),
        ("Seven Knights", 3),
        ("Netmarble games", 4),
        ("Marvel Future Fight", 5),
    ],
    "3659.T": [
        ("NEXON", 1),
        ("MapleStory", 2),
        ("Dungeon Fighter Online", 3),
        ("KartRider", 4),
        ("Mabinogi", 5),
    ],
    "7974.T": [
        ("Nintendo", 1),
        ("Switch", 2),
        ("Mario", 3),
        ("Zelda", 4),
        ("Pokemon", 5),
        ("Animal Crossing", 6),
        ("Splatoon", 7),
        ("Super Smash Bros", 8),
    ],
    "263750.KQ": [
        ("Pearl Abyss", 1),
        ("Black Desert", 2),
        ("Black Desert Online", 3),
        ("Pearl Abyss Corp", 4),
        ("BDO", 5),
    ],
    "PLTK": [
        ("Playtika", 1),
        ("Playtika Holding", 2),
        ("Slotomania", 3),
        ("Bingo Blitz", 4),
        ("Caesars Slots", 5),
    ],
    "RBLX": [
        ("Roblox", 1),
        ("Robux", 2),
        ("Roblox Studio", 3),
        ("Roblox game", 4),
        ("Roblox developer", 5),
    ],
    "9684.T": [
        ("Square Enix", 1),
        ("Final Fantasy", 2),
        ("Dragon Quest", 3),
        ("Kingdom Hearts", 4),
        ("Tomb Raider", 5),
        ("Deus Ex", 6),
        ("Just Cause", 7),
    ],
    "TTWO": [
        ("Take-Two", 1),
        ("GTA", 2),
        ("Grand Theft Auto", 3),
        ("Rockstar Games", 4),
        ("2K Games", 5),
        ("NBA 2K", 6),
        ("Red Dead Redemption", 7),
    ],
    "9890.HK": [
        ("Tanwan", 1),
        ("Tanwan Inc", 2),
        ("Tanwan games", 3),
        ("Tanwan mobile", 4),
        ("探玩", 5),
    ],
    "TBCH": [
        ("Turtle Beach", 1),
        ("Turtle Beach headset", 2),
        ("Turtle Beach audio", 3),
        ("Turtle Beach gaming", 4),
        ("Turtle Beach Recon", 5),
    ],
    "UBI.PA": [
        ("Ubisoft", 1),
        ("Assassin's Creed", 2),
        ("Far Cry", 3),
        ("Watch Dogs", 4),
        ("Rainbow Six", 5),
        ("Just Dance", 6),
    ],
    "U": [
        ("Unity", 1),
        ("Unity Software", 2),
        ("Unity engine", 3),
        ("Unity game engine", 4),
        ("Unity 3D", 5),
    ],
    "VIV.PA": [
        ("Vivendi", 1),
        ("Vivendi SE", 2),
        ("Canal+", 3),
        ("Vivendi Games", 4),
        ("Gameloft", 5),
    ],
    "112040.KQ": [
        ("Wemade", 1),
        ("Wemade Co", 2),
        ("Legend of Mir", 3),
        ("Wemade games", 4),
        ("미르의 전설", 5),
    ],
    "101730.KQ": [
        ("Wemade Max", 1),
        ("Wemade Max Co", 2),
        ("Wemade Max games", 3),
        ("위메이드맥스", 4),
        ("Wemade Max mobile", 5),
    ],
    "2400.HK": [
        ("XD Inc", 1),
        ("心动网络", 2),
        ("XD Games", 3),
        ("XD mobile games", 4),
        ("心动游戏", 5),
    ],
}


def get_main_db_connection():
    """Get connection to main database."""
    try:
        database_url = os.getenv('DATABASE_URL')
        if database_url:
            return psycopg.connect(database_url)
        
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT', '5432')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        database = os.getenv('DB_NAME')
        
        if not all([host, user, password, database]):
            raise ValueError("Database connection parameters not found")
        
        return psycopg.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=database
        )
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise


def get_quest_companies(conn):
    """Get companies from QUEST universe (id 491)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                uc.company_uid,
                ci.name as company_name,
                mt.ticker as main_ticker
            FROM universe_companies uc
            JOIN varrock.companies c ON uc.company_uid = c.company_uid
            LEFT JOIN varrock.company_info ci ON c.company_uid = ci.company_uid
            LEFT JOIN varrock.tickers mt ON c.company_uid = mt.company_uid AND mt.is_main_ticker = TRUE
            WHERE uc.universe_id = 491
            ORDER BY ci.name, mt.ticker
        """, prepare=False)
        return cur.fetchall()


def insert_keywords(conn, company_uid: str, ticker: str, keywords: list):
    """Insert keywords for a company."""
    inserted = 0
    skipped = 0
    
    with conn.cursor() as cur:
        for keyword, priority in keywords:
            # Check if keyword already exists for this company
            cur.execute("""
                SELECT id FROM varrock.company_keywords
                WHERE company_uid = %s AND keyword = %s
            """, (company_uid, keyword), prepare=False)
            
            if cur.fetchone():
                try:
                    print(f"  [SKIP] Existing keyword: '{keyword}' (priority {priority})")
                except UnicodeEncodeError:
                    print(f"  [SKIP] Existing keyword: (priority {priority})")
                skipped += 1
                continue
            
            # Insert keyword with metadata
            metadata = json.dumps({"priority": priority})
            cur.execute("""
                INSERT INTO varrock.company_keywords (company_uid, keyword, metadata)
                VALUES (%s, %s, %s::jsonb)
            """, (company_uid, keyword, metadata), prepare=False)
            inserted += 1
            try:
                print(f"  [OK] Inserted: '{keyword}' (priority {priority})")
            except UnicodeEncodeError:
                print(f"  [OK] Inserted keyword (priority {priority})")
    
    return inserted, skipped


def main():
    """Main function to insert keywords."""
    print("=" * 60)
    print("Inserting Keywords for QUEST Universe (ID: 491)")
    print("=" * 60)
    
    try:
        conn = get_main_db_connection()
        companies = get_quest_companies(conn)
        
        if not companies:
            print("No companies found in QUEST universe!")
            return 1
        
        print(f"\nFound {len(companies)} companies in QUEST universe\n")
        
        total_inserted = 0
        total_skipped = 0
        companies_without_keywords = []
        
        for company_uid, company_name, ticker in companies:
            if not ticker:
                print(f"[WARN] Skipping {company_name}: No ticker found")
                continue
            
            if ticker not in KEYWORDS_BY_TICKER:
                print(f"[WARN] Skipping {company_name} ({ticker}): No keywords defined")
                companies_without_keywords.append((company_name, ticker))
                continue
            
            print(f"Processing {company_name} ({ticker}):")
            keywords = KEYWORDS_BY_TICKER[ticker]
            inserted, skipped = insert_keywords(conn, company_uid, ticker, keywords)
            total_inserted += inserted
            total_skipped += skipped
            print()
        
        conn.commit()
        
        print("=" * 60)
        print(f"Summary: {total_inserted} inserted, {total_skipped} skipped")
        if companies_without_keywords:
            print(f"\nCompanies without keywords ({len(companies_without_keywords)}):")
            for name, ticker in companies_without_keywords:
                print(f"  - {name} ({ticker})")
        print("=" * 60)
        
        conn.close()
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
